# blip.nim
#
# Copyright (c) 2020 Couchbase, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import blip/[message, transport]
import blip/private/[codec, log, fixseq, outbox, protocol, varint]
import asyncdispatch, strformat, strmisc, tables
import deques # transitive dependency of outbox; compiler makes me import it for some reason

export message, transport

type
    BlipObj = object
        socket {.requiresInit.}: Transport  # The WebSocket
        outbox: Outbox[MessageOut]          # Messages being sent
        icebox: Icebox[MessageOut]          # Messages paused until an Ack is received
        outNumber: MessageNo                # Number of latest outgoing message
        inNumber: MessageNo                 # Number of latest incoming message
        incomingRequests: MessageMap        # Incoming partial request messages
        incomingResponses: MessageMap       # Incoming partial response messages
        outBuffer: fixseq[byte]             # Reuseable buffer for outgoing frames
        inBuffer: fixseq[byte]              # Reuseable buffer for incoming frames
        outCodec: Deflater                  # Compresses outgoing frames
        inCodec: Inflater                   # Decompresses incoming frames
        defaultHandler: Handler             # Default callback to handle incoming requests
        handlers: Table[string, Handler]    # Callbacks for requests with specific "Profile"s
        shouldCloseWhenIdle: bool           # When true, will close when isIdle becomes true

    Blip* = ref BlipObj
        ## A BLIP connection.

    MessageMap = Table[MessageNo, MessageIn]

    Handler* = proc(msg: MessageIn) {.gcsafe.}
        ## A callback to handle an incoming BLIP request.

proc setBLIPLogLevel*(level: int) =
    ## Sets the level of logging: 0 is errors only, 1 includes warnings, 2 info, 3 verbose, 4 debug
    CurrentLogLevel = LogLevel(level)

proc newBlip*(socket: Transport, compressionLevel: int = -1): Blip =
    ## Creates a new Blip object from a WebSocket. You still need to call ``run`` on it.
    assert socket != nil
    result = Blip(socket: socket)
    result.outCodec = newDeflater(CompressionLevel(compressionLevel))
    result.inCodec = newInflater()
    result.outBuffer = newFixseq[byte](32768)
    result.inBuffer = newFixseqOfCap[byte](32768)

proc addHandler*(blip: Blip, profile: string, handler: Handler) =
    ## Registers a callback that will receive incoming messages with a specific "Profile" property.
    blip.handlers[profile] = handler

proc setDefaultHandler*(blip: Blip, handler: Handler) =
    ## Registers a callback that will receive incoming messages not processed by other handlers.
    blip.defaultHandler = handler

func isIdle*(blip: Blip): bool =
    ## True if there are no messages being sent or received.
    return blip.incomingResponses.len == 0 and blip.incomingRequests.len == 0 and
           blip.outbox.empty and blip.icebox.empty

func isClosed*(blip: Blip): bool =
    return blip.outbox.isClosed

proc i_close(blip: Blip) =
    ## Shuts down the Blip object.
    blip.outbox.close()
    asyncCheck blip.socket.close()

proc close*(blip: Blip) =
    ## Shuts down the Blip object.
    if not blip.isClosed:
        log Info, "Closing by request..."
        blip.i_close()

proc closeIfIdle*(blip: Blip): bool =
    ## If the Blip object is idle (no messages incoming or outgoing), closes it and returns true.
    if blip.isClosed:
        return true
    elif blip.isIdle:
        log Info, "Closing idle Blip connection..."
        blip.i_close()
        return true
    else:
        return false

proc closeWhenIdle*(blip: Blip) =
    ## The Blip object will be closed as soon as it goes idle.
    ## If it's already idle, it's closed immediately.
    if not blip.closeIfIdle():
        blip.shouldCloseWhenIdle = true
        log Verbose, "Blip connection will close ASAP"

proc checkIdleClose(blip: Blip): bool =
    if blip.shouldCloseWhenIdle:
        if blip.closeIfIdle():
            return true
        log Debug, "(Not idle yet; still waiting)"
    return false

# Sending:

proc sendRequestProc(blip: Blip): SendProc =
    # Returns a proc that will send a MessageOut as a request
    return proc(msg: sink MessageOut): Future[MessageIn] =
        assert msg.messageType == kRequestType
        if blip.outCodec.compressionLevel == NoCompression:
            msg.noCompression()
        let msgNo = ++blip.outNumber
        msg.number = msgNo
        blip.outbox.push(msg)
        if msg.noReply:
            return nil
        else:
            let response = newPendingResponse(msg)
            blip.incomingResponses[msgNo] = response
            return response.createCompletionFuture

proc sendResponseProc(blip: Blip): SendProc =
    # Returns a proc that will send a MessageOut as a response
    return proc(msg: sink MessageOut): Future[MessageIn] =
        assert msg.messageType != kRequestType
        assert(msg.number != MessageNo(0))
        if blip.outCodec.compressionLevel == NoCompression:
            msg.noCompression()
        blip.outbox.push(msg)
        return nil


proc newRequest*(blip: Blip, profile: string = ""): MessageBuf =
    result = newMessage(blip.sendRequestProc())
    ## Creates a new request message.
    if profile != "":
        result.profile = profile

proc sendLoop(blip: Blip) {.async.} =
    ## Async loop that processes messages in the outbox and sends them as WebSocket messages,
    ## until the connection is closed.
    try:
        while not blip.checkIdleClose() and blip.socket.canSend:
            let msg = await blip.outbox.pop()
            if msg == nil:
                return
            if int(msg.number) mod 100 == 0:
                log Info, "---- sending msg #{int(msg.number)} ----" #TEMP
            let frameSize = if (msg.priority == Urgent or blip.outbox.empty): 32768 else: 4096
            var buffer = blip.outBuffer[0 ..< frameSize]
            buffer.clear()
            msg.nextFrame(buffer, blip.outCodec)
            if not msg.finished:
                if msg.needsAck:
                    log Info, "Freezing {msg} until acked"
                    blip.icebox.add(msg)
                else:
                    blip.outbox.push(msg)

            let f = blip.socket.send(buffer)
            yield f
            if f.failed:
                logException f.error, "sending frame"
                break
    except CatchableError as x:
        logException x, "in sendLoop"
    log Debug, "sendLoop is done"


# Receiving:

proc pendingRequest(blip: Blip, flags: byte, msgNo: MessageNo): MessageIn =
    ## Returns the MessageIn for an incoming _request_ frame.
    if msgNo == blip.inNumber + 1:
        # This is the start of a new request:
        blip.inNumber = msgNo
        let msg = newIncomingRequest(flags, msgNo, blip.sendResponseProc())
        if (flags and kMoreComing) != 0:
            blip.incomingRequests[msgNo] = msg      # Request is complete, stop tracking it
        return msg
    elif msgNo <= blip.inNumber:
        # This is a continuation frame of a request:
        let msg = blip.incomingRequests.getOrDefault(msgNo)
        if msg == nil:
            raise newException(BlipException, "Invalid incoming message number (duplicate)")
        if (flags and kMoreComing) == 0:
            blip.incomingRequests.del(msgNo)
        return msg
    else:
       raise newException(BlipException, "Invalid incoming message number (too high)")

proc pendingResponse(blip: Blip, flags: byte, msgNo: MessageNo): MessageIn =
    ## Returns the MessageIn for an incoming _response_ frame.
    # Look up the response object with this number:
    let msg = blip.incomingResponses.getOrDefault(msgNo)
    if msg == nil:
        raise newException(BlipException, "Invalid incoming response number")
    if (flags and kMoreComing) == 0:
        blip.incomingResponses.del(msgNo)   # Response is complete, no need to track this
    return msg

proc cancelPendingMessages(blip: Blip) =
    for n, msg in blip.incomingResponses:
        msg.cancel()
    blip.incomingResponses = MessageMap()

proc dispatchIncomingRequest(blip: Blip, msg: MessageIn) =
    ## Calls the appropriate client-registered handler proc for an incoming request message.
    let profile = msg.profile
    let handler = blip.handlers.getOrDefault(profile, blip.defaultHandler)
    if handler != nil:
        try:
            handler(msg)
        except CatchableError as e:
            logException e, "in handler for {msg}, profile '{profile}'"
            if not msg.noReply:
                msg.replyWithError("BLIP", 501, "Handler failed unexpectedly")
    elif not msg.noReply:
        msg.replyWithError("BLIP", 404, "No handler")
    else:
        log Warning, "No handler for incoming noreply request, profile='{profile}'"

proc handleFrame(blip: Blip, frame: fixseq[byte]) =
    ## Processes an incoming message frame.
    # Read the flags and message number:
    var frame = frame
    if frame.len < 2:
        log Error, "Received impossibly small frame"
        return
    var pos = 0
    let msgNo = MessageNo(getVarint(frame.toOpenArray, pos))
    if pos >= frame.len:
        raise newException(BlipException, "Missing flags in frame")
    let flags = frame[pos]
    frame.moveStart(pos + 1)
    var msgType = MessageType(flags and kTypeMask)
    log Verbose, "<<< Rcvd frame: {msgType}#{uint(msgNo)} {flagsToString(flags)} {frame.len} bytes"

    if msgType < kAckRequestType:
        # Handle an incoming request/response frame:
        # Look up or create the IncomingMessage object:
        let msg = (if msgType == kRequestType:
            blip.pendingRequest(flags, msgNo)
        else:
            blip.pendingResponse(flags, msgNo))
        # Append the frame to the message, and dispatch it if it's complete:
        let ack = msg.addFrame(flags, frame, blip.inBuffer, blip.inCodec)
        if ack != nil:
            blip.outbox.push(ack)
        if (flags and kMoreComing) == 0 and msgType == kRequestType:
            blip.dispatchIncomingRequest(msg)
    else:
        # Handle an incoming ACK:
        let findType = if msgType == kAckRequestType: kRequestType else: kResponseType
        let msg = blip.outbox.find(findType, msgNo)
        if msg != nil:
            msg.handleAck(frame.toOpenArray)
        else:
            let (i, msg) = blip.icebox.find(findType, msgNo)
            if msg != nil:
                msg.handleAck(frame.toOpenArray)
                if not msg.needsAck:
                    log Info, "Unfreezing acked {msg}"
                    blip.icebox.del(i)
                    blip.outbox.push(msg)
            else:
                log Warning, "Received {$msgType} for unknown message #{uint64(msgNo)}"

proc receiveLoop(blip: Blip) {.async.} =
    ## Async loop that receives WebSocket messages and passes them to `handleFrame`,
    ## until the socket closes.
    while not blip.checkIdleClose() and blip.socket.canReceive:
        var frame: fixseq[byte]
        try:
            frame = await blip.socket.receive()
        except CatchableError as e:
            logException e, "on receive"
            break

        if frame.len == 0:
            log Info, "BLIP connection closed cleanly"
            break

        try:
            blip.handleFrame(frame)
        except BLIPException as e:
            logException e, "handling incoming frame"
            await blip.socket.close() # TODO: Set close code
        except CatchableError as e:
            logException e, "handling incoming frame"
            await blip.socket.close() # TODO: Set close code
    blip.cancelPendingMessages()
    log Debug, "receiveLoop is done"

proc run*(blip: Blip): Future[void] =
    ## Runs the Blip's asynchronous send and receive loops.
    ## Returns a Future that completes when both loops have stopped.
    asyncCheck blip.sendLoop()
    return blip.receiveLoop()
