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

import blip/[message, outbox, protocol, transport]
import blip/private/[crc32, log, varint]
import asyncdispatch, strformat, tables


type
    Blip* = ref object
        socket: Transport               # The WebSocket
        outbox: Outbox                  # Messages being sent
        icebox: Icebox                  # Messages paused until an Ack is received
        outNumber: MessageNo            # Number of latest outgoing message
        inNumber: MessageNo             # Number of latest incoming message
        incomingRequests: MessageMap    # Incoming partial request messages
        incomingResponses: MessageMap   # Incoming partial response messages
        outChecksum: CRC32Accumulator
        inChecksum: CRC32Accumulator
        defaultHandler: Handler         # Default callback to handle incoming requests
        handlers: Table[string, Handler]# Callbacks for requests with specific "Profile"s

    MessageMap = Table[MessageNo, MessageIn]

    Handler* = proc(msg: MessageIn) {.gcsafe.}

proc setBLIPLogLevel*(level: int) =
    ## Sets the level of logging: 0 is errors only, 1 includes warnings, 2 info, 3 verbose, 4 debug
    CurrentLogLevel = LogLevel(level)

proc newBlip*(socket: Transport): Blip =
    ## Creates a new Blip object from a WebSocket. You still need to call ``run`` on it.
    assert socket != nil
    result = Blip(socket: socket)
    result.outChecksum.reset()
    result.inChecksum.reset()

proc addHandler*(blip: Blip, profile: string, handler: Handler) =
    ## Registers a callback that will receive incoming messages with a specific "Profile" property.
    blip.handlers[profile] = handler

proc setDefaultHandler*(blip: Blip, handler: Handler) =
    ## Registers a callback that will receive incoming messages not processed by other handlers.
    blip.defaultHandler = handler

proc close*(blip: Blip) {.async.} =
    ## Shuts down the Blip object.
    log Info, "BLIP closing"
    blip.outbox.close()
    await blip.socket.close()

# Sending:

proc sendRequestProc(blip: Blip): SendProc =
    # Returns a proc that will send a MessageOut as a request
    return proc(msg: MessageOut): Future[MessageIn] =
        assert msg.messageType == kRequestType
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
    return proc(msg: MessageOut): Future[MessageIn] =
        assert msg.messageType != kRequestType
        assert(msg.number != MessageNo(0))
        blip.outbox.push(msg)
        return nil


proc newRequest*(blip: Blip, profile: string = ""): MessageBuf =
    result = newMessage(blip.sendRequestProc())
    if profile != "":
        result.profile = profile

proc sendLoop(blip: Blip) {.async.} =
    ## Async loop that processes messages in the outbox and sends them as WebSocket messages,
    ## until the connection is closed.
    while blip.socket.canSend:
        let msg = await blip.outbox.pop()
        if msg == nil:
            return
        let frameSize = if (msg.priority == Urgent or blip.outbox.empty): 32768 else: 4096
        let frame = msg.nextFrame(frameSize, blip.outChecksum)
        if not msg.finished:
            if msg.needsAck:
                log Info, "Freezing {msg} until acked"
                blip.icebox.add(msg)
            else:
                blip.outbox.push(msg)

        let f = blip.socket.send(frame)
        yield f
        if f.failed:
            if f.error.name != "WebSocketClosedError":
                log Error, "Transport send error: {f.error.name} {f.error.msg}"
            break


# Receiving:

proc pendingRequest(blip: Blip, flags: byte, msgNo: MessageNo): MessageIn =
    ## Returns the MessageIn for an incoming request frame.
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
    ## Returns the MessageIn for an incoming response frame.
    # Look up the response object with this number:
    let msg = blip.incomingResponses.getOrDefault(msgNo)
    if msg == nil:
        raise newException(BlipException, "Invalid incoming response number")
    if (flags and kMoreComing) == 0:
        blip.incomingResponses.del(msgNo)   # Response is complete, no need to track this
    return msg

proc dispatchIncomingRequest(blip: Blip, msg: MessageIn) =
    let profile = msg.profile
    let handler = blip.handlers.getOrDefault(profile, blip.defaultHandler)
    if handler != nil:
        try:
            handler(msg)
        except:
            log Error, "Handler for {msg}, profile '{profile}', raised exception: {getCurrentExceptionMsg()}"
            if not msg.noReply:
                msg.replyWithError("BLIP", 501, "Handler failed unexpectedly")
    elif not msg.noReply:
        msg.replyWithError("BLIP", 404, "No handler")
    else:
        log Warning, "No handler for incoming noreply request, profile='{profile}'"

proc handleFrame(blip: Blip, frame: openarray[byte]) =
    # Read the flags and message number:
    if frame.len < 2:
        log Error, "Received impossibly small frame"
        return
    var pos = 0
    let msgNo = MessageNo(getVarint(frame, pos))
    if pos >= frame.len:
        raise newException(BlipException, "Missing flags in frame")
    let flags = frame[pos]
    pos += 1
    var msgType = MessageType(flags and kTypeMask)
    let body = frame[pos .. ^1]
    log Verbose, "<<< Rcvd frame: {msgType}#{uint(msgNo)} {flagsToString(flags)} {frame.len-pos} bytes"

    if msgType < kAckRequestType:
        # Handle an incoming request/response frame:
        # Look up or create the IncomingMessage object:
        let msg = if msgType == kRequestType:
            blip.pendingRequest(flags, msgNo)
        else:
            blip.pendingResponse(flags, msgNo)
        # Append the frame to the message, and dispatch it if it's complete:
        let ack = msg.addFrame(flags, body, blip.inChecksum)
        if ack != nil:
            blip.outbox.push(ack)
        if (flags and kMoreComing) == 0 and msgType == kRequestType:
            blip.dispatchIncomingRequest(msg)
    else:
        # Handle an incoming ACK:
        let findType = if msgType == kAckRequestType: kRequestType else: kResponseType
        let msg = blip.outbox.find(findType, msgNo)
        if msg != nil:
            msg.handleAck(body)
        else:
            let (i, msg) = blip.icebox.find(findType, msgNo)
            if msg != nil:
                msg.handleAck(body)
                if not msg.needsAck:
                    log Info, "Unfreezing acked {msg}"
                    blip.icebox.del(i)
                    blip.outbox.push(msg)
            else:
                log Warning, "Received {$msgType} for unknown message #{uint64(msgNo)}"

proc receiveLoop(blip: Blip) {.async.} =
    ## Async loop that receives WebSocket messages, reads them as BLIP frames, and assembles
    ## them into messages, until the connection is closed.
    while blip.socket.canReceive:
        let f = blip.socket.receive()
        yield f
        if f.failed:
            if f.error.name != "WebSocketClosedError":
                log Error, "Transport receive error: {f.error.name} {f.error.msg}"
            break
        else:
            let frame = f.read
            if frame.len > 0:
                blip.handleFrame(frame)
            else:
                break

proc run*(blip: Blip): Future[void] =
    ## Runs the Blip's asynchronous send and receive loops.
    ## Returns a Future that completes when both loops have stopped.
    return blip.sendLoop() and blip.receiveLoop()
