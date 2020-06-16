# This is just an example to get you started. A typical hybrid package
# uses this file as the main entry point of the application.

import blip/message, blip/outbox, blip/protocol, blip/transport, blip/private/crc32, blip/private/varint
import asyncdispatch, tables


type
    Blip* = ref object
        socket: Transport               # The WebSocket
        outbox: Outbox                  # Messages being sent
        outNumber: MessageNo            # Number of latest outgoing message
        inNumber: MessageNo             # Number of latest incoming message
        incomingRequests: MessageMap    # Incoming partial request messages
        incomingResponses: MessageMap   # Incoming partial response messages
        outChecksum: CRC32
        inChecksum: CRC32
        defaultHandler: Handler         # Default callback to handle incoming requests
        handlers: Table[string, Handler]# Callbacks for requests with specific "Profile"s

    MessageMap = Table[MessageNo, MessageIn]

    Handler* = proc(msg: MessageIn)

proc newBlip*(socket: Transport): Blip =
    ## Creates a new Blip object from a WebSocket. You still need to call ``run`` on it.
    Blip(socket: socket)

proc addHandler*(blip: Blip, profile: string, handler: Handler) =
    ## Registers a callback that will receive incoming messages with a specific "Profile" property.
    blip.handlers[profile] = handler

proc setDefaultHandler*(blip: Blip, handler: Handler) =
    ## Registers a callback that will receive incoming messages not processed by other handlers.
    blip.defaultHandler = handler

proc close*(blip: Blip) {.async.} =
    ## Shuts down the Blip object.
    blip.outbox.close()
    await blip.socket.close()

# Sending:

proc sendRequestProc(blip: Blip): SendProc =
    # Returns a proc that will send a MessageOut as a request
    return proc(msg: MessageOut): Future[MessageIn] =
        assert msg.messageType == kRequestType
        let msgNo = ++blip.outNumber
        msg.number = msgNo
        let response = newPendingResponse(msg)
        blip.incomingResponses[msgNo] = response
        blip.outbox.push(msg)
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
        let frameSize = if msg.priority == Urgent: 32768 else: 4096
        let frame = msg.nextFrame(frameSize, blip.outChecksum)
        if not msg.finished:
            blip.outbox.push(msg)

        let f = blip.socket.send(frame)
        yield f
        if f.failed:
            if f.error.name != "WebSocketClosedError":
                echo "*** Transport send error: ", f.error.name, " ", f.error.msg
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
        let msg = blip.incomingRequests[msgNo]
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
    let msg = blip.incomingResponses[msgNo]
    if msg == nil:
        raise newException(BlipException, "Invalid incoming response number")
    if (flags and kMoreComing) == 0:
        blip.incomingResponses.del(msgNo)   # Response is complete, no need to track this
    return msg

proc dispatchIncomingRequest(blip: Blip, msg: MessageIn) =
    let profile = msg.profile
    let handler = blip.handlers.getOrDefault(profile, blip.defaultHandler)
    if handler != nil:
        handler(msg)
    elif not msg.noReply:
        msg.replyWithError("BLIP", 404, "No handler")
    else:
        echo "(No handler for incoming noreply request, profile='", profile, "')"

proc handleFrame(blip: Blip, frame: openarray[byte]) =
    # Read the flags and message number:
    if frame.len < 2:
        raise newException(BlipException, "Impossibly small frame")
    let flags = frame[0]
    if (flags and kCompressed) != 0:
        raise newException(BlipException, "Compressed frames are not supported yet")
    var pos = 1
    let msgNo = MessageNo(getVarint(frame, pos))
    var msgType = MessageType(flags and kTypeMask)
    if msgType > kErrorType:
        raise newException(BlipException, "Unsupported frame type")
    # Look up or create an IncomingMessage object:
    let msg = if msgType == kRequestType:
        blip.pendingRequest(flags, msgNo)
    else:
        blip.pendingResponse(flags, msgNo)
    # Append the frame to the message, and dispatch it if it's complete:
    msg.addFrame(flags, frame[pos .. ^1], blip.inChecksum)
    if (flags and kMoreComing) == 0 and msgType == kRequestType:
            blip.dispatchIncomingRequest(msg)

proc receiveLoop(blip: Blip) {.async.} =
    ## Async loop that receives WebSocket messages, reads them as BLIP frames, and assembles
    ## them into messages, until the connection is closed.
    while blip.socket.canReceive:
        let f = blip.socket.receive()
        yield f
        if f.failed:
            if f.error.name != "WebSocketClosedError":
                echo "*** Transport receive error: ", f.error.name, " ", f.error.msg
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
