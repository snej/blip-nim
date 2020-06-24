# message.nim
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

## BLIP message implementation.

import protocol, private/[codec, log, fixseq, varint]
import asyncdispatch, strformat, strutils

proc messageType(flags: byte): MessageType =
    MessageType(flags and kTypeMask)
proc isAck(flags: byte): bool =
    flags.messageType in kAckRequestType .. kAckResponseType
proc withMessageType(flags: byte, typ: MessageType): byte =
    (flags and not kTypeMask) or byte(typ)
proc flagsToString*(flags: byte): string =
    result = "----"
    if (flags and kMoreComing) != 0: result[0] = '+'
    if (flags and kNoReply) != 0:    result[1] = 'x'
    if (flags and kUrgent) != 0:     result[2] = '!'
    if (flags and kCompressed) != 0: result[3] = 'z'


type
    MessageBuf* = object
        ## A mutable object for assembling a request or response before sending it.
        body*: string               ## Body of the message
        priority*: Priority         ## Message priority
        compressed*: bool           ## True to compress message
        noReply*: bool              ## True if no reply should be sent [Requests only]
        properties: seq[byte]       ## Encoded properties/headers
        messageType: MessageType    ## Type of message (request/response/error)
        re: MessageNo               ## If a response, the peer message# it's replying to
        sendProc: SendProc          ## Call this to send the message (points to Blip method)

    Priority* = enum
        ## A message's priority. Urgent messages get delivered quicker.
        Normal
        Urgent

    Message = ref object of RootObj
        ## Base class of incoming or outgoing message objects
        flags: byte             ## BLIP protocol message flags (see protocol.nim)
        number: MessageNo       ## Message number (sequential in its direction)

    MessageOut* = ref object of Message
        ## [INTERNAL ONLY] An outgoing message in the process of being sent.
        data: fixseq[byte]      ## Encoded message data (properties size + properties + body)
        bytesSent: int          ## Number of bytes sent
        unackedBytes: int       ## Number of sent bytes that haven't been ACKed

    MessageIn* = ref object of Message
        ## An incoming message from the peer, either a request or a response.
        state: MessageInState           ## Tracks what's been received so far
        propertyBuf: seq[byte]          ## Encoded properties
        body: seq[byte]                 ## Body
        rawBytesReceived: int           ## Total number of raw frame bytes received
        unackedBytes: int               ## Number of bytes received but not ACKed
        propertiesRemaining: int        ## Number of bytes of properties not yet received
        replyProc: SendProc             ## Function that will send a reply (points to Blip method)
        completionFuture: Future[MessageIn]

    MessageInState = enum
        Start
        ReadingProperties
        ReadingBody
        Complete

    SendProc* = proc(msg: MessageOut): Future[MessageIn] {.gcsafe.}


# MessageBuf

proc newMessage*(sendProc: SendProc): MessageBuf =
    ## Creates a new message you can add properties and/or a body to.
    return MessageBuf(sendProc: sendProc)

proc `[]=`*(buf: var MessageBuf, key: string, val: string) =
    ## Adds a property key/value to a message.
    buf.properties.add(cast[seq[byte]](key))
    buf.properties.add(0)
    buf.properties.add(cast[seq[byte]](val))
    buf.properties.add(0)

proc add*(buf: var MessageBuf, kv: openarray[(string,string)]) =
    ## Adds multiple properties to a message.
    for (key, val) in kv:
        buf[key] = val

proc `profile=`*(buf: var MessageBuf, profile: string) =
    buf[ProfileProperty] = profile


# Message

proc priority*(msg: Message): Priority  =
    if (msg.flags and kUrgent) != 0: Urgent else: Normal
proc noReply*(msg: Message): bool =
    return (msg.flags and kNoReply) != 0
proc messageType*(msg: Message): MessageType =
    return messageType(msg.flags)
proc isAck*(msg: Message): bool =
    return msg.flags.isAck
proc number*(msg: Message): MessageNo =
    return msg.number
proc `number=`*(msg: Message, n: MessageNo) =
    assert msg.number == MessageNo(0)
    assert n > MessageNo(0)
    msg.number = n

proc `$`*(msg: Message): string =
    $(msg.messageType) & '#' & $uint(msg.number)

# MessageOut

proc newMessageOut*(buf: sink MessageBuf): MessageOut =
    # [INTERNAL ONLY] Creates a new MessageOut from a MessageBuf.
    var flags = byte(buf.messageType)
    if buf.priority == Urgent:
        flags = flags or kUrgent
    if buf.compressed:
        flags = flags or kCompressed
    if buf.noReply:
        flags = flags or kNoReply
    # Encode the message into a byte sequence:
    var data = newFixseqOfCap[byte](9 + buf.properties.len + buf.body.len)
    if buf.messageType <= kErrorType:
        data.addVarint(uint64(buf.properties.len))
        data.add(buf.properties)
    data.add(cast[seq[byte]](buf.body))
    return MessageOut(flags: flags, number: buf.re, data: data)

proc newACKMessage*(msg: MessageIn, bytesReceived: int): MessageOut =
    let ackType = if msg.messageType == kRequestType: kAckRequestType else: kAckResponseType
    var buf = MessageBuf(priority: Urgent, noReply: true, messageType: ackType, re: msg.number)
    var body = newSeqOfCap[byte](4)
    body.addVarint(uint64(bytesReceived))
    buf.body = cast[string](body)
    return newMessageOut(buf)

proc finished*(msg: MessageOut): bool =
    msg.data.len == 0

proc nextFrame*(msg: MessageOut, frame: var fixseq[byte], codec: Codec) =
    # [INTERNAL ONLY] Fills `frame` with the next frame to send.
    frame.clear()
    frame.addVarint(uint64(msg.number))
    let flagsPos = frame.len
    var flags = msg.flags
    frame.add(flags)

    if msg.messageType >= kAckRequestType:
        frame.add(msg.data.toOpenArray)
        msg.data.reset()
        return

    let codecMode = if (flags and kCompressed) != 0: DefaultMode else: Raw
    codec.write(msg.data, frame, codecMode)

    if msg.data.len > 0:
        flags = flags or kMoreComing
        frame[flagsPos] = flags

    let bytesSent = frame.len - flagsPos - 1  # don't count metadata
    log Verbose, ">>> Send frame: {msg} {flagsToString(flags)} {bytesSent} bytes at {msg.bytesSent}"
    msg.bytesSent += bytesSent
    msg.unackedBytes += bytesSent

proc needsAck*(msg: MessageOut): bool =
    return msg.unackedBytes >= kOutgoingAckThreshold

proc handleAck*(msg: MessageOut, body: openarray[byte]) =
    let byteCount = getVarint(body)
    if byteCount <= uint64(msg.bytesSent):
        msg.unackedBytes = min(msg.unackedBytes, (msg.bytesSent - int(byteCount)))
    log Verbose, "{msg} received ack to {byteCount}; unackedBytes now {msg.unackedBytes}"


proc send*(buf: sink MessageBuf): Future[MessageIn] =
    return buf.sendProc(newMessageOut(buf))

proc sendNoReply*(buf: sink MessageBuf) =
    buf.noReply = true
    discard buf.send()


# MessageIn


proc newIncomingRequest*(flags: byte; number: MessageNo, replyProc: SendProc): MessageIn =
    # [INTERNAL ONLY] Creates a MessageIn for a new request coming from the peer.
    result = MessageIn(flags: flags, number: number, replyProc: replyProc)
    assert result.messageType == kRequestType

proc newPendingResponse*(request: MessageOut): MessageIn =
    # [INTERNAL ONLY] Creates a MessageIn for an expected response from the peer.
    assert request.messageType == kRequestType
    assert request.number > MessageNo(0)
    assert not request.noReply
    let flags = request.flags.withMessageType(kResponseType)
    return MessageIn(flags: flags, number: request.number, replyProc: nil)

proc readCString(str: var openarray[byte]; pos: var int): string =
    # No range checking here; we let the runtime throw a range error if props are invalid.
    var i = pos
    while str[i] != 0: i += 1
    result = cast[string](str[pos ..< i])
    pos = i + 1

iterator properties*(msg: MessageIn): (string, string) =
    assert msg.state > ReadingProperties
    var pos = 0
    while pos < msg.propertyBuf.len:
        let key = readCString(msg.propertyBuf, pos)
        let val = readCString(msg.propertyBuf, pos)
        yield (key, val)

proc property*(msg: MessageIn; key: string; default: string = ""): string =
    for (k, v) in msg.properties:
        if k == key: return v
    return default

proc intProperty*(msg: MessageIn; key: string; defaultValue: int = 0): int =
    let stringVal = msg.property(key)
    if stringVal != "":
        try:
            return stringVal.parseInt()
        except:
            discard
    return defaultValue

proc `[]`*(msg: MessageIn, key: string): string =
    msg.property(key)

proc profile*(msg: MessageIn): string =
    msg.property(ProfileProperty)

proc isError*(msg: MessageIn): bool =  msg.messageType == kErrorType

proc error*(msg: MessageIn): (string, int) =
    ## The message's error domain and code.
    if msg.isError:
        return (msg.property(ErrorDomainProperty), msg.intProperty(ErrorCodeProperty))

proc finished*(msg: MessageIn): bool =
    return msg.state == Complete

proc body*(msg: MessageIn): string =
    assert msg.state == Complete
    return cast[string](msg.body)

proc newResponseBuf(msg: MessageIn, messageType: MessageType): MessageBuf =
    MessageBuf(messageType: messageType, priority: msg.priority, noReply: true, re: msg.number,
               sendProc: msg.replyProc)

proc createResponse*(msg: MessageIn): MessageBuf =
    ## Returns a new MessageBuf for creating a response to this request.
    ## Once you've set the properties and body, call ``send`` on it.
    assert msg.finished
    assert msg.messageType == kRequestType
    assert not msg.noReply
    return newResponseBuf(msg, kResponseType)

proc createErrorResponse*(msg: MessageIn; domain = BLIPErrorDomain; code: int; errorMessage: string): MessageBuf =
    ## Returns a new MessageBuf for an error response.
    ## Once you've set the properties and body, call ``send`` on it.
    assert msg.finished
    assert msg.messageType == kRequestType
    assert not msg.noReply
    result = newResponseBuf(msg, kErrorType)
    result[ErrorCodeProperty] = $code
    if domain != BLIPErrorDomain:
        result[ErrorDomainProperty] = domain
    result.body = errorMessage

proc replyWithError*(msg: MessageIn; domain = BLIPErrorDomain; code: int; errorMessage: string) =
    ## Sends an error response to this message.
    msg.createErrorResponse(domain, code, errorMessage).sendNoReply()

proc addFrame*(msg: MessageIn;
               flags: byte;
               frame: fixseq[byte];
               buffer: fixseq[byte];
               codec: Codec): MessageOut =
    # [INTERNAL ONLY] Assembles the incoming message a frame at a time.
    # Note: `frame` does not include the frame flags and message number.
    # Returns an ACK message to be sent to the peer, if one is necessary.

    msg.rawBytesReceived += frame.len
    msg.unackedBytes += frame.len

    if messageType(flags) != msg.messageType:
        if messageType(flags) == kErrorType:
            # If an error frame arrives, reset state to read the error
            msg.flags = withMessageType(msg.flags, kErrorType)
            msg.state = Start
            msg.propertyBuf = @[]
            msg.body = @[]
        else:
            raise newException(BlipException, "Frame has inconsistent message type")

    let codecMode = if (flags and kCompressed) != 0: DefaultMode else: Raw

    var inputRemaining = frame
    while inputRemaining.len > 0:
        log Verbose, "    processing {inputRemaining.len} bytes..."
        var decoded = buffer
        codec.write(inputRemaining, decoded, codecMode)
        var pos = 0;

        if msg.state == Start:
            # First frame. This one starts with the properties length, and (some or all) properties.
            msg.propertiesRemaining = int(getVarint(decoded.toOpenArray, pos))
            msg.propertyBuf = newSeqOfCap[byte](min(msg.propertiesRemaining, 4096))
            msg.state = ReadingProperties
        if msg.state == ReadingProperties:
            # There are still bytes of properties left to read:
            let newPos = min(pos + msg.propertiesRemaining, decoded.len)
            msg.propertyBuf.add(decoded[pos ..< newPos].toOpenArray)
            msg.propertiesRemaining -= (newPos - pos)
            pos = newPos
            if msg.propertiesRemaining == 0:
                msg.state = ReadingBody
        if msg.state == ReadingBody:
            msg.body.add(decoded[pos .. ^1].toOpenArray)

    if (flags and kMoreComing) == 0:
        # End of message!
        if msg.state < ReadingBody:
            raise newException(BlipException, "Incomplete message properties")
        msg.state = Complete
        if msg.completionFuture != nil:
            msg.completionFuture.complete(msg)
        return nil
    elif msg.unackedBytes >= kIncomingAckThreshold:
        # Tell Blip to send back an ACK:
        msg.unackedBytes = 0
        log Verbose, "{msg} Sending ACK of {msg.rawBytesReceived} bytes"
        return newACKMessage(msg, bytesReceived = msg.rawBytesReceived)
    else:
        return nil

proc createCompletionFuture*(msg: MessageIn): Future[MessageIn] =
    if msg.completionFuture == nil:
        msg.completionFuture = newFuture[MessageIn]("BLIP response")
    return msg.completionFuture
