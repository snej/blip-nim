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

import private/[codec, log, fixseq, protocol, varint]
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
        properties: string          ## Encoded properties/headers
        messageType {.requiresInit.}: MessageType    ## Type of message (request/response/error)
        re: MessageNo               ## If a response, the peer message# it's replying to
        sendProc: SendProc          ## Call this to send the message (points to Blip method)

    Priority* = enum
        ## A message's priority. Urgent messages get more bandwidth.
        Normal
        Urgent

    Message = ref object of RootObj
        ## Base class of incoming or outgoing message objects
        flags: byte             ## BLIP protocol message flags (see protocol.nim)
        number {.requiresInit.}: MessageNo       ## Message number (sequential in its direction)

    MessageOut* {.requiresInit.} = ref object of Message
        ## [INTERNAL ONLY] An outgoing message in the process of being sent.
        data: fixseq[byte]      ## Encoded message data (properties size + properties + body)
        bytesSent: int          ## Number of bytes sent
        unackedBytes: int       ## Number of sent bytes that haven't been ACKed

    MessageIn* {.requiresInit.} = ref object of Message
        ## An incoming message from the peer, either a request or a response.
        state: MessageInState           ## Tracks what's been received so far
        propertyBuf: seq[byte]          ## Encoded properties
        body: string                    ## Body
        rawBytesReceived: int           ## Total number of raw frame bytes received
        unackedBytes: int               ## Number of bytes received but not ACKed
        propertiesRemaining: int        ## Number of bytes of properties not yet received
        replyProc {.requiresInit.}: SendProc             ## Function that will send a reply (points to Blip method)
        completionFuture: Future[MessageIn]

    MessageInState = enum
        Start
        ReadingProperties
        ReadingBody
        Complete

    SendProc* = proc(msg: sink MessageOut): Future[MessageIn] {.closure, gcsafe.}


# MessageBuf

#(NOTE: body, priority, compressed, and noReply are public fields that can be set directly.)

proc newMessage*(sendProc: SendProc): MessageBuf =
    # [INTERNAL ONLY] Creates a new message you can add properties and/or a body to.
    return MessageBuf(messageType: kRequestType, sendProc: sendProc)

proc `[]=`*(buf: var MessageBuf, key: string, val: string) =
    ## Adds a property key/value to a message.
    assert key.find('\0') < 0 and val.find('\0') < 0
    buf.properties = buf.properties & key & '\0' & val & '\0'

proc add*(buf: var MessageBuf, kv: openarray[(string,string)]) =
    ## Adds multiple properties to a message.
    for (key, val) in kv:
        buf[key] = val

proc `profile=`*(buf: var MessageBuf, profile: string) =
    buf[ProfileProperty] = profile


# Message (common between MessageOut and MessageIn)

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
    if buf.noReply and buf.messageType == kRequestType:
        flags = flags or kNoReply
    # Encode the message into a byte sequence:
    var data = newFixseqOfCap[byte](9 + buf.properties.len + buf.body.len)
    if buf.messageType <= kErrorType:
        data.addVarint(uint64(buf.properties.len))
        data.add(buf.properties)
    data.add(buf.body)
    return MessageOut(flags: flags, number: buf.re, data: data)

proc newAckMessage*(msg: MessageIn, bytesReceived: int): MessageOut =
    let ackType = if msg.messageType == kRequestType: kAckRequestType else: kAckResponseType
    var buf = MessageBuf(priority: Urgent, noReply: true, messageType: ackType, re: msg.number)
    var body = newSeqOfCap[byte](4)
    body.addVarint(uint64(bytesReceived))
    buf.body = cast[string](body)
    return newMessageOut(buf)

proc noCompression*(msg: MessageOut) =
    # [INTERNAL ONLY]
    msg.flags = msg.flags and not kCompressed

proc finished*(msg: MessageOut): bool =
    return msg.data.len == 0

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
    let p = buf.sendProc
    assert p != nil
    return p(newMessageOut(buf))

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

proc skipCString(str: cstring): cstring =
    return cast[cstring]( cast[int](str) + str.len + 1 )

iterator properties*(msg: MessageIn): (cstring, cstring) =
    assert msg.state > ReadingProperties
    var pos = cast[cstring](addr msg.propertyBuf[0])
    let endProps = cast[cstring](addr msg.propertyBuf[msg.propertyBuf.high])
    while pos < endProps:
        let key = pos
        let val = skipCString(key)
        pos = skipCString(val)
        yield (key, val)

proc property*(msg: MessageIn; key: string; default: string = ""): string =
    for (k, v) in msg.properties:
        if k == key: return $v
    return default

proc intProperty*(msg: MessageIn; key: string; defaultValue: int = 0): int =
    let stringVal = msg.property(key)
    if stringVal != "":
        try:
            return stringVal.parseInt()
        except ValueError:
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
    assert msg.replyProc != nil
    return newResponseBuf(msg, kResponseType)

proc createErrorResponseInternal(msg: MessageIn; domain = BLIPErrorDomain; code: int; errorMessage: string): MessageBuf =
    assert not msg.noReply
    result = newResponseBuf(msg, kErrorType)
    result[ErrorCodeProperty] = $code
    if domain != BLIPErrorDomain:
        result[ErrorDomainProperty] = domain
    result.body = errorMessage

proc createErrorResponse*(msg: MessageIn; domain = BLIPErrorDomain; code: int; errorMessage: string): MessageBuf =
    ## Returns a new MessageBuf for an error response.
    ## Once you've set the properties and body, call ``send`` on it.
    assert msg.finished
    assert msg.messageType == kRequestType
    assert msg.replyProc != nil
    return msg.createErrorResponseInternal(domain, code, errorMessage)

proc replyWithError*(msg: MessageIn; domain = BLIPErrorDomain; code: int; errorMessage: string) =
    ## Sends an error response to this message.
    msg.createErrorResponse(domain, code, errorMessage).sendNoReply()

proc addBytes(msg: MessageIn, decoded: fixseq[byte]) =
    var pos = 0;
    if msg.state == Start:
        # First frame. This one starts with the properties' length, and (some or all) properties.
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
        msg.body.add(decoded[pos .. ^1])

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
            msg.body = ""
        else:
            raise newException(BlipException, "Frame has inconsistent message type")

    if (flags and kCompressed) != 0:
        var inputRemaining = frame
        while inputRemaining.len > 0:
            log Verbose, "    processing {inputRemaining.len} bytes..."
            var decoded = buffer
            codec.write(inputRemaining, decoded, DefaultMode)
            msg.addBytes(decoded)
    else:
        msg.addBytes(codec.decodeRaw(frame))

    if (flags and kMoreComing) == 0:
        # End of message!
        if msg.state < ReadingBody:
            raise newException(BlipException, "Incomplete message properties")
        msg.state = Complete
        let f = msg.completionFuture
        if f != nil:
            f.complete(msg)
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

proc cancel*(msg: MessageIn; errDomain = BLIPErrorDomain, errCode = 502, errMsg = "Disconnected") =
    # [INTERNAL ONLY] Notify client of an error receiving a response, usually because the
    # socket was disconnected before the response arrived.
    if msg.messageType == kResponseType and msg.completionFuture != nil:
        # Make an error response:
        msg.state = Complete
        var buf = msg.createErrorResponseInternal(errDomain, errCode, errMsg)
        msg.flags = withMessageType(msg.flags, kErrorType)
        msg.propertyBuf = cast[seq[byte]](buf.properties)
        msg.body = buf.body
        # Now deliver to the Future's observer:
        msg.completionFuture.complete(msg)
