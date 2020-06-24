# testMessages.nim
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

import unittest

import blip/message, blip/protocol, blip/private/[codec, log, fixseq]

let kFrame1 = @['\x01',  # message#
                '\x40',  # flags
                '\x1F',  # properties length
                'P', 'r', 'o', 'f', 'i', 'l', 'e', '\x00',
                'I', 'n', 's', 'u', 'l', 't', '\x00',
                'L', 'a', 'n', 'g', 'u', 'a', 'g', 'e', '\x00',
                'F', 'r', 'e', 'n', 'c', 'h', '\x00',
                'Y', 'o', 'u', 'r', ' ', 'm',
                '}', '9', '\x13', '\xF3'] # checksum
let kFrame2 = @['\x01',  # message#
                '\x00',  # flags
                'o', 't', 'h', 'e', 'r', ' ', 'w', 'a', 's', ' ',
                'a', ' ', 'h', 'a', 'm', 's', 't', 'e', 'r',
                '\xF6', 'o', '\xCA', '='] # checksum

test "Outgoing Message":
    CurrentLogLevel = Verbose

    var buf = newMessage(nil)
    buf["Profile"] = "Insult"
    buf["Language"] = "French"
    buf.body = "Your mother was a hamster"

    var msg = newMessageOut(buf)
    check msg != nil
    msg.number = MessageNo(1)
    check msg.priority == Normal
    check not msg.noReply
    check msg.messageType == kRequestType
    check not msg.finished

    var codec = newDeflater()
    var frame = newFixseqOfCap[byte](44)
    msg.nextFrame(frame, codec)
    check frame.len == 44
    var chars = cast[seq[char]](frame.toSeq)
    echo chars
    check chars == kFrame1
    check not msg.finished

    msg.nextFrame(frame, codec)
    chars = cast[seq[char]](frame.toSeq)
    echo chars
    check chars == kFrame2
    check msg.finished


test "Incoming Message":
    #CurrentLogLevel = Debug

    let buffer = newFixseqOfCap[byte](1000)
    var codec = newInflater()
    let msg = newIncomingRequest(byte(kFrame1[1]), MessageNo(kFrame1[0]), nil)
    var frame = cast[seq[byte]](kFrame1).toFixseq()
    frame.moveStart(2)
    discard msg.addFrame(byte(kFrame1[1]), frame, buffer, codec)
    check not msg.finished
    frame = cast[seq[byte]](kFrame2).toFixseq()
    frame.moveStart(2)
    discard msg.addFrame(byte(kFrame2[1]), frame, buffer, codec)
    check msg.finished

    check msg.body == "Your mother was a hamster"
    check msg["Profile"] == "Insult"
    check msg["Language"] == "French"
    check msg["Horse"] == ""
    check msg.property("Horse", "coconuts") == "coconuts"
    check msg.intProperty("Language", -1) == -1


test "Frame Sizes":
    #CurrentLogLevel = Debug

    var body = ""
    for i in countUp(1, 100):
        body &= "Your mother was a hamster. "

    var buf = newMessage(nil)
    buf["Profile"] = "Insult"
    buf["Language"] = "French"
    buf.body = body

    let buffer = newFixseqOfCap[byte](1000)

    #for frameSize in 8..len(buf.body)+100:
    for frameSize in 100..100:
        #echo frameSize, " byte frames"
        var outCodec = newDeflater()
        var inCodec = newInflater()
        var frame = newFixseqOfCap[byte](frameSize)
        var msgOut = newMessageOut(buf)
        msgOut.number = MessageNo(1)
        var msgIn: MessageIn = nil
        while not msgOut.finished:
            msgOut.nextFrame(frame, outCodec)
            if msgIn == nil:
                msgIn = newIncomingRequest(byte(frame[1]), MessageNo(frame[0]), nil)
            discard msgIn.addFrame(byte(frame[1]), frame[2 .. ^1], buffer, inCodec)
        check msgIn.finished

        check msgIn["Profile"] == "Insult"
        check msgIn["Language"] == "French"
        check msgIn["Horse"] == ""
        check msgIn.property("Horse", "coconuts") == "coconuts"
        check msgIn.intProperty("Language", -1) == -1
        check msgIn.body == body

        var props: string
        for (k, v) in msgIn.properties:
            props &= k & "=" & v & ","
        check props == "Profile=Insult,Language=French,"
