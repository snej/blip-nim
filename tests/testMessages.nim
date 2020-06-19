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

import blip/message, blip/protocol, blip/private/crc32, blip/private/log

let kFrame1 = @['\x40',  # flags
                '\x01',  # message#
                '\x1F',  # properties length
                'P', 'r', 'o', 'f', 'i', 'l', 'e', '\x00',
                'I', 'n', 's', 'u', 'l', 't', '\x00',
                'L', 'a', 'n', 'g', 'u', 'a', 'g', 'e', '\x00',
                'F', 'r', 'e', 'n', 'c', 'h', '\x00',
                'Y', 'o', 'u', 'r', ' ', 'm',
                '\x1A', '?', 'w', '\xBF'] # checksum
let kFrame2 = @['\x00',  # flags
                '\x01',  # message#
                'o', 't', 'h', 'e', 'r', ' ', 'w', 'a', 's', ' ',
                'a', ' ', 'h', 'a', 'm', 's', 't', 'e', 'r',
                '\xD4', 'A', '\xEB', '\xDE'] # checksum

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

    var checksum: CRC32
    var frame = msg.nextFrame(44, checksum)
    check frame.len == 44
    echo cast[seq[char]](frame)
    check cast[seq[char]](frame) == kFrame1
    check not msg.finished

    frame = msg.nextFrame(44, checksum)
    echo cast[seq[char]](frame)
    check cast[seq[char]](frame) == kFrame2
    check msg.finished


test "Incoming Message":
    CurrentLogLevel = Verbose

    var checksum: CRC32
    let msg = newIncomingRequest(byte(kFrame1[0]), MessageNo(kFrame1[1]), nil)
    msg.addFrame(byte(kFrame1[0]), cast[seq[byte]](kFrame1[2 .. ^1]), checksum)
    check not msg.finished
    msg.addFrame(byte(kFrame2[0]), cast[seq[byte]](kFrame2[2 .. ^1]), checksum)
    check msg.finished

    check msg.body == "Your mother was a hamster"
    check msg["Profile"] == "Insult"
    check msg["Language"] == "French"
    check msg["Horse"] == ""
    check msg.property("Horse", "coconuts") == "coconuts"
    check msg.intProperty("Language", -1) == -1


test "Frame Sizes":
    CurrentLogLevel = Warning

    var body = ""
    for i in countUp(1, 100):
        body &= "Your mother was a hamster. "

    var buf = newMessage(nil)
    buf["Profile"] = "Insult"
    buf["Language"] = "French"
    buf.body = body

    for frameSize in 8..len(buf.body)+100:
        #echo frameSize, " byte frames"
        var outSum: CRC32
        var inSum: CRC32
        var msgOut = newMessageOut(buf)
        msgOut.number = MessageNo(1)
        var msgIn: MessageIn = nil
        while not msgOut.finished:
            var frame = msgOut.nextFrame(frameSize, outSum)
            if msgIn == nil:
                msgIn = newIncomingRequest(byte(frame[0]), MessageNo(frame[1]), nil)
            msgIn.addFrame(byte(frame[0]), cast[seq[byte]](frame[2 .. ^1]), inSum)
        check msgIn.finished

        check msgIn.body == body
        check msgIn["Profile"] == "Insult"
        check msgIn["Language"] == "French"
        check msgIn["Horse"] == ""
        check msgIn.property("Horse", "coconuts") == "coconuts"
        check msgIn.intProperty("Language", -1) == -1

        var props: string
        for (k, v) in msgIn.properties:
            props &= k & "=" & v & ","
        check props == "Profile=Insult,Language=French,"
