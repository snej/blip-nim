# client.nim
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

## A very basic BLIP client program for testing purposes.

import blip, blip/[message, transport]
import asyncdispatch, parseopt, strformat, strutils

when isMainModule:
    proc fail(msg: string) =
        echo "Error: ", msg
        quit(1)

    var port = 9001
    var path = "/"
    var subprotocol = ""
    var echoMessages = false
    var logMessages = true

    for kind, key, val in getopt(shortNoVal = {'v'}, longNoVal = @["verbose", "echo", "log"]):
        case kind
        of cmdShortOption:
            case key
            of "v":         setBLIPLogLevel(3)
            else:           fail &"Unknown flag '{key}'"
        of cmdLongOption:
            case key
            of "verbose":   setBLIPLogLevel(3)
            of "port":      port = val.parseInt()
            of "path":      path = val
            of "protocol":  subprotocol = val
            of "log":       logMessages = true
            else:           fail &"Unknown flag '{key}'"
        else:
            fail("Unsupported parameter '{key}'")

    if path[0] != '/':
        fail "Path must begin with '/'"
    echo &"Connecting to server at ws://localhost:{port}{path}"

    proc handleResponse(msg: MessageIn) =
        if logMessages:
            echo "Got a response:"
            for (k, v) in msg.properties:
                echo "    ", k, " = ", v
            if msg.body.len < 500:
                echo "    \"", msg.body, "\""
            else:
                echo "    (", msg.body.len, "-byte body)"

    proc Run(blip: Blip) {.async.} =
        var n = 0
        while true:
            if logMessages or (n mod 1000 == 0):
                echo "Sending message ", n
            var msg = blip.newRequest("Test")
            msg["Testing"] = "123"
            msg.body = "This is test message #" & $n & ". "
            for i in countUp(1, 15):
                msg.body = msg.body & msg.body
            n += 1
            let f = msg.send()
            f.addCallback(proc (response: Future[MessageIn]) =
                if logMessages:
                    let body = response.read.body
                    if body.len < 500:
                        echo "Got response with body '", response.read.body, "'"
                    else:
                        echo "Got response with ", msg.body.len, "-byte body"
            )
            #await sleepAsync(0)
            break

    proc DoIt() {.async.} =
        var ws = await newWebSocketTransport(&"ws://127.0.0.1:{port}{path}", subprotocol)
        echo "Creating new Blip"
        var blip = newBlip(ws)
        await blip.run() and Run(blip)
        echo "...Closed Blip"

    waitfor DoIt()
