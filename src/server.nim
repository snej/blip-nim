# server.nim
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

## A very basic BLIP server program for testing purposes.

import blip, blip/private/log
import asyncdispatch, asynchttpserver, parseopt, strformat, strutils

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
            of "echo":      echoMessages = true
            of "log":       logMessages = true
            else:           fail &"Unknown flag '{key}'"
        else:
            fail("Unsupported parameter '{key}'")

    if path[0] != '/':
        fail "Path must begin with '/'"
    echo &"Starting Blip server at ws://localhost:{port}{path}, subprotocol '{subprotocol}'"

    var nReceived = 0

    proc handleMessage(msg: MessageIn) =
        nReceived += 1
        if logMessages:
            echo "Received message ", nReceived
            for (k, v) in msg.properties:
                echo "    ", k, " = ", v
            if msg.body.len < 500:
                echo "    \"", msg.body, "\""
            else:
                echo "    (", msg.body.len, " byte body)"
        elif nReceived mod 1000 == 0:
            echo "Received ", nReceived, " messages"

        if not msg.noReply and echoMessages:
            log Info, "Sending response..."
            var response = msg.createResponse
            response.body = msg.body
            response["IsReply"] = "true"
            response.sendNoReply()


    var server = newAsyncHttpServer()
    proc cb(req: Request) {.async, gcsafe.} =
        if req.url.path == path:
            var t = await newWebSocketTransport(req, subprotocol)
            if t == nil:
                log Error, "WebSocket handshake failed"
                return
            echo "Creating new Blip"
            var blip = newBlip(t)
            blip.setDefaultHandler(proc(msg: MessageIn) = handleMessage(msg))
            await blip.run()
            echo "...Closed Blip\c\l"
        else:
            await req.respond(Http404, "Nope", newHttpHeaders())

    waitFor server.serve(Port(port), cb)
