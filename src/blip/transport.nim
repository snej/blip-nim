# transport.nim
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

import protocol, private/log
import asyncdispatch, asyncnet, asynchttpserver, news, strformat, strtabs, strutils


type Transport* = ref object of RootObj
    ## Abstract object that can send/receive BLIP frames.

method close*(t: Transport) {.base, async.} = discard
method disconnect*(t: Transport) {.base.} = discard
method canSend*(t: Transport): bool {.base.} = false
method canReceive*(t: Transport): bool {.base.} = false
method send*(t: Transport; frame: seq[byte]) {.base, async.} = discard
method receive*(t: Transport): Future[seq[byte]] {.base, async.} = discard


# WebSocketTransport

type WebSocketTransport* = ref object of Transport
    ## BLIP Transport implementation using a WebSocket
    socket: WebSocket
    sentClose: bool       # workaround for news not implementing WS close handshake
    receivedClose: bool

proc newWebSocketTransport*(socket: WebSocket): WebSocketTransport =
    ## Wraps a Transport around an existing WebSocket object.
    WebSocketTransport(socket: socket)

proc protocolName(subprotocol: string): string =
    result = BLIPWebSocketProtocol
    if subprotocol.len > 0:
        result &= "+" & subprotocol

proc newWebSocketTransport*(url: string, subprotocol: string =""): Future[WebSocketTransport] {.async.} =
    ## Creates a Transport by opening a WebSocket client connection.
    let protocol = protocolName(subprotocol)
    let socket = await newWebSocket(url, newStringTable({"Sec-WebSocket-Protocol": protocol}))
    return newWebSocketTransport(socket)

proc newWebSocketTransport*(request: Request, subprotocol: string =""): Future[WebSocketTransport] {.async.} =
    ## Creates a Transport by handling a WebSocket server request.
    var p: string
    if request.headers.hasKey("sec-webSocket-protocol"):
        p = request.headers["sec-webSocket-protocol"].strip()
    log Verbose, "Client Sec-WebSocket-Protocol = '{p}'"
    if p != protocolName(subprotocol):
        var response = "HTTP/1.1 400 WebSocket subprotocol missing or unknown\c\l\c\l"
        await request.client.send(response)
        return nil
    let socket = await newWebsocket(request)
    echo "socket.protocol = ", socket.protocol
    return newWebSocketTransport(socket)

method disconnect*(t: WebSocketTransport) =
    t.socket.close()

method close*(t: WebSocketTransport) {.async.} =
    if t.sentClose:
        t.socket.close()
    else:
        t.sentClose = true
        await t.socket.send("", Opcode.Close)


method canSend*(t: WebSocketTransport): bool =
    return t.socket.readyState == Open

method canReceive*(t: WebSocketTransport): bool =
    return t.socket.readyState == Open

method send*(t: WebSocketTransport; frame: seq[byte]) {.async.} =
    let f = t.socket.send(cast[string](frame), Opcode.Binary)
    yield f
    if f.failed and f.error.name != "WebSocketClosedError" and t.sentClose and t.receivedClose:
        log Info, "Transport closed cleanly"
    else:
        await f

method receive*(t: WebSocketTransport): Future[seq[byte]] {.async.} =
    while t.socket.readyState == Open:
        let packet = await t.socket.receivePacket()
        case packet.kind
            of Binary:
                return cast[seq[byte]](packet.data)
            of Close:
                t.receivedClose = true
                if t.sentClose:
                    t.socket.close()
                else:
                    t.sentClose = true
                    await t.socket.send("", Opcode.Close)
            else:
                log Warning, "Ignoring WebSocket frame of type {$packet.kind}"
    return @[]
