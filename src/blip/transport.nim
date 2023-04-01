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

import private/[fixseq, log, protocol]
import asyncdispatch, asyncnet, asynchttpserver, news, strformat, strtabs, strutils
import std/monotimes, std/times

proc abstract() = raise newException(Defect, "Unimplemented abstract method")

type Transport* = ref object of RootObj
    ## Abstract object that can send/receive BLIP frames.

method close*(t: Transport) {.base, async.} =
    ## Requests an orderly close of the Transport.
    abstract()
method disconnect*(t: Transport) {.base.} =
    ## Immediately disconnects without an orderly close.
    abstract()
method canSend*(t: Transport): bool {.base.} =
    ## Returns true if the Transport is open for sending frames.
    abstract()
method canReceive*(t: Transport): bool {.base.} =
    ## Returns true if the Transport is open for receiving frames.
    abstract()
method send*(t: Transport; frame: fixseq[byte]) {.base, async.} =
    ## Sends a frame. If there is backpressure this method will block until the frame is sent.
    abstract()
method receive*(t: Transport): Future[fixseq[byte]] {.base, async.} =
    ## Waits for and returns the next frame.
    ## If the transport was closed cleanly, returns an empty frame.
    ## Other errors are thrown as exceptions (including unexpected disconnects.)
    abstract()


# WebSocketTransport

type WebSocketTransport* {.requiresInit.} = ref object of Transport
    ## BLIP Transport implementation using a WebSocket
    socket: WebSocket
    sendingBytes: int64
    sendWaiter: Future[void]
    sendError: ref Exception
    # Metrics:
    bytesSent: uint64
    bytesRcvd: uint64
    startTime: MonoTime
    sendWaitStarted: MonoTime
    totalSendWaitTime: Duration
    totalRcvWaitTime: Duration
    sendIdleStarted: MonoTime
    totalIdleTime: Duration

const kMaxSendingBytes = 500 * 1024
    ## Max number of bytes written to the socket that haven't been acknowledged yet
    ## (by the WebSocket's send future completing.)

proc newWebSocketTransport*(socket: WebSocket): WebSocketTransport =
    ## Wraps a Transport around an existing WebSocket object.
    WebSocketTransport(socket: socket, startTime: getMonoTime())

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
    return newWebSocketTransport(socket)

method disconnect*(t: WebSocketTransport) =
    log Warning, "Disconnecting WebSocket"
    t.socket.close()

method close*(t: WebSocketTransport) {.async.} =
    let openTime = getMonoTime() - t.startTime
    let openMs = openTime.inMilliseconds
    let sbps = int(t.bytesSent) * 1000 / int(openMs)
    let rbps = int(t.bytesRcvd) * 1000 / int(openMs)
    log Info, "Closing WebSocket ...\nSent {t.bytesSent} bytes in {openMs} ms, {sbps:.0f} bytes/sec ... send buffer was empty for {t.totalIdleTime.inMilliseconds} ms, full for {t.totalSendWaitTime.inMilliseconds} ms\nReceived {t.bytesRcvd} bytes, {rbps:.0f} bytes/sec ... receiver spent {t.totalRcvWaitTime.inMilliseconds} ms waiting"
    await t.socket.shutdown()

method canSend*(t: WebSocketTransport): bool =
    return t.socket.readyState == Open

method canReceive*(t: WebSocketTransport): bool =
    return t.socket.readyState == Open or t.socket.readyState == Closing

method send*(t: WebSocketTransport; frame: fixseq[byte]): Future[void] =
    result = newFuture[void]("WebSocketTransport.send")

    if t.sendError != nil:
        result.fail(t.sendError)
        return

    if t.sendingBytes == 0 and t.sendIdleStarted.ticks != 0:
        let idleTime = getMonoTime() - t.sendIdleStarted
        t.totalIdleTime += idleTime
        let idleUs = idleTime.inMicroseconds
        if idleUs > 10000:
            log Verbose, "Transport send was idle for {idleUs} µs"

    let byteCount = frame.len
    t.sendingBytes += byteCount
    t.bytesSent += uint64(byteCount)

    log Debug, "Transport sending {byteCount} bytes"
    t.socket.send(frame.toString, Opcode.Binary).addCallback proc(f: Future[void]) =
        # Callback when frame has been sent: decrement sendingBytes and maybe complete a Future
        if not f.failed:
            t.sendingBytes -= byteCount
            let waiter = t.sendWaiter
            if t.sendingBytes < kMaxSendingBytes and waiter != nil:

                let wait = getMonoTime() - t.sendWaitStarted
                t.totalSendWaitTime += wait
                if wait.inMilliseconds > 1:
                    log Info, "Transport is un-blocking after {wait.inMicroseconds} µs (sendingBytes={t.sendingBytes})"

                t.sendWaiter = nil
                waiter.complete()

            if t.sendingBytes == 0:
                log Verbose, "Transport is idle!"
                t.sendIdleStarted = getMonoTime()
        else:
            if f.error.name == "WebSocketClosedError" and t.socket.readyState >= Closing:
                log Verbose, "Transport closed cleanly"
            else:
                logException f.error, "Transport send"
            if t.sendError == nil:
                t.sendError = f.error
            if t.sendWaiter != nil:
                t.sendWaiter.fail(f.error)

    if t.sendingBytes < kMaxSendingBytes:
        result.complete()
    else:
        assert t.sendWaiter == nil
        t.sendWaiter = result
        t.sendWaitStarted = getMonoTime()
        log Verbose, "Transport is blocking (sendingBytes={t.sendingBytes})"


method receive*(t: WebSocketTransport): Future[fixseq[byte]] {.async.} =
    while t.socket.readyState == Open:
        let start = getMonoTime()
        let packet = await t.socket.receivePacket()
        let rcvTime = getMonoTime() - start
        t.totalRcvWaitTime += rcvTime
        if rcvTime.inMilliseconds >= 100:
            log Warning, "Transport.receive waited {rcvTime.inMicroseconds} µs; total is {t.totalRcvWaitTime}"

        t.bytesRcvd += uint64(packet.data.len)
        case packet.kind
            of Binary:
                return packet.data.toFixseq
            of Ping:
                discard
            of Close:
                break
            else:
                log Warning, "Ignoring WebSocket frame of type {$packet.kind}"
    # On close, return an empty frame
