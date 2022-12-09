# sync_server.nim
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

# SYNC PROTOCOL DOCUMENTATION:
# https://github.com/couchbase/couchbase-lite-core/blob/master/modules/docs/pages/replication-protocol.adoc

import blip, blip/private/log
import asyncdispatch, asynchttpserver, deques, json, os, parseopt, random, strformat, strutils

const PROFILE = false
when PROFILE:
    import nimprof


#######################################
# PIPE CLASS
#######################################

type Pipe[T] = object
    ## A rate-limited queue of pending work items, that tracks the 'weight' of current work
    ## and stops allowing items to be popped while the weight is too high.
    queue: Deque[T]
    pending: int
    maxPending {.requiresInit.}: int
    waiter: Future[T]

func canPop[T](pipe: Pipe[T]): bool =
    pipe.queue.len > 0 and pipe.pending < pipe.maxPending

proc completePop[T](pipe: var Pipe[T], f: Future[T]) =
    assert pipe.canPop
    pipe.waiter = nil
    f.complete(pipe.queue.popFirst())

proc maybeWakeWaiter[T](pipe: var Pipe[T]) =
    if pipe.waiter != nil and pipe.canPop:
        pipe.completePop(pipe.waiter)

proc makePipe*[T](maxPending: int): Pipe[T] =
    ## Creates a new Pipe
    Pipe[T](maxPending: maxPending)

proc push*[T](pipe: var Pipe[T], seq: T) =
    ## Adds an item to the Pipe.
    pipe.queue.addLast(seq)
    pipe.maybeWakeWaiter()

proc pop*[T](pipe: var Pipe[T]): Future[T] =
    ## Removes & returns the oldest item from the Pipe.
    ## Waits if the Pipe is empty or there is too much pending work.
    var f = newFuture[T]("Pipe.pop")
    if pipe.canPop:
        pipe.completePop(f)
    else:
        assert pipe.waiter == nil
        pipe.waiter = f
    return f

proc beginPending*[T](pipe: var Pipe[T], weight: int) =
    ## Tells the Pipe that work is in progress (usually due to working on an item just popped.)
    assert weight >= 0
    pipe.pending += weight

proc endPending*[T](pipe: var Pipe[T], weight: int) =
    ## Tells the Pipe that work has ended, usually because an item has been processed somehow.
    ## This potentially allows another item to be popped, completing a waiting pop() call.
    assert weight >= 0
    assert weight <= pipe.pending
    pipe.pending -= weight
    pipe.maybeWakeWaiter()

proc clear*[T](pipe: var Pipe[T]) =
    pipe.queue.clear()
    pipe.pending = 0
    pipe.waiter = nil


#######################################


proc fail(msg: string) =
    echo "Error: ", msg
    quit(1)


proc main() =

    var blip: Blip

    #######################################
    # FAKE DATABASE
    #######################################

    type Sequence = int

    var db = newSeqOfCap[string](1000)

    proc initDB(dbPath: string =""): int =
        for line in lines(expandTilde(dbPath)):
            db.add(line)
        let docCount = db.len
        echo &"Read {docCount} docs from {dbPath}."
        return docCount

    proc initDB(docCount: int; docSize: int) =
        let bodyCount = int((docSize - 20)/7)
        for seq in 1..docCount:
            var r = newSeqOfCap[int](bodyCount)
            for i in 0..<bodyCount:
                r.add(rand(1000000))
            db.add( $( %* {"my_number_is": seq, "random": r} ) )
        echo &"Created {docCount} random docs, each approx. {docSize} bytes."


    proc getDocID(seq: Sequence): string = &"doc-{seq}"
    proc getRevID(seq: Sequence): string = "1-123456781234567812345678"
    proc getDocBody(seq: Sequence): string = db[seq - 1]



    #######################################
    # PUSHER
    #######################################

    var changesPipe = makePipe[Sequence](200 * 5)       # Number of revs in unreplied `changes` messages
    var revsPipe    = makePipe[Sequence](200 * 1024)    # Bytes of `rev` messages in flight

    proc sendRevNow(seq: Sequence) =
        #echo &"    sending rev {seq} -->"
        var msg = blip.newRequest("rev")
        let docID = getDocID(seq)
        let revID = getRevID(seq)
        let body = getDocBody(seq)
        let weight = 100 + docID.len + revID.len + body.len
        revsPipe.beginPending(weight)
        msg["id"] = docID
        msg["rev"] = revID
        msg["sequence"] = $seq
        #msg["history"] = ""
        msg.compressed = true
        msg.body = body
        let f = msg.send()
        f.addCallback(proc (response: Future[MessageIn]) =
            #echo &"--> response to rev {seq}"
            revsPipe.endPending(weight)
        )

    proc sendRevsTask {.async.} =
        while true:
            let seq = await revsPipe.pop()
            if seq > 0:
                sendRevNow(seq)
            else:
                break
        echo "Finished sendRevsTask"


    proc handleChangesResponse(response: MessageIn; firstSeq: Sequence) {.async.} =
        echo &"--> response to changes ({firstSeq}...)"
        if response.isError:
            echo "(ignoring 'changes' error response)"
            return
        var seq = firstSeq
        for item in parseJson(response.body).getElems():
            if item.kind == JArray:
                #let revs = item.getElems()
                revsPipe.push(seq)
            seq += 1

    proc sendChangesNow(firstSeq: Sequence) =
        echo &"    changes ({firstSeq}...{firstSeq+199}) -->"
        var revs = newJArray()
        let lastSeq = min(firstSeq+200, db.len)
        let weight = lastSeq - firstSeq
        changesPipe.beginPending(weight)
        for seq in firstSeq ..< lastSeq:
            revs.add( %* [seq, getDocID(seq), getRevID(seq)] )
        var msg = blip.newRequest("changes")
        msg.compressed = true
        msg.body = $revs
        let f = msg.send()
        f.addCallback(proc (response: Future[MessageIn]) =
            asyncCheck handleChangesResponse(response.read(), firstSeq)
            changesPipe.endPending(weight)
        )

    proc sendCaughtUp() =
        echo &"    changes () -->"
        var msg = blip.newRequest("changes")
        msg.compressed = true
        msg.body = "[]"
        asyncCheck msg.send()

    proc sendChangesTask {.async.} =
        while true:
            let firstSeq = await changesPipe.pop()
            if firstSeq > 0:
                sendChangesNow(firstSeq)
            else:
                sendCaughtUp()
                break
        echo "Finished sendChangesTask"


    # BLIP request handlers:

    proc subChanges(req: MessageIn) =
        echo "--> subChanges"
        asyncCheck sendRevsTask()
        asyncCheck sendChangesTask()
        var msgNo = 1
        while msgNo <= db.len:
            changesPipe.push(msgNo)
            msgNo += 200
        changesPipe.push(0) # End
        req.createResponse().sendNoReply()


    #######################################
    # PULLER
    #######################################

    proc changes(req: MessageIn) =
        echo "--> changes"
        req.replyWithError("BLIP", 409, "Use subChanges instead")

    proc proposeChanges(req: MessageIn) =
        let questions = parseJson(req.body).getElems()
        echo &"--> proposeChanges ({questions.len} revs)"
        var answers = newJArray()
        # for item in questions:
        #     let change = item.getElems()
        #     let docID = change[0]
        #     let revID = change[1]
        #     # let serverRevID = change[2] #optional
        #     # let bodySize = change[3].parseInt() #optional
        #     answers.add(0)
        var response = req.createResponse()
        response.compressed = true
        response.body = $answers
        response.sendNoReply()

    proc handleRev(req: MessageIn) =
        #let docID = req["id"]
        #let revID = req["rev"]
        #echo &"--> rev ('{docID}' {revID})"
        if not req.noReply:
            let response = req.createResponse()
            response.sendNoReply()

    proc handleNoRev(req: MessageIn) =
        let docID = req["id"]
        let revID = req["rev"]
        echo "--> &norev ('{docID}' {revID})"
        if not req.noReply:
            let response = req.createResponse()
            response.sendNoReply()


    #######################################
    # COMMON
    #######################################

    proc getCheckpoint(req: MessageIn) =
        echo "--> getCheckpoint"
        req.replyWithError("HTTP", 404, "Missing checkpoint")

    proc setCheckpoint(req: MessageIn) =
        echo "--> setCheckpoint"
        req.createResponse().sendNoReply()


    #######################################
    # SERVER
    #######################################

    # Parse options from command-line arguments:
    var port = 4984
    var dbName = "db"
    var subprotocol = "CBMobile_2"
    var compression = -1
    var docCount = 0
    var docSize = 1000
    var dbPath: string

    proc usage() =
        echo "sync_server: Trivial Couchbase Mobile sync server"
        echo "    --compression N  : Zlib compression level, 0..9"
        echo "    --docs N         : Number of random docs in db"
        echo "    --docSize N      : Size in bytes of random docs [default: ", docSize, "]"
        echo "    --json PATH      : Path to data file, in JSON-lines format"
        echo "    --name NAME      : Database name, in HTTP path [default: '", dbName, "'']"
        echo "    --port N         : Port to listen on [default: ", port, "]"
        echo "    --verbose        : Enable verbose logging"
        echo "    --help           : Display this message"
        echo "Either --docs or --json must be given."
        quit()

    for kind, key, val in getopt(shortNoVal = {'v'}, longNoVal = @["verbose", "echo", "help"]):
        case kind
        of cmdShortOption:
            case key
            of "v":         setBLIPLogLevel(3)
            else:           fail &"Unknown flag '{key}'"
        of cmdLongOption:
            case key
            of "help":      usage()
            of "port":      port = val.parseInt()
            of "name":      dbName = val
            of "compression": compression = val.parseInt()
            of "docs":      docCount = val.parseInt()
            of "docSize":   docSize = val.parseInt()
            of "json":      dbPath = val
            of "verbose":   setBLIPLogLevel(3)
            else:           fail &"Unknown flag '{key}'"
        else:
            fail("Unsupported parameter '{key}'")


    # Create the "database":
    if dbPath != "":
        docCount = initDB(dbPath)
    elif docCount > 0:
        initDB(docCount, docSize)
    else:
        echo "Please give either the --docs or the --json flag."
        quit(1)


    # Run the server:
    let uriPath = &"/{dbName}/_blipsync"
    echo &"Starting sync server at ws://localhost:{port}{uriPath} ... zlib compression level {compression}"
    var server = newAsyncHttpServer()

    proc cb(req: Request) {.async, gcsafe.} =
        if req.url.path == uriPath:
            try:
                var t = await newWebSocketTransport(req, subprotocol)
                if t == nil:
                    log Error, "WebSocket handshake failed"
                    return
                echo "Incoming connection!"
                when PROFILE:
                    enableProfiling()
                blip = newBlip(t, compressionLevel=compression)

                # Add sync protocol handlers:
                blip.addHandler("getCheckpoint", getCheckpoint)
                blip.addHandler("setCheckpoint", setCheckpoint)
                blip.addHandler("subChanges",    subChanges)
                blip.addHandler("changes",       changes)
                blip.addHandler("proposeChanges",proposeChanges)
                blip.addHandler("rev",           handleRev)
                blip.addHandler("norev",         handleNoRev)

                changesPipe.clear()
                revsPipe.clear()

                await blip.run()
                blip = nil
                echo "...Closed Blip\c\l"
                changesPipe.clear()
                revsPipe.clear()
                echo "Memory: ", getOccupiedMem(), " used, ", getFreeMem(), " free, ", getTotalMem(), " total"
            except Exception as x:
                logException x, "in HTTP handler"
                echo x.msg
            when PROFILE:
                quit()
        else:
            await req.respond(Http404, "Nope", newHttpHeaders())

    waitFor server.serve(Port(port), cb)

main()
