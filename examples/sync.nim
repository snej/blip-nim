# sync.nim
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

const PROFILE = false
when PROFILE:
    import nimprof
    disableProfiling()

import blip, blip/private/log
import asyncdispatch, asynchttpserver, deques, json, os, parseopt, random, strformat, strutils, times
import std/monotimes


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
    log Error, "Error: {msg}"
    quit(1)


proc main() =

    var blip: Blip

    #######################################
    # FAKE DATABASE
    #######################################

    type Sequence = int

    type Document = object
        id: string
        rev: string
        body: string
        seq: Sequence

    const kBogusRev = "1-123456781234567812345678"

    var db = newSeqOfCap[Document](1000)

    proc addDoc(body: string) =
        let seq = len(db) + 1
        db.add(Document(id: &"doc-{seq}", rev: kBogusRev, body: body, seq: seq))


    proc initDB(dbPath: string =""): int =
        for line in lines(expandTilde(dbPath)):
            addDoc(line)
        let docCount = db.len
        log Info, "Read {docCount} docs from {dbPath}."
        return docCount

    proc initDB(docCount: int; docSize: int) =
        let bodyCount = int((docSize - 20)/7)
        for seq in 1..docCount:
            var r = newSeqOfCap[int](bodyCount)
            for i in 0..<bodyCount:
                r.add(rand(1000000))
            addDoc($( %* {"my_number_is": seq, "random": r} ))

        log Info, "Created {docCount} random docs, each approx. {docSize} bytes."



    #######################################
    # PUSHER
    #######################################

    const changesBatchSize = 200

    var changesPipe = makePipe[Sequence](changesBatchSize * 20)       # Number of revs in unreplied `changes` messages
    var revsPipe    = makePipe[Sequence](10 * 1024 * 1024)    # Bytes of `rev` messages in flight

    proc sendRevNow(seq: Sequence) =
        var msg = blip.newRequest("rev")
        let doc = db[seq]
        let weight = 100 + doc.id.len + doc.rev.len + doc.body.len
        if int(seq) mod 100 == 0:
            log Verbose, "<-- sending rev #{seq}: {doc.id} {doc.rev} (+{doc.body.len})"
        revsPipe.beginPending(weight)
        msg["id"] = doc.id
        msg["rev"] = doc.rev
        msg["sequence"] = $seq
        #msg["history"] = ""
        msg.compressed = true
        msg.body = doc.body
        let f = msg.send()
        f.addCallback(proc (rf: Future[MessageIn]) =
            #echo &"--> response to rev {seq}"
            revsPipe.endPending(weight)
            let response = rf.read
            if response.isError:
                let domain, code = response.error
                log Error, "--> Error reply to rev {seq}: {domain} {code} '{response.body}"
        )

    proc sendRevsTask {.async.} =
        while true:
            let seq = await revsPipe.pop()
            if seq > 0:
                sendRevNow(seq)
            else:
                break
        log Info, "Finished sendRevsTask"


    proc handleChangesResponse(response: MessageIn; firstSeq: Sequence) {.async.} =
        log Info, "--> response to changes ({firstSeq}...)"
        if response.isError:
            let domain, code = response.error
            log Error, "Error reply to 'changes' ({firstSeq}) is error {domain} {code} '{response.body}"
            return
        var seq = firstSeq
        var n = 0
        for item in parseJson(response.body).getElems():
            if item.kind == JArray:
                #let revs = item.getElems()
                revsPipe.push(seq)
                n += 1
            seq += 1
        log Info, "        queued {n} of {seq - firstSeq} revs"

    proc sendChangesNow(reqType: string, firstSeq: Sequence) =
        log Info, "<-- sending {reqType} ({firstSeq}...{firstSeq+199})"
        let sendSeq = (reqType == "changes")
        let endSeq = min(firstSeq + changesBatchSize, db.len)
        let weight = endSeq - firstSeq
        changesPipe.beginPending(weight)
        var msg = blip.newRequest(reqType)
        msg.compressed = true
        msg.priority = Urgent
        when true:
            var revs = newStringOfCap(100 * (endSeq - firstSeq))
            revs.add('[')
            for seq in firstSeq ..< endSeq:
                if seq > firstSeq:
                    revs.add(',')
                let doc = db[seq]
                revs.add('[')
                if sendSeq:
                    revs.add($seq)
                    revs.add(',')
                revs.add('"')
                revs.add(doc.id)    #TODO: JSON escapes
                revs.add("\",\"")
                revs.add(doc.rev)    #TODO: JSON escapes
                revs.add("\"]")
            revs.add(']')
            msg.body = revs
        else: # (Using JSON API is slower)
            var revs = newJArray()
            for seq in firstSeq ..< endSeq:
                revs.add( %* [seq, getDocID(seq), getRevID(seq)] )
            msg.body = $revs
        let f = msg.send()
        f.addCallback(proc (response: Future[MessageIn]) =
            asyncCheck handleChangesResponse(response.read(), firstSeq)
            changesPipe.endPending(weight)
        )

    proc sendCaughtUp(reqType: string) =
        log Info, "<-- sending {reqType} ()"
        var msg = blip.newRequest(reqType)
        msg.body = "[]"
        let f = msg.send()
        f.addCallback(proc =
            log Info, "--> response to {msg}()"
        )


    proc sendChangesTask(reqType: string) {.async.} =
        while true:
            let firstSeq = await changesPipe.pop()
            if firstSeq > 0:
                sendChangesNow(reqType, firstSeq)
            else:
                sendCaughtUp(reqType)
                break
        log Info, "Finished sendChangesTask"


    proc sendChanges(reqType: string) {.async.} =
        var seq = 1
        while seq <= db.len:
            changesPipe.push(seq)
            seq += changesBatchSize
        changesPipe.push(0) # EOF
        asyncCheck sendRevsTask()
        await sendChangesTask(reqType)
        log Info, "Finished sendChanges"


    proc handleSubChanges(req: MessageIn) =
        log Info, "--> subChanges"
        asyncCheck sendChanges("changes")
        req.createResponse().sendNoReply()


    #######################################
    # PULLER
    #######################################

    var pullCaughtUp = false
    var pendingRevCount = 0
    var revsReceived = 0

    proc checkDonePulling() =
         if pullCaughtUp and pendingRevCount == 0:
            log Info, "All revs have been pulled!"
            blip.closeWhenIdle()

    proc handleChanges(req: MessageIn) =
        let questions = parseJson(req.body).getElems()
        if questions.len == 0:
            log Info, "--> {req.profile} (caught up!)"
            pullCaughtUp = true
            checkDonePulling()
        else:
            if req.profile == "changes":
                log Info, "--> changes ({questions.len} revs from {questions[0][0]})"
            else:
                log Info, "--> {req.profile} ({questions.len} revs)"

        if not req.noReply:
            let off = (if req.profile == "changes": 1 else: 0)
            var answers = newJArray()
            for item in questions:
                let change = item.getElems()
                let docID = change[off + 0]
                let revID = change[off + 1]
                answers.add(newJArray())
                pendingRevCount += 1
            var response = req.createResponse()
            response.compressed = true
            response.body = $answers
            response.sendNoReply()

    proc handleRev(req: MessageIn) =
        let docID = req["id"]
        let revID = req["rev"]
        log Verbose, "--> rev ('{docID}' {revID}, {req.body.len} bytes)"
        revsReceived += 1
        if revsReceived mod 100 == 0:
            log Info, "--> Received {revsReceived} revisions"
        if not req.noReply:
            let response = req.createResponse()
            response.sendNoReply()
        pendingRevCount -= 1
        checkDonePulling()

    proc handleNoRev(req: MessageIn) =
        let docID = req["id"]
        let revID = req["rev"]
        log Info, "--> &norev ('{docID}' {revID})"
        if not req.noReply:
            let response = req.createResponse()
            response.sendNoReply()
        pendingRevCount -= 1
        checkDonePulling()

    # Active pull (client):

    proc subscribeToChanges() =
        var msg = blip.newRequest("subChanges")
        msg.compressed = true
        let f = msg.send()
        f.addCallback(proc (rf: Future[MessageIn]) =
            let response = rf.read
            if response.isError:
                let domain, code = response.error
                log Error, "--> Error reply to subChanges: {domain} {code} '{response.body}"
        )


    #######################################
    # COMMON
    #######################################

    proc handleGetCheckpoint(req: MessageIn) =
        log Info, "--> getCheckpoint"
        req.replyWithError("HTTP", 404, "Missing checkpoint")

    proc handleSetCheckpoint(req: MessageIn) =
        log Info, "--> setCheckpoint"
        req.createResponse().sendNoReply()


    #######################################
    # MAIN
    #######################################

    # Parse options from command-line arguments:
    var port = -1
    var clientURL = ""
    var pulling = false
    var dbName = "db"
    var subprotocol = "CBMobile_2"
    var compression = -1
    var docCount = 0
    var docSize = 1000
    var dbPath: string

    setBLIPLogLevel(2) # Info

    proc usage() =
        echo "sync: Trivial Couchbase Mobile sync client/server"
        echo "    --push URL       : Run in client mode, pushing local db to URL"
        echo "    --pull URL       : Run in client mode, pulling from URL"
        echo "    --server PORT    : Run in server mode, listening on port PORT"
        echo "    --docs N         : Number of random docs in db"
        echo "    --docSize N      : Size in bytes of random docs [default: ", docSize, "]"
        echo "    --json PATH      : Path to data file, in JSON object-per-line format"
        echo "    --name NAME      : Server database name [default: '", dbName, "'']"
        echo "    --compression N  : Zlib compression level, 0..9"
        echo "    --verbose  or -v : Enable verbose logging"
        echo "    --help           : Display this message"
        echo "Either --push, -pull, or --server must be given."
        echo "Either --docs or --json must be given, unless --pull is given."
        quit()

    for kind, key, val in getopt(shortNoVal = {'v'}, longNoVal = @["verbose", "help"]):
        case kind
        of cmdShortOption:
            case key
            of "v":         setBLIPLogLevel(3)
            else:           fail &"Unknown flag '{key}'"
        of cmdLongOption:
            case key
            of "help":      usage()
            of "push":      clientURL = val; pulling = false
            of "pull":      clientURL = val; pulling = true
            of "server":    port = val.parseInt()
            of "name":      dbName = val
            of "compression": compression = val.parseInt()
            of "docs":      docCount = val.parseInt()
            of "docSize":   docSize = val.parseInt()
            of "json":      dbPath = val
            of "verbose":   setBLIPLogLevel(3)
            else:           fail &"Unknown flag '{key}'"
        else:
            fail("Unsupported parameter '{key}'")

    if (clientURL == "" and port == -1) or (clientURL != "" and port != -1):
        fail "Please use one of --push, -pull, or --server."

    # Create the "database":
    if not pulling:
        log Info, "Initializing 'database'..."
        if dbPath != "":
            docCount = initDB(dbPath)
        elif docCount > 0:
            initDB(docCount, docSize)
        else:
            fail "Please use either the --docs or the --json flag."


    proc runBlip() {.async.} =
        # Add sync protocol handlers:
        blip.addHandler("getCheckpoint", handleGetCheckpoint)
        blip.addHandler("setCheckpoint", handleSetCheckpoint)
        blip.addHandler("subChanges",    handleSubChanges)
        blip.addHandler("changes",       handleChanges)
        blip.addHandler("proposeChanges",handleChanges)
        blip.addHandler("rev",           handleRev)
        blip.addHandler("norev",         handleNoRev)

        changesPipe.clear()
        revsPipe.clear()

        await blip.run()
        blip = nil
        log Info, "...Closed Blip\c\l"
        changesPipe.clear()
        revsPipe.clear()
        log Info, "Memory: {getOccupiedMem()} used, {getFreeMem()} free, {getTotalMem()} total"


    if clientURL == "":
        # Run the server:
        let uriPath = &"/{dbName}/_blipsync"
        log Info, "Starting sync server at ws://localhost:{port}{uriPath} ... zlib compression level {compression}"
        var server = newAsyncHttpServer()

        proc cb(req: Request) {.async, gcsafe.} =
            if req.url.path == uriPath:
                try:
                    var t = await newWebSocketTransport(req, subprotocol)
                    if t == nil:
                        log Error, "WebSocket handshake failed"
                        return
                    log Info, "Incoming connection!"
                    when PROFILE:
                        enableProfiling()
                    blip = newBlip(t, compressionLevel=compression)
                    await runBlip()
                except CatchableError as x:
                    logException x, "in HTTP handler"
                    log Error, "Exception: {x.msg}"
                when PROFILE:
                    quit()
            else:
                await req.respond(Http404, "Nope", newHttpHeaders())
        waitFor server.serve(Port(port), cb)


    else:
        # Run the client:
        log Info, "Connecting to sync server at {clientURL} ... zlib compression level {compression}"
        proc client() {.async.} =
            var ws = await newWebSocketTransport(clientURL, subprotocol)
            blip = newBlip(ws, compressionLevel=compression)

            when PROFILE:
                enableProfiling()
            let cpuStart = cpuTime()
            let monoStart = getMonoTime()
            log Info, "START!"

            proc runClient() {.async.} =
                if pulling:
                    subscribeToChanges()
                else:
                    await sendChanges("proposeChanges")
                    blip.closeWhenIdle()

            await runBlip() and runClient()
            log Info, "Time:     {getMonoTime() - monoStart}"
            log Info, "CPU time: {cpuTime() - cpuStart} sec"

        waitfor client()

main()
