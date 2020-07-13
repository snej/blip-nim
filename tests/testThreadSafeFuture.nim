# testThreadSafeFuture.nim

import blip/private/threadSafeFuture
import asyncdispatch, asyncfutures, os, threadpool, unittest


let mainThreadID = getThreadId()


proc testThreadSafe() {.async.} =
    echo "---- Testing threadSafe ----"
    let f1 = newFuture[int]()

    proc completeElsewhere(f: Future[int]) {.thread.} =
        os.sleep 100
        echo "testThreadSafe: On thread ", getThreadID(), "!"
        assert getThreadID() != mainThreadID
        f.complete(49)

    spawn completeElsewhere(threadSafe(f1))

    var callbackResult = -1
    f1.addCallback proc(f: Future[int]) =
        echo "testThreadSafe: Callback! got ", f.read()
        assert getThreadID() == mainThreadID
        callbackResult = f.read()

    echo "testThreadSafe: waiting on thread ", mainThreadID, "..."
    var waitResult = await f1
    echo "testThreadSafe: checking callbackResult ", callbackResult, ", waitResult ", waitResult, "..."
    assert callbackResult == 49
    assert waitResult == 49


proc testAsyncSpawn() {.async.} =
    echo "---- Testing asyncSpawn ----"
    let f2: Future[int] = asyncSpawn proc():int =
        os.sleep 100
        echo "testAsyncSpawn: On thread ", getThreadID(), "!"
        assert getThreadID() != mainThreadID
        return 94
    assert f2.finished == false

    var callbackResult = -1
    f2.addCallback proc(f: Future[int]) =
        echo "testAsyncSpawn: Callback! got ", f.read()
        assert getThreadID() == mainThreadID
        callbackResult = f.read()

    echo "testAsyncSpawn: waiting on thread ", mainThreadID, "..."
    var waitResult = await f2
    echo "testAsyncSpawn: checking callbackResult ", callbackResult, ", waitResult ", waitResult, "..."
    assert callbackResult == 94
    assert waitResult == 94

test "ThreadSafe":
    waitfor testThreadSafe() and testAsyncSpawn()
