# outbox.nim

import message
import asyncdispatch

type
    Outbox* = object
        queue: seq[MessageOut]
        waiting: Future[MessageOut]
        closed: bool

proc push*(ob: var Outbox, msg: MessageOut) =
    ## Adds (or returns) a MessageOut to the outbox for processing.
    assert not ob.closed
    let waiter = ob.waiting
    if waiter != nil:
        assert ob.queue.len == 0
        ob.waiting = nil
        waiter.complete(msg)
    else:
        ob.queue.add(msg)

proc pop*(ob: var Outbox): Future[MessageOut] =
    ## Removes and returns the first message. If empty, waits until a message is added.
    ## If closed, returns nil.
    var f = newFuture[MessageOut]("Outbox.pop")
    if ob.queue.len > 0:
        f.complete(ob.queue[0])
        ob.queue.del(0)
    elif ob.closed:
        f.complete(nil)
    else:
        assert(ob.waiting == nil)   # Multiple waiters not supported
        ob.waiting = f
    return f

proc close*(ob: var Outbox) =
    ## Marks the Outbox as closed. Nothing more can be added.
    ob.closed = true
    ob.queue = @[]
    if ob.waiting != nil:
        let f = ob.waiting
        ob.waiting = nil
        f.complete(nil)
