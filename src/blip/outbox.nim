# outbox.nim

import message, protocol
import asyncdispatch, deques

type Outbox* = object
    ## Queue of outgoing messages. The Blip connection cycles through the queue, repeatedly
    ## popping the first message, sending the next frame's worth of bytes, and pushing it
    ## back to the end of the queue.
    queue: Deque[MessageOut]
    waiting: Future[MessageOut]
    closed: bool

proc empty*(ob: Outbox): bool =
    return ob.queue.len == 0

proc push*(ob: var Outbox, msg: MessageOut) =
    ## Adds (or returns) a MessageOut to the outbox for processing.
    assert not ob.closed
    assert not (msg in ob.queue)
    let waiter = ob.waiting
    if waiter != nil:
        # If someone's waiting on an empty queue, give the message to them:
        assert ob.queue.len == 0
        ob.waiting = nil
        waiter.complete(msg)
    else:
        # Else add message to queue:
        if msg.isAck:
            ob.queue.addFirst(msg)
        else:
            ob.queue.addLast(msg)
        #TODO: Implement special placement of urgent messages [BLIP 3.2]

proc pop*(ob: var Outbox): Future[MessageOut] =
    ## Removes and returns the first message. If empty, waits until a message is added.
    ## If closed, returns nil.
    var f = newFuture[MessageOut]("Outbox.pop")
    if ob.queue.len > 0:
        f.complete(ob.queue.popFirst())
    elif ob.closed:
        f.complete(nil)
    else:
        assert(ob.waiting == nil)   # Multiple waiters not supported
        ob.waiting = f
    return f

proc find*(ob: Outbox; msgType: MessageType; msgNo: MessageNo): MessageOut =
    for msg in ob.queue:
        if msg.number == msgNo and msg.messageType == msgType:
            return msg
    return nil
    #OPT: Linear search; could be optimized by keeping a Table, if necessary

proc close*(ob: var Outbox) =
    ## Marks the Outbox as closed. Nothing more can be added.
    ob.closed = true
    ob.queue.clear()
    if ob.waiting != nil:
        let f = ob.waiting
        ob.waiting = nil
        f.complete(nil)


type Icebox* = object
    ## A set of outgoing messages that are blocked waiting for acknowledgement from the peer.
    # OPT: Could be optimized by using a Table[(MessageType,MessageNo), MessageOut]
    messages: seq[MessageOut]

proc add*(ib: var Icebox, msg: MessageOut) =
    assert not (msg in ib.messages)
    ib.messages.add(msg)

proc find*(ib: Icebox; msgType: MessageType; msgNo: MessageNo): (int, MessageOut) =
    for i, msg in ib.messages:
        if msg.number == msgNo and msg.messageType == msgType:
            return (i, msg)
    return (-1, nil)

proc del*(ib: var Icebox, i: int) =
    ib.messages.del(i)
