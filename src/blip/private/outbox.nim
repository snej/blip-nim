# outbox.nim
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

import protocol, log
import asyncdispatch, deques

type Outbox*[T] = object
    ## Queue of outgoing messages. The Blip connection cycles through the queue, repeatedly
    ## popping the first message, sending the next frame's worth of bytes, and pushing it
    ## back to the end of the queue.
    queue: Deque[T]
    waiting: Future[T]
    closed: bool
    maxDepth: int

func empty*[T](ob: Outbox[T]): bool =
    return ob.queue.len == 0

proc push*[T](ob: var Outbox[T], msg: T) =
    ## Adds (or returns) a T to the outbox for processing.
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
        ob.maxDepth = max(ob.maxDepth, ob.queue.len)
        #TODO: Implement special placement of urgent messages [BLIP 3.2]

proc pop*[T](ob: var Outbox[T]): Future[T] =
    ## Removes and returns the first message. If empty, waits until a message is added.
    ## If closed, returns nil.
    var f = newFuture[T]("Outbox.pop")
    if ob.queue.len > 0:
        f.complete(ob.queue.popFirst())
    elif ob.closed:
        f.complete(nil)
    else:
        assert(ob.waiting == nil)   # Multiple waiters not supported
        ob.waiting = f
    return f

func find*[T](ob: Outbox[T]; msgType: MessageType; msgNo: MessageNo): T =
    for msg in ob.queue:
        if msg.number == msgNo and msg.messageType == msgType:
            return msg
    return nil
    #OPT: Linear search; could be optimized by keeping a Table, if necessary

proc close*[T](ob: var Outbox[T]) =
    ## Marks the Outbox as closed. Nothing more can be added.
    ob.closed = true
    ob.queue.clear()
    if ob.waiting != nil:
        let f = ob.waiting
        ob.waiting = nil
        f.complete(nil)
    log Info, "Outbox max depth was {ob.maxDepth}"

func isClosed*[T](ob: Outbox[T]): bool =
    return ob.closed


######## ICEBOX


type Icebox*[T] = object
    ## A set of outgoing messages that are blocked waiting for acknowledgement from the peer.
    # OPT: Could be optimized by using a Table[(MessageType,MessageNo), T]
    messages: seq[T]

proc add*[T](ib: var Icebox[T], msg: T) =
    assert not (msg in ib.messages)
    ib.messages.add(msg)

func find*[T](ib: Icebox[T]; msgType: MessageType; msgNo: MessageNo): (int, T) =
    for i, msg in ib.messages:
        if msg.number == msgNo and msg.messageType == msgType:
            return (i, msg)
    return (-1, nil)

proc del*(ib: var Icebox, i: int) =
    ib.messages.del(i)

func empty*(ib: Icebox): bool =
    return ib.messages.len == 0
