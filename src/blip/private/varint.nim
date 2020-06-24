# varint.nim
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

## Variable-length integer implementation.
##
## Important: These are Google (protobuf, Go) style varints, *not* the SQLite-style varints
## that Nim's ``varint`` package implements.

import subseq

proc sizeOfVarint*(n: uint64): int =
    ## The number of bytes that a varint representation of ``n`` occupies.
    var len = 1
    var n = n
    while n >= 0x80:
        len += 1
        n = n shr 7
    return len

proc putVarint*(buf: var openArray[byte]; n: uint64): int =
    ## Stores a varint in the given byte array. Returns the number of bytes used.
    var i = 0
    var n = n
    while n >= 0x80:
        buf[i] = byte((n and 0x7F) or 0x80)
        i += 1
        n = n shr 7
    buf[i] = byte(n)
    return i + 1

type ByteSeq = seq[byte] | subseq[byte]

proc addVarint*(s: var ByteSeq; n: uint64) =
    ## Appends a varint to a byte sequence.
    var buf: array[0..10, byte]
    let len = putVarint(buf, n)
    s.add(buf.toOpenArray(0, len-1))

proc getVarint*(buf: openarray[byte], pos: var int): uint64 =
    ## Reads a varint from a byte array, returning the decoded number.
    ## On input `pos` is the starting array index to read from; on return, it's just past the end.
    ## Raises a ValueError if the varint is invalid (truncated).
    result = 0
    var i = pos
    var shift = 0
    let imax = min(buf.len, 10)
    while i < imax:
        result = result or (uint64(buf[i] and 0x7F) shl shift)
        if buf[i] < 0x80:
            pos = i + 1
            return
        i += 1
        shift += 7
    raise newException(ValueError, "Invalid varint")

proc getVarint*(buf: openarray[byte]): uint64 =
    ## Reads a varint from the start of the array. Ignores any bytes after the varint.
    var pos = 0
    return getVarint(buf, pos)
