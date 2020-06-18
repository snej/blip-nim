# testVarints.nim

import unittest

import blip/private/varint
import strutils

test "Varint sizes":
    check sizeOfVarint(0x0000) == 1
    check sizeOfVarint(0x007F) == 1
    check sizeOfVarint(0x0080) == 2
    check sizeOfVarint(0x0081) == 2
    check sizeOfVarint(0x3FFF) == 2
    check sizeOfVarint(0x4000) == 3
    check sizeOfVarint(0x4001) == 3

proc hexVarint(n: uint64): string =
    var buf: array[0..10, byte]
    let len = putVarint(buf, n)
    return cast[string](buf[0..len-1]).toHex

test "PutVarint":
    check hexVarint(0x0000) == "00"
    check hexVarint(0x007F) == "7F"
    check hexVarint(0x0080) == "8001"
    check hexVarint(0x0081) == "8101"
    check hexVarint(0x3FFF) == "FF7F"
    check hexVarint(0x4000) == "808001"
    check hexVarint(0x4001) == "818001"

proc getAll(buf: openarray[byte]): uint64 =
    var n = 0
    result = getVarint(buf, n)
    check n == buf.len

test "GetVarint":
    check getAll([0x00'u8]) == 0x00
    check getAll([0x7F'u8]) == 0x7F
    check getAll([0x80'u8, 0x01]) == 0x80

proc roundtrip(n: uint64) =
    var buf: array[0..50, byte]
    let ln = putVarint(buf, n)
    var pos = 0
    check getVarint(buf, pos) == n
    check pos == ln

test "RoundTrip":
    for n in [0x00'u64, 0x7F, 0x0080, 0x0081, 0x3FFF, 0x4000, 0x4001,
              0x7FFFFFFFFFFFFFFF'u64, 0xFFFFFFFFFFFFFFFF'u64]:
        roundtrip(n)
