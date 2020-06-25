# crc32.nim
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

## CRC32 digest API, using the implementation in zlib.

import zip/zlib

type
  CRC32* = uint32

  CRC32Accumulator* = object
    ## CRC32 digest generator. Add bytes to it with `+=`, then get its `result`.
    state: Ulong

const CRC32Size* = 4

assert CRC32Size == sizeof(CRC32)


func reset*(crc: var CRC32Accumulator) {.inline.} =
  ## Rests an accumulator back to its initial state.
  crc.state = 0   # = crc32(0, nil, 0)

func `+=`*(crc: var CRC32Accumulator, b: byte) {.inline.} =
  ## Adds a byte to the accumulator.
  crc.state = crc32(crc.state, cast[Pbytef](unsafeaddr b), 1.Uint)

func `+=`*(crc: var CRC32Accumulator, c: char) {.inline.} =
  ## Adds a character (byte) to the accumulator.
  crc += byte(ord(c))

func `+=`*(crc: var CRC32Accumulator, a: openarray[byte]) =
  ## Adds bytes to the accumulator.
  if a.len > 0:
    crc.state = crc32(crc.state, cast[Pbytef](unsafeaddr a[0]), a.len.Uint)

func `+=`*(crc: var CRC32Accumulator, s: string) =
  ## Adds the bytes of the UTF-8 encoded string to the accumulator.
  if s.len > 0:
    crc.state = crc32(crc.state, cast[Pbytef](unsafeaddr s[0]), s.len.Uint)


func result*(crc: CRC32Accumulator): CRC32 {.inline.} =
  ## The CRC32 checksum of all bytes added to the accumulator so far.
  return CRC32(crc.state)


# Convenience functions:

func crc32*(s: string): CRC32 =
  ## Returns the CRC32 checksum of the bytes of a string.
  var crc: CRC32Accumulator
  crc += s
  return crc.result

func crc32*(a: openarray[byte]): CRC32 =
  ## Returns the CRC32 checksum of the bytes.
  var crc: CRC32Accumulator
  crc += a
  return crc.result

proc crc32FromFile*(filename: string): CRC32 =
  ## Returns the CRC32 checksum of the contents of a file.
  const bufSize = 8192
  var bin: File
  var crc: CRC32Accumulator

  if not open(bin, filename):
    return

  var buf {.noinit.}: array[bufSize, char]

  while true:
    var readBytes = bin.readChars(buf, 0, bufSize)
    for i in countup(0, readBytes - 1):
      crc += buf[i]
    if readBytes != bufSize:
      break

  close(bin)
  return crc.result


when is_main_module:
  import unittest

  check crc32("") == 0

  check crc32("The quick brown fox jumps over the lazy dog.") == 0x519025E9'u32

  var a: CRC32Accumulator
  a += "The quick brown fox"
  check a.result == 0xB74574DE'u32
  a += " jumps over the lazy dog."
  check a.result == 0x519025E9'u32


