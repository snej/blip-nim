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

## CRC32 digest implementation,
## based on github.com/juancarlospaco/nim-crc32, which was "copied from RosettaCode".

import subseq
from strutils import toHex

type
  CRC32* = uint32

  CRC32Accumulator* = object
    ## CRC32 digest generator. Add bytes to it with `+=`, then get its `result`.
    state: CRC32

const CRC32Size* = 4


func createCrcTable(): array[0..255, uint32] {.inline.} =
  for i in 0..255:
    var rem = uint32(i)
    for j in 0..7:
      if (rem and 1) > 0'u32: rem = (rem shr 1) xor uint32(0xedb88320)
      else: rem = rem shr 1
    result[i] = rem

const crc32table = createCrcTable()


func reset*(crc: var CRC32Accumulator) {.inline.} =
  ## Rests an accumulator back to its initial state (FFFFFFFF).
  crc.state = not CRC32(0)

func `+=`*(crc: var CRC32Accumulator, b: byte) {.inline.} =
  ## Adds a byte to the accumulator.
  crc.state = (crc.state shr 8) xor crc32table[(crc.state and 0xff) xor uint32(b)]

func `+=`*(crc: var CRC32Accumulator, c: char) {.inline.} =
  ## Adds a character (byte) to the accumulator.
  crc += byte(ord(c))

func `+=`*(crc: var CRC32Accumulator, a: openarray[byte]) =
  ## Adds bytes to the accumulator.
  for b in a:
    crc += b

func `+=`*(crc: var CRC32Accumulator, a: subseq[byte]) =
  ## Adds bytes to the accumulator.
  for b in a:
    crc += b

func `+=`*(crc: var CRC32Accumulator, s: string) =
  ## Adds the bytes of the UTF-8 encoded string to the accumulator.
  for c in s:
    crc += c

func result*(crc: CRC32Accumulator): CRC32 {.inline.} =
  ## The CRC32 checksum of all bytes added to the accumulator so far.
  return not crc.state

proc `$`*(crc: CRC32Accumulator): string =
  ## Returns the current digest as an 8-character hex string.
  result = crc.result.int64.toHex(8)


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
  echo crc32("The quick brown fox jumps over the lazy dog.")
