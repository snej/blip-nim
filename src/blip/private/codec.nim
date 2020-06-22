# codec.nim
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

## Compression/decompression via the Deflate algorithm.
## (This code was ported from Codec.{cc,hh} in LiteCore.)

import crc32, log
import endians, strformat, zip/zlib


type
    Codec* = ref object of RootObj
        ## Abstract base class
        checksum: CRC32Accumulator

    Mode* = enum
        ## Zlib write mode; see https://zlib.net/manual.html#Basic for info
        Raw = -1,               # not a zlib mode; means copy bytes w/o compression
        NoFlush = 0,
        PartialFlush,
        SyncFlush,
        FullFlush,
        Finish,
        Block,
        Trees

    CodecException* = object of CatchableError

const DefaultMode = Mode.SyncFlush

proc initCodec(c: Codec) =
    c.checksum.reset()

method write*(c: Codec;
              input: openarray[byte]; inRange: var Slice[int];
              output: var openarray[byte]; outRange: var Slice[int];
              mode: Mode = DefaultMode) {.base.} =
    ## Processes bytes through the codec; could be deflate, inflate or passthrough.
    ## As many bytes as possible are read starting from ``input[inputPos]``;
    ## then ``inputPos`` is updated to the index of the first unread byte.
    ## Output is appended to ``output``.
    discard

method unflushedBytes*(c: Codec): int {.base.} =
    ## Number of bytes buffered in the codec that haven't been written to the output yet for lack
    ## of space.
    return 0

proc writeChecksum*(c: Codec;
                    output: var openarray[byte]; outRange: var Slice[int]) =
    ## Writes the codec's current checksum to the output slice.
    ## This is a big-endian CRC32 checksum of all the unencoded data processed so far.
    let checksum = c.checksum.result
    bigEndian32(addr output[outRange.a], unsafeAddr checksum)
    outRange.a += CRC32Size

proc readAndVerifyChecksum*(c: Codec;
                            input: openarray[byte]; inRange: var Slice[int]) =
    ## Reads a checksum from the input slice and compares it with the codec's current one.
    ## If they aren't equal, throws an exception.
    if inRange.len < CRC32Size:
        raise newException(CodecException, &"Missing checksum")
    var inputChecksum: CRC32
    bigEndian32(unsafeAddr inputChecksum, unsafeAddr input[inRange.a])
    inRange.a += CRC32Size
    if inputChecksum != c.checksum.result:
        raise newException(CodecException, &"Invalid checksum {inputChecksum:x}: should be {c.checksum.result:x}")

proc copyBytes(dst: var openarray[byte]; dstPos: int;
               src: openarray[byte]; srcPos: int;
               n: int) =
    ## Range-checked copy between byte arrays
    rangeCheck(dstPos >= 0 and dstPos + n <= len(dst) and n >= 0)
    rangeCheck(srcPos >= 0 and srcPos + n <= len(src))
    copyMem(unsafeAddr dst[dstPos], unsafeAddr src[srcPos], n)

proc writeRaw(c: Codec;
              input: openarray[byte]; inRange: var Slice[int];
              output: var openarray[byte]; outRange: var Slice[int]) =
    ## Uncompressed write
    log Debug, "Copying {inRange.len} bytes into {outRange.len}-byte buf (no compression)"
    let n = min(inRange.len, outRange.len)
    c.checksum += input[inRange.a ..< inRange.a + n]
    copyBytes(output, outRange.a, input, inRange.a, n)
    inRange.a += n
    outRange.a += n


# Zlib Codec:

type
    ZlibCodec* = ref object of Codec
        z: ZStream
        flateProc: proc(strm: var ZStream, flush: int32): int32 {.cdecl.}

    DeflaterObj = object of ZlibCodec
    Deflater* = ref DeflaterObj

    InflaterObj = object of ZlibCodec
    Inflater* = ref InflaterObj

    CompressionLevel* = enum
        DefaultCompression  = -1,
        NoCompression       =  0,
        FastestCompression  =  1,
        BestCompression     =  9,


# Zlib tuning parameters:
const ZlibWindowSize = 15       # log2 of window size; 15 is the max and the suggested default
const ZlibDeflateMemLevel = 9   # Amount of memory to use for compressor state; 9 = 256KB

proc check(c: ZlibCodec; ret: int) =
    if ret < 0 and ret != Z_BUF_ERROR:
        raise newException(CodecException, &"Zlib error {ret}: {c.z.msg}")

proc zwrite(c: ZlibCodec;
            operation: cstring,
            input: openarray[byte];
            inRange: var Slice[int];
            output: var openarray[byte];
            outRange: var Slice[int];
            mode: Mode;
            maxInput: int) =
    ## Low-level wrapper around `deflate` / `inflate`. Mostly just translates between the Nim
    ## openarray-and-Slice representation and the C void*-and-int representation.
    assert mode > Mode.Raw
    let inSize = min(inRange.len, maxInput)
    c.z.availIn = Uint(inSize)
    c.z.nextIn = cast[Pbytef](unsafeAddr input[inRange.a])
    let outSize = outRange.len
    assert outSize > 0
    c.z.availOut = Uint(outSize)
    c.z.nextOut = cast[Pbytef](unsafeAddr output[outRange.a])
    let err = c.flateProc(c.z, int32(mode))
    log Debug, "    {operation}(in[{inRange.a}..{inRange.a+inSize-1}, out[{outRange.a}..{outRange.b}], mode {mode})-> {err}"
    c.check(err)
    inRange.a  = cast[int](c.z.nextIn)  - cast[int](unsafeAddr input[0])
    outRange.a = cast[int](c.z.nextOut) - cast[int](unsafeAddr output[0])
    log Debug, "        now inRange starts {inRange.a}, outRange starts {outRange.a}"


# Deflater:

proc `=destroy`*(c: var DeflaterObj) =
    discard deflateEnd(c.z)

proc newDeflater*(level: CompressionLevel = DefaultCompression): Deflater =
    result = Deflater()
    initCodec(result)
    result.flateProc = zlib.deflate
    result.check(deflateInit2u(result.z, level.int32, Z_DEFLATED.int32,
                 -ZlibWindowSize.int32, ZlibDeflateMemLevel.int32, Z_DEFAULT_STRATEGY.int32,
                 "1.2.11", sizeof(ZStream).cint))

proc writeAndFlush(c: Deflater;
              input: openarray[byte]; inRange: var Slice[int];
              output: var openarray[byte]; outRange: var Slice[int]) =
    const HeadroomForFlush = 12
    const StopAtOutputSize = 100

    var curMode = PartialFlush
    while inRange.len > 0:
        if Ulong(outRange.len) >= deflateBound(c.z, Ulong(inRange.len)):
            # Entire input is guaranteed to fit, so write it & flush:
            curMode = SyncFlush
            c.zwrite("deflate", input, inRange, output, outRange, SyncFlush, inRange.len)
        else:
            # Limit input size to what we know can be compressed into output.
            # Don't flush, because we may try to write again if there's still room.
            c.zwrite("deflate", input, inRange, output, outRange, curMode,
                     outRange.len - HeadroomForFlush)
        if outRange.len <= StopAtOutputSize:
            break;
    if curMode != SyncFlush:
        # Flush if we haven't yet (consuming no input)
        c.zwrite("deflate", input, inRange, output, outRange, SyncFlush, 0)

method write*(c: Deflater;
              input: openarray[byte]; inRange: var Slice[int];
              output: var openarray[byte]; outRange: var Slice[int];
              mode: Mode) =
    if mode == Mode.Raw:
        c.writeRaw(input, inRange, output, outRange)
    else:
        let origInRange = inRange
        let origOutRange = outRange
        log Debug, "Compressing {inRange.len} bytes into {outRange.len}-byte buf"
        case mode
            of NoFlush:   c.zwrite("deflate", input, inRange, output, outRange, mode, inRange.len)
            of SyncFlush: c.writeAndFlush(input, inRange, output, outRange)
            else:         raise newException(CodecException, &"Invalid mode")
        c.checksum += input[origInRange.a ..< inRange.a]
        log Debug, "    compressed {origInRange.len - inRange.len} bytes to {origOutRange.len - outRange.len}, {c.unflushedBytes} unflushed"

method unflushedBytes*(c: Deflater): int =
    var bytes: cuint
    var bits: cint
    c.check(deflatePending(c.z, bytes, bits))
    if bits > 0:
        bytes += 1
    return bytes.int


# Inflater:

proc `=destroy`*(c: var InflaterObj) =
    discard inflateEnd(c.z)

proc newInflater*(): Inflater =
    result = Inflater()
    initCodec(result)
    result.flateProc = zlib.inflate
    result.check(inflateInit2(result.z, -ZlibWindowSize))

method write*(c: Inflater;
              input: openarray[byte]; inRange: var Slice[int];
              output: var openarray[byte]; outRange: var Slice[int];
              mode: Mode) =
    if mode == Mode.Raw:
        c.writeRaw(input, inRange, output, outRange)
    else:
        log Debug, "Decompressing {inRange.len} bytes into {outRange.len}-byte buf"
        let outStart = outRange.a
        c.zwrite("inflate", input, inRange, output, outRange, mode, inRange.len)
        c.checksum += output[outStart ..< outRange.a]
        log Debug, "    decompressed to {outRange.a - outStart} bytes"
