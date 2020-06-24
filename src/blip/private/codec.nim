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

import crc32, log, subseq
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

const DefaultMode* = Mode.SyncFlush

proc initCodec(c: Codec) =
    c.checksum.reset()

method write*(c: Codec;
              input: var subseq[byte];
              output: var subseq[byte];
              mode: Mode = DefaultMode) {.base.} =
    ## Processes bytes through the codec; could be deflate, inflate or passthrough.
    ## As many bytes as possible are read transferred, then ``input``'s start is moved forward
    ## to the first unread byte, and ``output`` is shortened to fit the bytes written to it.
    discard

method unflushedBytes*(c: Codec): int {.base.} =
    ## Number of bytes buffered in the codec that haven't been written to the output yet for lack
    ## of space.
    return 0

proc writeChecksum*(c: Codec; output: var subseq[byte]) =
    ## Writes the codec's current checksum to the output slice.
    ## This is a big-endian CRC32 checksum of all the unencoded data processed so far.
    let pos = output.len
    output.grow(CRC32Size)
    let checksum = c.checksum.result
    bigEndian32(addr output[pos], unsafeAddr checksum)
    log Debug, "    wrote checksum {checksum:8x}"

proc readAndVerifyChecksum*(c: Codec; input: var subseq[byte]) =
    ## Reads a checksum from the input slice and compares it with the codec's current one.
    ## If they aren't equal, throws an exception.
    if input.len < CRC32Size:
        raise newException(CodecException, &"Missing checksum")
    var inputChecksum: CRC32
    bigEndian32(unsafeAddr inputChecksum, addr input[0])
    log Debug, "    read checksum {inputChecksum:8x}, expecting {c.checksum.result:8x}"
    if inputChecksum != c.checksum.result:
        raise newException(CodecException, &"Invalid checksum {inputChecksum:x}: should be {c.checksum.result:x}")
    input.moveStart(CRC32Size)

proc writeRaw(c: Codec; input: var subseq[byte]; output: var subseq[byte], maxBytes: int) =
    ## Uncompressed write
    let n = min(min(input.len, output.spare), maxBytes)
    log Debug, "    Copying {n} bytes from {input.len}-byte input to {output.spare}-byte output (no compression)"
    let pos = output.len
    output.grow(n)
    copyMem(unsafeAddr output[pos], unsafeAddr input[0], n)
    input.moveStart(n)


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

proc indexOfPtr(s: subseq[byte]; p: pointer): int =
    result = cast[int](p)  - cast[int](unsafeAddr s[0])
    rangeCheck result in 0 .. s.len

proc zwrite(c: ZlibCodec;
            operation: cstring,
            input: var subseq[byte];
            output: var subseq[byte];
            mode: Mode;
            maxInput: int) =
    ## Low-level wrapper around `deflate` / `inflate`. Mostly just translates between the Nim
    ## openarray-and-Slice representation and the C void*-and-int representation.
    assert mode > Mode.Raw
    let inSize = min(input.len, maxInput)
    c.z.availIn = Uint(inSize)
    c.z.nextIn = cast[Pbytef](unsafeAddr input[0])

    let outSpare = output.spare
    assert outSpare > 0
    c.z.availOut = Uint(outSpare)
    let outLen = output.len
    output.resize(output.cap)
    c.z.nextOut = cast[Pbytef](unsafeAddr output[outLen])
    let err = c.flateProc(c.z, int32(mode))
    if err != 0:
        log Debug, "    {operation}(in[0..{inSize-1}], out[0..{outSpare-1}], mode {mode})-> err {err}"
        output.resize(outLen)
        c.check(err)
    input.moveStart(input.indexOfPtr(c.z.nextIn))
    output.resize(output.indexOfPtr(c.z.nextOut))
    log Debug, "    {operation}(in[0..{inSize-1}], out[0..{outSpare-1}], mode {mode})-> {output.len - outLen} bytes"


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
              input: var subseq[byte];
              output: var subseq[byte]) =
    const HeadroomForFlush = 12
    const StopAtOutputSize = 100

    var curMode = PartialFlush
    while input.len > 0:
        if Ulong(output.spare) >= deflateBound(c.z, Ulong(input.len)):
            # Entire input is guaranteed to fit, so write it & flush:
            curMode = SyncFlush
            c.zwrite("deflate", input, output, SyncFlush, input.len)
        else:
            # Limit input size to what we know can be compressed into output.
            # Don't flush, because we may try to write again if there's still room.
            c.zwrite("deflate", input, output, curMode, output.spare - HeadroomForFlush)
        if output.spare <= StopAtOutputSize:
            break;
    if curMode != SyncFlush:
        # Flush if we haven't yet (consuming no input)
        c.zwrite("deflate", input, output, SyncFlush, 0)

method write*(c: Deflater;
              input: var subseq[byte];
              output: var subseq[byte];
              mode: Mode) =
    let origInput = input
    var origOutput = output
    log Debug, "Compressing {input.len} bytes into {output.spare}-byte buf"
    # Compress the input to the output:
    case mode
        of Raw:       c.writeRaw(input, output, output.spare - CRC32Size)
        of NoFlush:   c.zwrite("deflate", input, output, mode, input.len)
        of SyncFlush: c.writeAndFlush(input, output)
        else:         raise newException(CodecException, &"Invalid mode")

    # Compute and write the checksum:
    let inputConsumed = origInput.len - input.len
    c.checksum += origInput[0 ..< inputConsumed].toOpenArray
    if mode == Raw:
        # In raw mode the checksum is just appended:
        c.writeChecksum(output)

    let outputWritten = output.len - origOutput.len

    if mode != Raw:
        # When deflating, the last 4 bytes of the output are always 0000FFFF; as a space saving
        # measure, overwrite them with the checksum. (The Inflater will restore them.)
        let trailer = output[^CRC32Size .. ^1]
        assert trailer[0] == 0 and trailer[1] == 0 and trailer[2] == 0xFF and trailer[3] == 0xFF
        output.resize(output.len - CRC32Size)
        c.writeChecksum(output)
    log Debug, "    compressed {inputConsumed} bytes to {outputWritten} ({c.unflushedBytes} unflushed)"

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

let kTrailer = @[0x00'u8, 0x00, 0xFF, 0xFF].toSubseq

method write*(c: Inflater;
              input: var subseq[byte];
              output: var subseq[byte];
              mode: Mode) =
    log Debug, "Decompressing {input.len} bytes into {output.spare}-byte buf"
    var origOutput = output
    if mode == Mode.Raw:
        # Raw 'decompress'. Just copy input bytes, except for the trailing checksum:
        c.writeRaw(input, output, input.len - CRC32Size)
    else:
        # Inflate the input. The checksum was written over the expected 0000FFFF trailer,
        # so handle the last 4 bytes separately:
        if input.len > CRC32Size:
            c.zwrite("inflate", input, output, mode, input.len - CRC32Size)
        if input.len <= CRC32Size:
            var trailer = kTrailer
            c.zwrite("inflate", trailer, output, mode, trailer.len)
            assert trailer.len == 0
        log Debug, "    decompressed to {output.len - output.len} bytes"

    let bytesWritten = output.len - origOutput.len
    output.resize(bytesWritten)
    c.checksum += output[origOutput.len ..< output.len]
    if input.len <= CRC32Size:
        c.readAndVerifyChecksum(input)
