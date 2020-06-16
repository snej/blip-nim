# crc32.nim
#
# adapted from github.com/juancarlospaco/nim-crc32, which was "copied from RosettaCode"

from strutils import toHex

type
  CRC32* = object
    ## CRC32 accumulator. Add bytes to it with `+=`, then get its `result`.
    state: uint32


func createCrcTable(): array[0..255, uint32] {.inline.} =
  for i in 0..255:
    var rem = uint32(i)
    for j in 0..7:
      if (rem and 1) > 0'u32: rem = (rem shr 1) xor uint32(0xedb88320)
      else: rem = rem shr 1
    result[i] = rem

const crc32table = createCrcTable()


func reset(crc: var CRC32) {.inline.} =
  crc.state = 0

func `+=`*(crc: var CRC32, b: byte) {.inline.} =
  ## Adds a byte to the accumulator.
  crc.state = (crc.state shr 8) xor crc32table[(crc.state and 0xff) xor uint32(b)]

func `+=`*(crc: var CRC32, c: char) {.inline.} =
  ## Adds a character (byte) to the accumulator.
  crc += byte(ord(c))

func `+=`*(crc: var CRC32, a: openarray[byte]) =
  ## Adds bytes to the accumulator.
  for b in a:
    crc += b

func `+=`*(crc: var CRC32, s: string) =
  ## Adds the bytes of the UTF-8 encoded string to the accumulator.
  for c in s:
    crc += c

func result*(crc: CRC32): uint32 {.inline.} =
  ## The CRC32 checksum of all bytes added to the accumulator so far.
  return not crc.state

proc `$`*(crc: CRC32): string =
  result = crc.result.int64.toHex(8)


# Convenience functions:

func crc32*(s: string): uint32 =
  ## Returns the CRC32 checksum of the bytes of a string.
  var crc: CRC32
  crc += s
  return crc.result

func crc32*(a: openarray[byte]): uint32 =
  ## Returns the CRC32 checksum of the bytes.
  var crc: CRC32
  crc += a
  return crc.result

proc crc32FromFile*(filename: string): uint32 =
  ## Returns the CRC32 checksum of the contents of a file.
  const bufSize = 8192
  var bin: File
  var crc: CRC32

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
