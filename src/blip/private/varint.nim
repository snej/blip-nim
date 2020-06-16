# varint.nim

proc sizeOfVarInt*(n: uint64): int =
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

proc addVarint*(s: var seq[byte]; n: uint64) =
    ## Appends a varint to a byte sequence.
    var buf: array[0..10, byte]
    let len = putVarint(buf, n)
    s.add(buf[0..len-1])

proc getVarint*(buf: openarray[byte], pos: var int): uint64 =
    ## Parses a varint from a byte array, returning the decoded number.
    ## On input `pos` is the starting array index to read from; on return, it's just past the end.
    ## Raises a ValueError if the varint is invalid (truncated).
    var n: uint64 = 0
    var i = pos
    while i < buf.len:
        n = (n shl 7) or (buf[i] and 0x7F)
        if buf[i] < 0x80:
            pos = i + 1
            return n
        i += 1
    raise newException(ValueError, "Truncated varint")
