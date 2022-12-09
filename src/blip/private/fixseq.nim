# fixseq.nim

import strformat


type fixseq*[T] = object
    ## A lightweight fixed-capacity sequence.
    ## Multiple fixseqs can share the same data buffer, so creating a subrange is very lightweight.
    owner: seq[T]                   # Sequence that owns the data
    data:  ptr UncheckedArray[T]    # Pointer to first item
    len:   int                      # Current number of items
    cap:   int                      # Max capacity

#TODO: Optimize to allocate memory myself rather than using a `seq` (requires --gc:arc?)


func uArrayAt[T](item: var T): ptr UncheckedArray[T] {.inline.} =
    cast[ptr UncheckedArray[T]](unsafeaddr item)


# Creation:

proc make[T](owner: sink seq[T], len: int, cap: int): fixseq[T] =
    result = fixseq[T](owner: owner, len: len, cap: cap)
    shallow(result.owner)
    result.data = uArrayAt(result.owner[0])


proc newFixseq*[T](len: int): fixseq[T] =
    ## Creates a new ``fixseq`` with items initialized to default values.
    ## Its capacity will be equal to its current length.
    return make[T](newSeq[T](len), len, len)

proc newFixseqUninitialized*[T](len: int): fixseq[T] =
    ## Creates a new ``fixseq`` with uninitialized contents.
    ## Its capacity will be equal to its current length.
    return make[T](newSeqUninitialized[T](len), len, len)

proc newFixseqOfCap*[T](cap: int): fixseq[T] =
    ## Creates a new ``fixseq`` with the given capacity and zero length.
    return make[T](newSeq[T](cap), 0, cap)

proc toFixseq*[T](owner: seq[T]): fixseq[T] =
    ## Creates a ``fixseq`` copied from an existing ``seq``.
    ## Its capacity will be equal to its current length.
    return make[T](owner, owner.len, owner.len)

proc toFixseq*(str: string): fixseq[byte] =
    ## Creates a ``fixseq`` copied from a string.
    ## Its capacity will be equal to its current length.
    return make[byte](cast[seq[byte]](str), str.len, str.len)


# Accessors:

func len*[T](s: fixseq[T]): int  = s.len            ## The current length.
func high*[T](s: fixseq[T]): int = s.len - 1        ## The maximum index (``len - 1``)
func low*[T](s: fixseq[T]): int  = 0                ## The minimum index (``0``)
func cap*[T](s: fixseq[T]): int  = s.cap            ## The maximum the length can grow to
func spare*[T](s: fixseq[T]): int  = s.cap - s.len  ## How much it can grow (``cap - len``)


# Item Accessors:

when compileOption("rangechecks"):
  proc checkRange(i: int, range: Slice[int]) =
    if not (i in range):
        raise newException(RangeDefect, &"{i} is not in range {range} for fixseq")
else:
  proc checkRange(i: int, range: Slice[int]) {.inline.} = discard

func normalize[T](s: fixseq[T], i: int           ): int {.inline.} = i
func normalize[T](s: fixseq[T], i: BackwardsIndex): int {.inline.} = s.normalize(s.len - int(i))


func `[]`*[T](s: fixseq[T], index: int or BackwardsIndex): lent T =
    let index = s.normalize(index)
    checkRange index, 0 ..< s.len
    return s.data[index]

func `[]`*[T](s: var fixseq[T], index: int or BackwardsIndex): var T =
    let index = s.normalize(index)
    checkRange index, 0 ..< s.len
    return s.data[index]

proc `[]=`*[T](s: var fixseq[T], index: int or BackwardsIndex, value: sink T) =
    let index = s.normalize(index)
    checkRange index, 0 ..< s.len
    s.data[index] = move(value)


iterator items*[T](s: fixseq[T]): T =
    ## Iterator over the items.
    for i in 0 ..< s.len:
        yield s.data[i]


# Conversions:

template toOpenArray*[T](s: fixseq[T]): openarray[T] =
    ## Returns an ``openarray`` on a ``fixseq``, to pass as a parameter.
    # (Note: has to be a template because openarray is magic and can't be returned normally.)
    s.data.toOpenArray(0, s.len - 1)

template toOpenArray*[T](s: fixseq[T]; first, last: int): openarray[T] =
    ## Returns an ``openarray`` on a range of a ``fixseq``, to pass as a parameter.
    # (Note: has to be a template because openarray is magic and can't be returned normally.)
    checkRange first, 0 ..< s.len
    checkRange last,  0 ..< s.len
    s.data.toOpenArray(first, last)

proc toSeq*[T](s: fixseq[T]): seq[T] =
    ## Returns a new ``seq`` copied from the contents of the ``fixseq``.
    result = newSeqOfCap[T](s.len)
    for item in s:
        result.add(item)

proc toString*(s: fixseq[byte] | fixseq[char]): string =
    ## Returns a new string copied from a ``fixseq`` of bytes.
    result = newString(s.len)
    if s.len > 0:
        copyMem(addr result[0], unsafeAddr s.data[0], s.len)

# Subrange Accessors:

func normalize[T, S1, S2](s: fixseq[T], range: HSlice[S1, S2]): (int, int) {.inline.} =
    let rstart = s.normalize(range.a)
    let rlen = max(0, s.normalize(range.b) - rstart + 1)
    checkRange rstart, 0 .. (s.len - rlen)
    return (rstart, rlen)


func `[]`*[T, S1, S2](s: fixseq[T], range: HSlice[S1, S2]): fixseq[T] =
    ## Returns a new ``fixseq`` on a subrange of the buffer.
    ## The new object has capacity limited to its length, i.e. it cannot be grown.
    ## This ensures it cannot be used to access memory outside the range it was given.
    let (rstart, rlen) = s.normalize(range)
    let data = uArrayAt(s.data[rstart])
    return fixseq[T](owner: s.owner, data: data, len: rlen, cap: rlen)

proc `[]=`*[T, S1, S2](s: var fixseq[T], range: HSlice[S1, S2], values: openarray[T]) =
    ## Replaces a range of items, copying from an array.
    let (rstart, rlen) = s.normalize(range)
    checkRange rlen, values.len .. values.len
    var i = rstart
    for item in values:
        s.data[i] = item
        i += 1
    #FIXME: This won't work right when `values` overlaps with me!

proc `[]=`*[T, S1, S2](s: var fixseq[T], range: HSlice[S1, S2], values: fixseq[T]) =
    ## Replaces a range of items, copying from another fixseq.
    s[range] = values.toOpenArray


# Updating:

proc moveStart*[T](s: var fixseq[T], delta: int) =
    ## Moves the start forwards, leaving the end in place (so the `len` decreases.)
    ## This looks like deleting items from the start, except that it doesn't actually affect the
    ## buffer, so other ``fixseq``s won't see any change.
    ## The start cannot be moved backwards, so a negative ``delta`` causes a range exception.
    checkRange delta, 0 .. s.len
    s.len -= delta
    s.cap -= delta
    s.data = uArrayAt(s.data[delta])

proc setLen*[T](s: var fixseq[T], size: int) =
    ## Grows or shrinks the ``fixseq`` to the given length.
    ## Throws a range error if the length would exceed the capacity.
    checkRange size, 0 .. s.cap
    s.len = size

proc grow*[T](s: var fixseq[T], by: int) =
    ## Grows or shrinks the ``fixseq`` by a relative amount.
    ## Throws a range error if the length would exceed the capacity.
    checkRange by, 0 .. s.spare
    s.len += by

proc clear*[T](s: var fixseq[T]) =
    ## Resets the length to 0.
    s.len = 0


# Adding:

proc add*[T](s: var fixseq[T], value: sink T) =
    ## Appends a value.
    ## Throws a range error if the length is already equal to the capacity.
    let pos = s.len
    s.setLen(pos + 1)
    s.data[pos] = value

proc add*[T](s: var fixseq[T], values: openarray[T]) =
    ## Appends an array of values.
    ## Throws a range error if the length would exceed the capacity.
    let pos = s.len
    s.setLen(pos + values.len)
    s[pos ..< s.len] = values

proc add*(s: var fixseq[byte], str: string) =
    if str.len > 0:
        let pos = s.len
        s.setLen(pos + str.len)
        copyMem(unsafeaddr s.data[pos], unsafeaddr str[0], str.len)

# Reading (popping):

proc readFirst*[T](s: var fixseq[T]): T =
    result = s[0]
    s.moveStart(1)

proc read*[T](s: var fixseq[T], n: int): fixseq[T] =
    result = s[0 ..< n]
    s.moveStart(n)


# String-specific utilities:

proc add*(str: var string, s: fixseq[byte]) =
    if s.len > 0:
        let pos = str.len
        str.setLen(pos + s.len)
        copyMem(unsafeaddr str[pos], unsafeaddr s[0], s.len)
