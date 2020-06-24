# subseq.nim


type subseq*[T] = object
    owner: seq[T]                   # Sequence that owns the data
    data: ptr UncheckedArray[T]     # Pointer to first item
    len: int                        # Number of items
    cap: int                        # Max capacity

template asUArray(T: type; item: typed): ptr UncheckedArray[T] =
    cast[ptr UncheckedArray[T]](unsafeaddr item)

# Creation:

proc newSubseq*[T](size: int): subseq[T] =
    ## Creates a new ``subseq`` with items initialized to default values.
    var owner = newSeq[T](size)
    shallow(owner)
    let data = asUArray(T, owner[0])
    return subseq[T](owner: owner, data: data, len: size, cap: size)

proc newSubseqOfCap*[T](cap: int): subseq[T] =
    ## Creates a new empty ``subseq`` with the given capacity.
    var owner = newSeq[T](cap)
    shallow(owner)
    let data = asUArray(T, owner[0])
    return subseq[T](owner: owner, data: data, len: 0, cap: cap)

proc newSubseqUninitialized*[T](size: int): subseq[T] =
    ## Creates a new ``subseq`` with uninitialized contents.
    let owner = newSeqUninitialized[T](size)
    shallow(owner)
    let data = newUncheckedArray[T](addr owner[0])
    return subseq[T](owner: owner, data: data, len: size, cap: size)

proc toSubseq*[T](owner: seq[T]): subseq[T] =
    ## Creates a ``subseq`` copied from an existing ``seq``
    result.owner = owner
    result.data = asUArray(T, result.owner[0])
    result.len = result.owner.len
    result.cap = result.len

# Conversion:

template toOpenArray*[T](s: subseq[T]): openarray[T] =
    ## Returns an ``openarray`` on a ``subseq``, to pass as a parameter.
    # (Note: has to be a template because openarray is magic and can't be returned normally.)
    s.data.toOpenArray(0, s.len - 1)

template toOpenArray*[T](s: subseq[T]; first, last: int): openarray[T] =
    ## Returns an ``openarray`` on a range of a ``subseq``, to pass as a parameter.
    # (Note: has to be a template because openarray is magic and can't be returned normally.)
    rangeCheck first >= 0 and last < s.len
    s.data.toOpenArray(first, last)

proc toSeq*[T](s: subseq[T]): seq[T] =
    ## Returns a new ``seq`` initialized with the contents of the ``subseq``.
    result = newSeqOfCap[T](s.len)
    for item in s:
        result.add(item)

proc with*[T](s: subseq[T], p: proc(a: openarray[T])) =
    ## Access the contents of the subseq as an openarray.
    ## (This is a workaround since Nim won't let me return an `openarray`.)
    p(s.data.toOpenArray(0, s.len - 1))

# Accessors:

proc len*[T](s: subseq[T]): int  = s.len
proc high*[T](s: subseq[T]): int = s.len - 1
proc low*[T](s: subseq[T]): int  = 0
proc cap*[T](s: subseq[T]): int  = s.cap
proc spare*[T](s: subseq[T]): int  = s.cap - s.len

proc `[]`*[T](s: subseq[T], index: int): lent T =
    rangeCheck index in 0 ..< s.len
    return s.data[index]

proc `[]`*[T](s: subseq[T], index: BackwardsIndex): lent T =
    rangeCheck int(index) in 0 ..< s.len
    return s.data[s.len - int(index)]

proc `[]`*[T](s: var subseq[T], index: int): var T =
    rangeCheck index in 0 ..< s.len
    return s.data[index]

proc `[]`*[T](s: var subseq[T], index: BackwardsIndex): var T =
    rangeCheck int(index) in 0 ..< s.len
    return s.data[s.len - int(index)]

proc `[]=`*[T](s: var subseq[T], index: int, value: sink T) =
    rangeCheck index in 0 ..< s.len
    s.data[index] = move(value)

proc `[]=`*[T](s: var subseq[T], index: BackwardsIndex, value: sink T) =
    rangeCheck int(index) in 0 ..< s.len
    s.data[s.len - index] = move(value)

iterator items*[T](s: subseq[T]): T =
    for i in 0 ..< s.len:
        yield s.data[i]

# Range accessors:

proc `[]`*[T](s: subseq[T], range: Slice[int]): subseq[T] =
    rangeCheck range.a >= 0 and range.b < s.len
    let data = asUArray(T, s.data[range.a])
    return subseq[T](owner: s.owner, data: data, len: range.len, cap: range.len)

proc `[]`*[T](s: subseq[T], range: HSlice[int, BackwardsIndex]): subseq[T] =
    return s[range.a .. (s.len - int(range.b))]

proc `[]`*[T](s: subseq[T], range: HSlice[BackwardsIndex, BackwardsIndex]): subseq[T] =
    return s[(s.len - int(range.a)) .. (s.len - int(range.b))]


proc `[]=`*[T](s: var subseq[T], range: Slice[int], values: openarray[T]) =
    rangeCheck range.a >= 0 and range.b < s.len
    rangeCheck range.len == values.len
    var i = range.a
    for item in values:
        s[i] = item
        i += 1

proc `[]=`*[T](s: var subseq[T], range: Slice[int], values: subseq[T]) =
    rangeCheck range.a >= 0 and range.b < s.len
    rangeCheck range.len == values.len
    var i = range.a
    for item in values:
        s[i] = item
        i += 1

proc `[]=`*[T](s: var subseq[T], range: HSlice[int, BackwardsIndex], values: subseq[T]) =
    s[range.a .. (s.len - int(range.b))] = values.toOpenArray


# Updating:

proc moveStart*[T](s: var subseq[T], delta: int) =
    ## Moves the start forwards, leaving the end in place (so the `len` decreases.)
    rangeCheck delta in 0 .. s.len
    s.len -= delta
    s.cap -= delta
    s.data = asUArray(T, s.data[delta])

proc resize*[T](s: var subseq[T], size: int) =
    rangeCheck size in 0 .. s.cap
    s.len = size

proc grow*[T](s: var subseq[T], by: int) =
    rangeCheck by in 0 .. s.spare
    s.len += by

proc clear*[T](s: var subseq[T]) =
    s.len = 0

# Adding:

proc add*[T](s: var subseq[T], value: sink T) =
    let pos = s.len
    s.resize(pos + 1)
    s[pos] = value

proc add*[T](s: var subseq[T], values: openarray[T]) =
    let pos = s.len
    s.resize(pos + values.len)
    s[pos ..< s.len] = values
