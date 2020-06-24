# testSubseq.nim

import blip/private/subseq
import unittest

test "Subseq":
    var s = newSubseq[byte](100)
    check s.len == 100
    check s.cap == 100
    check s.spare == 0
    for i in 0..99:
        s[i] = i.byte
    check s[0] == 0
    check s[49] == 49
    check s[99] == 99
    check s[^1] == 99
    #check s[^ -1] == s[^ -1]
    #check s[100] == s[100]

    var s2 = s[10..19]
    check s2.len == 10
    check s2.cap == 10
    check s2[0] == 10
    check s2[9] == 19

    s2[5] = 5
    check s2[5] == 5
    check s[15] == 5

    s2.moveStart(2)
    check s2.len == 8
    check s2.cap == 8
    check s2[0] == 12

    var s3 = s[5..0]
    check s3.len == 0
    check s3.cap == 0

    var s4 = s[80 .. ^1]
    check s4.len == 20
    check s4.cap == 20

    s[10..19] = s[20..29]
    check s[10] == 20
    check s[19] == 29

    check s[0..5].toSeq == @[0'u8, 1, 2, 3, 4, 5]

test "toSubSeq":
    var q = @["hi", "there"]
    let qsub = toSubseq(q)
    check qsub[0] == "hi"
    check qsub[1] == "there"
    q[1] = "bye"
    check qsub[1] == "there"

test "toOpenArray":
    var s = newSubseq[byte](100)
    for i in 0..99:
        s[i] = i.byte
    var ss = newSeq[byte]()
    ss.add(s.toOpenArray)
    for i in 0..99:
        check ss[i] == i.byte
    ss.reset()
    ss.add(s.toOpenArray(5, 4))
    check ss.len == 0
    ss.add(s.toOpenArray(5, 9))
    check ss == @[5'u8, 6, 7, 8, 9]

test "Grow":
    var s = newSubseqOfCap[int](100)
    check s.len == 0
    check s.cap == 100
    check s.spare == 100

    s.add(1)
    check s.len == 1
    check s.cap == 100
    check s.spare == 99
    check s[0] == 1

    s.add([2, 3, 4, 5])
    check s.len == 5
    check s.cap == 100
    check s.spare == 95
    check s[1] == 2
    check s[4] == 5

    s.resize(50)
    check s.len == 50
    check s.cap == 100
    check s.spare == 50
    check s[5] == 0
    check s[49] == 0

    s.moveStart(4)
    check s.len == 46
    check s.cap == 96
    check s.spare == 50
    check s[0] == 5
