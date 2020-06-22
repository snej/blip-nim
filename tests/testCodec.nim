# testCodec.nim

import blip/private/[codec, log]
import unittest, strformat, strutils

const InputText = """It was a dark and stormy night; the rain fell in torrents, except at
                     occasional intervals, when it was checked by a violent gust of wind
                     which swept up the streets (for it is in London that our scene lies),
                     rattling along the house-tops, and fiercely agitating the scanty flame
                     of the lamps that struggled against the darkness. Through one of the
                     obscurest quarters of London, and among haunts little loved by the
                     gentlemen of the police, a man, evidently of the lowest orders, was
                     wending his solitary way. He stopped twice or thrice at different shops
                     and houses of a description correspondent with the appearance of the
                     quartier in which they were situated, and tended inquiry for some
                     article or another which did not seem easily to be met with. All the
                     answers he received were couched in the negative; and as he turned from
                     each door he muttered to himself, in no very elegant phraseology, his
                     disappointment and discontent. At length, at one house, the landlord, a
                     sturdy butcher, after rendering the same reply the inquirer had hitherto
                     received, added, “But if this vill do as vell, Dummie, it is quite at
                     your sarvice!” Pausing reflectively for a moment, Dummie responded that
                     he thought the thing proffered might do as well; and thrusting it into
                     his ample pocket, he strode away with as rapid a motion as the wind and
                     the rain would allow. He soon came to a nest of low and dingy buildings,
                     at the entrance to which, in half-effaced characters, was written
                     “Thames Court.” Halting at the most conspicuous of these buildings, an
                     inn or alehouse, through the half-closed windows of which blazed out in
                     ruddy comfort the beams of the hospitable hearth, he knocked hastily at
                     the door. He was admitted by a lady of a certain age, and endowed with a
                     comely rotundity of face and person."""

test "Deflate/Inflate":
    CurrentLogLevel = LogLevel.Debug

    var frames: seq[seq[byte]]
    var outputLen = 0

    var defl = newDeflater()
    let input = cast[seq[byte]](InputText)
    var inRange = 0 .. (input.len - 1)
    while inRange.len > 0:
        var output: array[0 .. 299, byte]
        var outRange = 0 .. 299
        defl.write(input, inRange, output, outRange)
        frames.add(output[0 ..< outRange.a])
        outputLen += outRange.a

    echo &"Compressed {InputText.len} bytes to {outputLen}:"
    for frame in frames:
        echo cast[string](frame).toHex

    # Now re-inflate:
    var infl = newInflater()
    var result: seq[byte]
    for frame in frames:
        var output: array[0 .. 999, byte]
        var outRange = 0 .. 999
        inRange = 0 .. (frame.len - 1)
        infl.write(frame, inRange, output, outRange)
        let chunk = output[0 ..< outRange.a]
        echo "-- ", cast[string](chunk)
        result &= chunk

    let resultStr = cast[string](result)
    echo "Decompressed = '", resultStr, "'"

    check resultStr == InputText
