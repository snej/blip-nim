# log.nim

import strformat

type LogLevel* = enum
    Error,
    Warning,
    Info,
    Verbose,
    Debug

var CurrentLogLevel* = LogLevel.Warning

template log*(level: LogLevel, message: typed) =
    if level <= CurrentLogLevel:
        echo "BLIP ", $level, ": ", &message
