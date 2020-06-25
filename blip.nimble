# Package

version       = "0.1.0"
author        = "Jens Alfke"
description   = "BLIP protocol implementation"
license       = "Apache-2.0"
srcDir        = "src"
binDir        = "bin"
installExt    = @["nim"]
bin           = @["../examples/server", "../examples/client"]

# Dependencies

requires "nim >= 1.2.0"

requires "news >= 0.5.0"
requires "zip  >= 0.2.1"
