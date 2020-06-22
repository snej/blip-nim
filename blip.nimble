# Package

version       = "0.1.0"
author        = "Jens Alfke"
description   = "BLIP protocol implementation"
license       = "Apache-2.0"
srcDir        = "src"
installExt    = @["nim"]
bin           = @["server", "client"]

# Dependencies

requires "nim >= 1.2.0"

requires "news >= 0.4.0"
requires "zip  >= 0.2.1"
