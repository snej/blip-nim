# log.nim
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

## This is a super basic logging implementation.
## (I didn't use the standard-library one because it uses globals, which async is allergic to.)

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
