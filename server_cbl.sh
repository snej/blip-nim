#! /bin/bash

nim c -r examples/server.nim --echo --log --port 4994 --path /db/_blipsync --protocol CBMobile_2 -v
