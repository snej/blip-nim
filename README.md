# BLIP In Nim

This is an implementation of the [BLIP][BLIP] messaging protocol, in the [Nim][NIM] language &hearts;.  
(BLIP is an extension of WebSockets that supportst multiplexed request-response messaging.)

At the moment (June 2020) it's quite new and incomplete, but coming along quickly.

## To-Do:

- [ ] Compressed frames _(first I need to write a zlib wrapper...)_
- [ ] Prioritized queuing of urgent messages _(not sure this is important, though)_
- [ ] Better error handling
- [ ] Better WebSocket "subprotocol" parsing


[BLIP]: https://github.com/couchbaselabs/BLIP-Cpp/blob/master/README.md
[NIM]: https://nim-lang.org
