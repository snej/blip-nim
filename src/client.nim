import blip, blip/message, blip/transport
import asyncdispatch

when isMainModule:
    echo("Connecting to server on port 9001...")

    proc handleResponse(msg: MessageIn) =
        echo "Got a response:"
        for (k, v) in msg.properties:
            echo "    ", k, " = ", v
        echo "    \"", msg.body, "\""


    proc DoIt() {.async.} =
        var ws = await newWebSocketTransport("ws://127.0.0.1:9001/_blipsync")
        echo "Creating new Blip"
        var blip = newBlip(ws)
        let f = blip.run()

        var msg = blip.newRequest("Test")
        msg["Testing"] = "123"
        msg.body = "This is a test message"
        let response = msg.send()
        response.addCallback(proc () =
            echo "Got response with body '", response.read.body, "'"
            discard blip.close()
        )

        await f
        echo "...Closed Blip"
    waitfor DoIt()
