import blip, blip/[message, transport]
import asyncdispatch

when isMainModule:
    const Verbose = true

    if Verbose:
        setBLIPLogLevel(3)

    echo("Connecting to server on port 9001...")

    proc handleResponse(msg: MessageIn) =
        if Verbose:
            echo "Got a response:"
            for (k, v) in msg.properties:
                echo "    ", k, " = ", v
            if msg.body.len < 500:
                echo "    \"", msg.body, "\""
            else:
                echo "    (", msg.body.len, "-byte body)"

    proc Run(blip: Blip) {.async.} =
        var n = 0
        while true:
            if Verbose or (n mod 1000 == 0):
                echo "Sending message ", n
            var msg = blip.newRequest("Test")
            msg["Testing"] = "123"
            msg.body = "This is test message #" & $n & ". "
            for i in countUp(1, 15):
                msg.body = msg.body & msg.body
            n += 1
            let f = msg.send()
            f.addCallback(proc (response: Future[MessageIn]) =
                if Verbose:
                    let body = response.read.body
                    if body.len < 500:
                        echo "Got response with body '", response.read.body, "'"
                    else:
                        echo "Got response with ", msg.body.len, "-byte body"
            )
            #await sleepAsync(0)
            break

    proc DoIt() {.async.} =
        var ws = await newWebSocketTransport("ws://127.0.0.1:9001/_blipsync")
        echo "Creating new Blip"
        var blip = newBlip(ws)
        await blip.run() and Run(blip)
        echo "...Closed Blip"

    waitfor DoIt()
