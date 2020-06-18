import blip, blip/[message, transport]
import asyncdispatch, asynchttpserver

when isMainModule:
    const Verbose = true

    if Verbose:
        setBLIPLogLevel(3)

    echo("Starting Blip server on port 9001...")

    var nReceived = 0

    proc handleMessage(msg: MessageIn) =
        nReceived += 1
        if Verbose:
            echo "Received message ", nReceived
            for (k, v) in msg.properties:
                echo "    ", k, " = ", v
            if msg.body.len < 500:
                echo "    \"", msg.body, "\""
            else:
                echo "    (", msg.body.len, " byte body)"
        elif nReceived mod 1000 == 0:
            echo "Received ", nReceived, " messages"

        if not msg.noReply:
            if Verbose: echo "Sending response..."
            var response = msg.createResponse
            response.body = msg.body
            response["IsReply"] = "true"
            response.sendNoReply()


    var server = newAsyncHttpServer()
    proc cb(req: Request) {.async, gcsafe.} =
        if req.url.path == "/_blipsync":
            var t = await newWebSocketTransport(req)
            echo "Creating new Blip"
            var blip = newBlip(t)
            blip.setDefaultHandler(proc(msg: MessageIn) = handleMessage(msg))
            await blip.run()
            echo "...Closed Blip\c\l"
        else:
            await req.respond(Http404, "Nope", newHttpHeaders())

    waitFor server.serve(Port(9001), cb)
