import blip, blip/message, blip/transport
import asyncdispatch, asynchttpserver

when isMainModule:
    echo("Starting Blip server on port 9001...")

    proc handleTestMessage(msg: MessageIn) =
        echo "Received a message:"
        for (k, v) in msg.properties:
            echo "    ", k, " = ", v
        echo "    \"", msg.body, "\""

        if not msg.noReply:
            echo "Sending response..."
            var response = msg.createResponse
            response.body = "Hello, client!"
            response["OK"] = "true"
            response.sendNoReply()


    var server = newAsyncHttpServer()
    proc cb(req: Request) {.async, gcsafe.} =
        if req.url.path == "/_blipsync":
            var t = await newWebSocketTransport(req)
            echo "Creating new Blip"
            var blip = newBlip(t)
            blip.setDefaultHandler(proc(msg: MessageIn) = handleTestMessage(msg))
            await blip.run()
            echo "...Closed Blip\c\l"
        else:
            await req.respond(Http404, "Nope", newHttpHeaders())

    waitFor server.serve(Port(9001), cb)
