package ai.fennel.engine

import io.ktor.application.*
import io.ktor.response.*
import io.ktor.routing.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*

fun main() {
    embeddedServer(Netty, port = 8080) {
        routing {
            get("/") {
                log.info("received request")
                call.respondText("Hello, world!")
            }
        }
    }.start(wait = true)
}
