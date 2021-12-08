package ai.fennel.engine

import io.ktor.application.*
import io.ktor.features.*
import io.ktor.request.*
import io.ktor.response.*
import io.ktor.routing.*
import io.ktor.serialization.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import starql.ParseException
import starql.parser.Parser

@ExperimentalSerializationApi
val format = Json { explicitNulls = false }

@Serializable
data class EvalRequest(val query: String)

@Serializable
data class EvalResponse(var result: String?, var error: String?) {
    constructor() : this(null, null)
}

@ExperimentalSerializationApi
fun main() {
    // TODO(abhay): Use Netty EngineMain in the long run to separate configuration from code.
    embeddedServer(Netty, port = 8080) {
        install(ContentNegotiation) {
            json()
        }
        routing {
            get("/") {
                log.info("received request")
                call.respondText("Hello, world!")
            }
            post("/runQuery") {
                val request = call.receive<EvalRequest>()
                val response = EvalResponse()
                try {
                    val query = request.query
                    log.info("Query: $query")
                    val parser = Parser(query)
                    val ast = parser.parse()
                    response.result = ast.eval().toString()
                } catch (ex: ParseException) {
                    response.error = ex.toString()
                }
                call.respond(format.encodeToString(response))
            }
        }
    }.start(wait = true)
}
