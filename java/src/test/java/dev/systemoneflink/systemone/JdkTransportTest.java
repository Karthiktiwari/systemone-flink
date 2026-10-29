package dev.systemoneflink.systemone;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JdkTransportTest {
    HttpServer server;
    volatile String auth;
    volatile String contentType;
    volatile String received;
    volatile int status = 200;
    volatile long delayMs = 0;

    @BeforeEach
    void start() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/systemone", ex -> {
            auth = ex.getRequestHeaders().getFirst("Authorization");
            contentType = ex.getRequestHeaders().getFirst("Content-Type");
            received = new String(ex.getRequestBody().readAllBytes());
            try { Thread.sleep(delayMs); } catch (InterruptedException ignored) { }
            byte[] body = "{\"ok\":true}".getBytes();
            ex.sendResponseHeaders(status, body.length);
            ex.getResponseBody().write(body);
            ex.close();
        });
        server.start();
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    String url() {
        return "http://127.0.0.1:" + server.getAddress().getPort() + "/v1/systemone";
    }

    @Test
    void postsJsonWithBearerAuth() throws Exception {
        var r = new JdkTransport(url(), "secret", Duration.ofSeconds(2)).post("{\"a\":1}").get();
        assertEquals(200, r.status());
        assertEquals("Bearer secret", auth);
        assertEquals("application/json", contentType);
        assertEquals("{\"a\":1}", received);
    }

    @Test
    void omitsAuthWithoutKeyAndSurfacesErrorStatusesAsResponses() throws Exception {
        status = 429;
        var r = new JdkTransport(url(), null, Duration.ofSeconds(2)).post("{}").get();
        assertEquals(429, r.status());
        assertEquals(null, auth);
    }

    @Test
    void timeoutCompletesExceptionally() {
        delayMs = 1500;
        var f = new JdkTransport(url(), null, Duration.ofMillis(200)).post("{}");
        var e = org.junit.jupiter.api.Assertions.assertThrows(ExecutionException.class, f::get);
        assertTrue(e.getCause() instanceof java.net.http.HttpTimeoutException, String.valueOf(e.getCause()));
    }
}
