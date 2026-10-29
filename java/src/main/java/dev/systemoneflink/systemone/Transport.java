package dev.systemoneflink.systemone;

import java.util.concurrent.CompletableFuture;

/** One POST of a JSON body to the decision endpoint. Swappable for tests or other HTTP stacks. */
public interface Transport {
    record Response(int status, String body) {}

    /** Completes exceptionally on I/O failure or timeout; HTTP error statuses are normal responses. */
    CompletableFuture<Response> post(String jsonBody);
}
