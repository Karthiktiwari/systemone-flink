package dev.systemoneflink.systemone;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/** {@link Transport} on the JDK HTTP client. */
public final class JdkTransport implements Transport {
    private final HttpClient http = HttpClient.newHttpClient();
    private final URI endpoint;
    private final String apiKey;
    private final Duration timeout;

    public JdkTransport(String endpoint, String apiKey, Duration timeout) {
        this.endpoint = URI.create(endpoint);
        this.apiKey = apiKey;
        this.timeout = timeout;
    }

    @Override
    public CompletableFuture<Response> post(String jsonBody) {
        HttpRequest.Builder rb = HttpRequest.newBuilder(endpoint)
                .timeout(timeout)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(jsonBody));
        if (apiKey != null) {
            rb.header("Authorization", "Bearer " + apiKey);
        }
        return http.sendAsync(rb.build(), HttpResponse.BodyHandlers.ofString())
                .thenApply(r -> new Response(r.statusCode(), r.body()));
    }
}
