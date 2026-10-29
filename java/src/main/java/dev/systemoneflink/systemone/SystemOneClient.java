package dev.systemoneflink.systemone;

import dev.systemoneflink.engine.DecisionEngine;
import dev.systemoneflink.engine.DecisionException;
import dev.systemoneflink.model.Decision;
import dev.systemoneflink.model.DecisionSpec;

import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/** {@link DecisionEngine} over the System One protocol: codec + transport, retried by Failsafe. */
public final class SystemOneClient implements DecisionEngine {
    private static final Set<Integer> RETRYABLE_STATUS = Set.of(429, 500, 502, 503, 504, 529);

    private final SystemOneCodec codec;
    private final Transport transport;
    private final RetryPolicy<Transport.Response> retry;

    public SystemOneClient(DecisionSpec spec, Transport transport, RetryPolicy<Transport.Response> retry) {
        this.codec = new SystemOneCodec(spec);
        this.transport = transport;
        this.retry = retry;
    }

    /** Retries I/O errors and 429/5xx/529 with exponential backoff; other statuses fail fast. */
    public static RetryPolicy<Transport.Response> retries(int maxRetries, Duration baseDelay) {
        return RetryPolicy.<Transport.Response>builder()
                .handleIf((resp, err) -> err != null || RETRYABLE_STATUS.contains(resp.status()))
                .withBackoff(baseDelay, baseDelay.multipliedBy(16))
                .withMaxRetries(maxRetries)
                .build();
    }

    /** Convenience: JDK HTTP transport with a per-attempt timeout and 200 ms base backoff. */
    public static SystemOneClient create(
            DecisionSpec spec, String endpoint, String apiKey, Duration timeout, int maxRetries) {
        return new SystemOneClient(
                spec, new JdkTransport(endpoint, apiKey, timeout), retries(maxRetries, Duration.ofMillis(200)));
    }

    @Override
    public CompletableFuture<Map<String, Decision>> decide(Object state) {
        String body;
        try {
            body = codec.encode(state);
        } catch (DecisionException e) {
            return CompletableFuture.failedFuture(e);
        }
        return Failsafe.with(retry)
                .getStageAsync(() -> transport.post(body))
                .handle((resp, err) -> {
                    if (err == null && resp.status() == 200) {
                        return decodeSafely(resp.body());
                    }
                    boolean transientFailure = err != null || RETRYABLE_STATUS.contains(resp.status());
                    String why = err != null ? rootMessage(err) : "HTTP " + resp.status();
                    return CompletableFuture.<Map<String, Decision>>failedFuture(
                            new DecisionException(why, transientFailure, err));
                })
                .thenCompose(f -> f);
    }

    private CompletableFuture<Map<String, Decision>> decodeSafely(String body) {
        try {
            return CompletableFuture.completedFuture(codec.decode(body));
        } catch (DecisionException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private static String rootMessage(Throwable t) {
        while (t.getCause() != null) {
            t = t.getCause();
        }
        return t.getClass().getSimpleName() + ": " + t.getMessage();
    }
}
