package dev.systemoneflink.engine;

import dev.systemoneflink.model.Decision;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** Turns a state into typed decisions. Implementations must be thread-safe and non-blocking. */
public interface DecisionEngine {
    /** {@code state} is a String or a JSON-serializable object/array. */
    CompletableFuture<Map<String, Decision>> decide(Object state);
}
