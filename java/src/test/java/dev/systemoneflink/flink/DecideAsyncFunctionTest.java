package dev.systemoneflink.flink;

import dev.systemoneflink.engine.DecisionEngineFactory;
import dev.systemoneflink.engine.DecisionException;
import dev.systemoneflink.model.Decision;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Operator behaviour in real local Flink jobs, with a fake engine (no network). */
class DecideAsyncFunctionTest {
    // Flink runs the job in-process, so a static counter is visible to the engine.
    static final AtomicInteger CALLS = new AtomicInteger();

    @BeforeEach
    void reset() {
        CALLS.set(0);
    }

    static DecisionEngineFactory echoEngine() {
        return () -> state -> {
            CALLS.incrementAndGet();
            // Kryo cannot serialize immutable Map.of(); the real codec returns LinkedHashMap too.
            Map<String, Decision> m = new java.util.LinkedHashMap<>();
            m.put("echo", new Decision("choice", state, new java.util.LinkedHashMap<>(), null));
            return CompletableFuture.completedFuture(m);
        };
    }

    static DecisionEngineFactory failingEngine() {
        return () -> state -> CompletableFuture.failedFuture(new DecisionException("HTTP 422", false, null));
    }

    static List<Decided<String>> run(DecisionEngineFactory f, int cacheSize, String... events) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> in = env.fromData(events);
        DataStream<Decided<String>> out = DecideAsyncFunction.apply(
                in, f, e -> e, e -> e.split(" ")[1], cacheSize, 10_000, 1);
        List<Decided<String>> results = new ArrayList<>();
        try (CloseableIterator<Decided<String>> it = out.executeAndCollect()) {
            it.forEachRemaining(results::add);
        }
        return results;
    }

    @Test
    void attachesDecisionsToEachEvent() throws Exception {
        var results = run(echoEngine(), 0, "a t1", "b t2");
        assertEquals(2, results.size());
        assertTrue(results.stream().noneMatch(Decided::failed));
        assertEquals("a t1", results.get(0).decisions().get("echo").value());
        assertEquals(2, CALLS.get());
    }

    @Test
    void cachesByKey() throws Exception {
        var results = run(echoEngine(), 100, "a t1", "b t1", "c t1");
        assertEquals(3, results.size());
        assertEquals(1, CALLS.get(), "same key must hit the engine once (capacity 1 serialises lookups)");
        assertEquals(2, results.stream().filter(Decided::cached).count());
    }

    @Test
    void cacheCanBeDisabled() throws Exception {
        run(echoEngine(), 0, "a t1", "b t1");
        assertEquals(2, CALLS.get());
    }

    @Test
    void failuresAreEmittedNotThrown() throws Exception {
        var results = run(failingEngine(), 100, "a t1", "b t1");
        assertEquals(2, results.size());
        assertTrue(results.stream().allMatch(Decided::failed));
        assertTrue(results.get(0).error().contains("422"), results.get(0).error());
        assertFalse(results.get(0).cached());
    }
}
