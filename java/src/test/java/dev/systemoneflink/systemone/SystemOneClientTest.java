package dev.systemoneflink.systemone;

import dev.systemoneflink.engine.DecisionException;
import dev.systemoneflink.model.DecisionSpec;
import dev.systemoneflink.model.Question;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class SystemOneClientTest {
    static final ObjectMapper JSON = new ObjectMapper();
    static final String OK = """
            {"model":"m","answers":{
              "category":{"type":"choice","choice":"db","probabilities":{"db":0.9,"noise":0.1},"confidence":0.8},
              "severity":{"type":"score","score":1.5,"probabilities":{"0":0.1,"1":0.4,"2":0.5},"confidence":0.5},
              "is_failure":{"type":"noul","noul":0.93}}}""";

    /** Scripted transport: returns queued responses (or failures), recording each request body. */
    static final class FakeTransport implements Transport {
        final List<String> bodies = new ArrayList<>();
        final List<Object> script = new ArrayList<>(); // Response or Throwable
        final AtomicInteger calls = new AtomicInteger();

        FakeTransport then(Object r) { script.add(r); return this; }

        public CompletableFuture<Response> post(String body) {
            bodies.add(body);
            Object next = script.get(Math.min(calls.getAndIncrement(), script.size() - 1));
            return next instanceof Throwable t ? CompletableFuture.failedFuture(t)
                    : CompletableFuture.completedFuture((Response) next);
        }
    }

    static Transport.Response resp(int status, String body) { return new Transport.Response(status, body); }

    static DecisionSpec.Builder spec() {
        LinkedHashMap<String, String> opts = new LinkedHashMap<>();
        opts.put("db", "database");
        opts.put("noise", null);
        return DecisionSpec.builder()
                .model("openjev")
                .question("category", new Question.Choice("Which subsystem?", opts))
                .question("severity", new Question.Score("How severe?", List.of("low", "medium", "high")))
                .question("is_failure", new Question.Noul("This log line reports a failure."));
    }

    static SystemOneClient client(FakeTransport t, int retries) {
        return new SystemOneClient(spec().build(), t, SystemOneClient.retries(retries, Duration.ofMillis(1)));
    }

    static Throwable failure(CompletableFuture<?> f) {
        return assertThrows(ExecutionException.class, f::get).getCause();
    }

    @Test
    void encodesTypedQuestions() throws Exception {
        FakeTransport t = new FakeTransport().then(resp(200, OK));
        client(t, 0).decide("ERROR pool exhausted").get();
        JsonNode req = JSON.readTree(t.bodies.get(0));
        assertEquals("openjev", req.get("model").asText());
        assertEquals("ERROR pool exhausted", req.get("state").asText());
        assertEquals("database", req.at("/questions/category/criteria/db").asText());
        assertTrue(req.at("/questions/category/criteria/noise").isNull());
        assertEquals("low", req.at("/questions/severity/criteria/0").asText());
        assertEquals("noul", req.at("/questions/is_failure/type").asText());
    }

    @Test
    void decodesAllThreeTypes() throws Exception {
        var d = client(new FakeTransport().then(resp(200, OK)), 0).decide("x").get();
        assertEquals("db", d.get("category").value());
        assertEquals(0.9, d.get("category").probabilities().get("db"));
        assertEquals(0.8, d.get("category").confidence());
        assertEquals(1.5, d.get("severity").value());
        assertEquals(0.93, d.get("is_failure").value());
        assertNull(d.get("is_failure").confidence());
    }

    @Test
    void retriesOverloadAndNetworkErrorsThenSucceeds() throws Exception {
        FakeTransport t = new FakeTransport()
                .then(resp(529, "{}")).then(new java.io.IOException("reset")).then(resp(200, OK));
        client(t, 2).decide("x").get();
        assertEquals(3, t.calls.get());
    }

    @Test
    void givesUpAfterMaxRetriesAsRetryable() {
        FakeTransport t = new FakeTransport().then(resp(503, "{}"));
        var e = (DecisionException) failure(client(t, 2).decide("x"));
        assertEquals(3, t.calls.get());
        assertTrue(e.retryable());
        assertTrue(e.getMessage().contains("503"), e.getMessage());
    }

    @Test
    void doesNotRetryClientErrors() {
        FakeTransport t = new FakeTransport().then(resp(422, "{}"));
        var e = (DecisionException) failure(client(t, 3).decide("x"));
        assertEquals(1, t.calls.get());
        assertFalse(e.retryable());
        assertTrue(e.getMessage().contains("422"));
    }

    @Test
    void malformedOrIncompleteResponsesFailWithoutRetry() {
        FakeTransport bad = new FakeTransport().then(resp(200, "not json"));
        assertTrue(failure(client(bad, 2).decide("x")).getMessage().contains("invalid JSON"));
        assertEquals(1, bad.calls.get());
        FakeTransport missing = new FakeTransport().then(resp(200, "{\"answers\":{}}"));
        assertTrue(failure(client(missing, 0).decide("x")).getMessage().contains("no answer for question"));
    }

    @Test
    void truncatesLongStateKeepingTail() throws Exception {
        FakeTransport t = new FakeTransport().then(resp(200, OK));
        new SystemOneClient(spec().maxStateChars(5).build(), t, SystemOneClient.retries(0, Duration.ofMillis(1))).decide("0123456789").get();
        assertEquals("56789", JSON.readTree(t.bodies.get(0)).get("state").asText());
    }

    @Test
    void acceptsStructuredState() throws Exception {
        FakeTransport t = new FakeTransport().then(resp(200, OK));
        client(t, 0).decide(java.util.Map.of("level", "ERROR", "count", 3)).get();
        assertEquals(3, JSON.readTree(t.bodies.get(0)).at("/state/count").asInt());
    }

    @Test
    void specRequiresAQuestion() {
        assertThrows(IllegalArgumentException.class, () -> DecisionSpec.builder().build());
    }
}
