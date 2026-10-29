package dev.systemoneflink;

import dev.systemoneflink.engine.DecisionEngineFactory;
import dev.systemoneflink.engine.DecisionException;
import dev.systemoneflink.flink.DecideAsyncFunction;
import dev.systemoneflink.flink.Decided;
import dev.systemoneflink.model.DecisionSpec;
import dev.systemoneflink.model.Question;
import dev.systemoneflink.systemone.SystemOneClient;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * Runs against a real System One server (e.g. {@code laya-serve}). Enabled only when
 * {@code -Djev.endpoint=http://127.0.0.1:8000/v1/systemone} is set; {@code -Djev.apiKey} and
 * {@code -Djev.model} are optional.
 */
@EnabledIfSystemProperty(named = "jev.endpoint", matches = ".+")
class LiveServerTest {
    static final String ENDPOINT = System.getProperty("jev.endpoint");
    static final String API_KEY = System.getProperty("jev.apiKey");
    static final String MODEL = System.getProperty("jev.model", "jev-latest");

    static DecisionSpec spec() {
        LinkedHashMap<String, String> subsystems = new LinkedHashMap<>();
        subsystems.put("database", "database or connection pool problems");
        subsystems.put("authentication", "login, tokens, permissions");
        subsystems.put("network", "timeouts, DNS, connection resets");
        subsystems.put("benign", "routine noise that needs no action");
        return DecisionSpec.builder()
                .model(MODEL)
                .question("is_failure", new Question.Noul("This log line reports a real failure that needs attention."))
                .question("subsystem", new Question.Choice("Which subsystem does this log line concern?", subsystems))
                .question("severity", new Question.Score("How severe is this log line?",
                        List.of("informational", "warning", "error", "critical")))
                .build();
    }

    static SystemOneClient client() {
        return SystemOneClient.create(spec(), ENDPOINT, API_KEY, Duration.ofSeconds(60), 1);
    }

    @Test
    void answersAllThreeQuestionTypes() throws Exception {
        var d = client().decide(
                "2026-10-29 14:02:11 ERROR HikariPool-1 - Connection is not available, request timed out after 30000ms")
                .get();
        System.out.println("LIVE decisions: " + d);
        assertEquals(java.util.Set.of("is_failure", "subsystem", "severity"), d.keySet());
        double failure = (double) d.get("is_failure").value();
        assertTrue(failure >= 0 && failure <= 1, "p(true) in [0,1]: " + failure);
        String subsystem = (String) d.get("subsystem").value();
        assertTrue(spec().questions().get("subsystem") instanceof Question.Choice c && c.options().containsKey(subsystem));
        assertEquals(1.0, d.get("subsystem").probabilities().values().stream().mapToDouble(x -> x).sum(), 0.02);
        double severity = (double) d.get("severity").value();
        assertTrue(severity >= 0 && severity <= 3, "level index in [0,3]: " + severity);
    }

    @Test
    void separatesFailureFromNoiseInOrdering() throws Exception {
        var bad = client().decide("ERROR HikariPool-1 - Connection is not available, request timed out after 30000ms").get();
        var ok = client().decide("INFO Scheduled job cleanup completed in 120ms, 0 items removed").get();
        double pBad = (double) bad.get("is_failure").value();
        double pOk = (double) ok.get("is_failure").value();
        System.out.println("LIVE p(failure): error line=" + pBad + " routine line=" + pOk);
        assertTrue(pBad > pOk, "error line should score above routine line: " + pBad + " vs " + pOk);
    }

    @Test
    void wrongApiKeyFailsFastWithoutRetry() {
        var c = SystemOneClient.create(spec(), ENDPOINT, "definitely-wrong", Duration.ofSeconds(30), 3);
        var e = org.junit.jupiter.api.Assertions.assertThrows(java.util.concurrent.ExecutionException.class,
                () -> c.decide("x").get());
        System.out.println("LIVE bad key: " + e.getCause().getMessage());
        assertTrue(e.getCause().getMessage().contains("401") || e.getCause().getMessage().contains("403"),
                e.getCause().getMessage());
        assertFalse(((DecisionException) e.getCause()).retryable());
    }

    @Test
    void flinkJobEnrichesCandidatesFromRealServer() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> candidates = env.fromData(
                "tpl-db ERROR HikariPool-1 - Connection is not available, request timed out after 30000ms",
                "tpl-db ERROR HikariPool-1 - Connection is not available, request timed out after 31000ms",
                "tpl-auth WARN invalid token for user 4412, access denied");
        DecisionSpec spec = spec();
        DecisionEngineFactory factory = () -> SystemOneClient.create(
                spec, ENDPOINT, API_KEY, Duration.ofSeconds(60), 1);
        DataStream<Decided<String>> out = DecideAsyncFunction.apply(
                candidates, factory, e -> e.substring(e.indexOf(' ') + 1), e -> e.substring(0, e.indexOf(' ')),
                1000, 120_000, 1);

        List<Decided<String>> results = new ArrayList<>();
        try (CloseableIterator<Decided<String>> it = out.executeAndCollect()) {
            it.forEachRemaining(results::add);
        }
        results.forEach(r -> System.out.println("LIVE flink: " + r));
        assertEquals(3, results.size());
        assertTrue(results.stream().noneMatch(Decided::failed), results.toString());
        assertEquals(1, results.stream().filter(Decided::cached).count(), "second tpl-db event comes from the cache");
        results.forEach(r -> assertNotNull(r.decisions().get("subsystem").value()));
    }
}
