package dev.systemoneflink;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.systemoneflink.engine.DecisionEngineFactory;
import dev.systemoneflink.flink.DecideAsyncFunction;
import dev.systemoneflink.flink.Decided;
import dev.systemoneflink.model.DecisionSpec;
import dev.systemoneflink.model.Question;
import dev.systemoneflink.systemone.SystemOneClient;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * End-to-end demo on the LogHub BGL 2k sample: a cheap detector picks candidates, systemone-flink asks a live
 * model server about them, and the result is scored against the dataset's alert labels.
 *
 * <p>Enabled with {@code -Djev.endpoint=... -Djev.bgl=path\to\BGL_2k.csv}; optional {@code -Djev.apiKey},
 * {@code -Djev.model}, {@code -Djev.out} (CSV of per-candidate scores).
 */
@EnabledIfSystemProperty(named = "jev.bgl", matches = ".+")
class BglPipelineTest {
    public static class LogLine {
        public String level;
        public String component;
        public String templateId;
        public String content;
        public boolean alert;

        public LogLine() {}

        String asState() {
            return "[" + component + "] [" + level + "] " + content;
        }
    }

    /** Cheap stage: anything above INFO, or one of the first three sightings of a template. */
    static class CheapDetector extends KeyedProcessFunction<String, LogLine, LogLine> {
        private transient ValueState<Integer> seen;

        @Override
        public void open(OpenContext ctx) {
            seen = getRuntimeContext().getState(new ValueStateDescriptor<>("seen", Integer.class));
        }

        @Override
        public void processElement(LogLine line, Context ctx, Collector<LogLine> out) throws Exception {
            int n = seen.value() == null ? 1 : seen.value() + 1;
            seen.update(n);
            if (!"INFO".equals(line.level) || n <= 3) {
                out.collect(line);
            }
        }
    }

    static List<LogLine> load(Path csv) throws Exception {
        List<LogLine> lines = new ArrayList<>();
        try (Reader r = Files.newBufferedReader(csv)) {
            for (CSVRecord rec : CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).build().parse(r)) {
                LogLine l = new LogLine();
                l.level = rec.get("Level");
                l.component = rec.get("Component");
                l.templateId = rec.get("EventId");
                l.content = rec.get("Content");
                l.alert = !"-".equals(rec.get("Label"));
                lines.add(l);
            }
        }
        return lines;
    }

    static DecisionSpec spec(String model) {
        LinkedHashMap<String, String> kind = new LinkedHashMap<>();
        kind.put("hardware", "hardware, memory or interconnect fault");
        kind.put("software", "application or kernel software failure");
        kind.put("benign", "routine or self-corrected event that needs no action");
        return DecisionSpec.builder()
                .model(model)
                .question("is_failure", new Question.Noul("This log " + System.getProperty("jev.unit", "line") + " reports a real failure that needs operator attention."))
                .question("kind", new Question.Choice("What kind of event is this?", kind))
                .build();
    }

    @Test
    void detectThenDecide() throws Exception {
        String endpoint = System.getProperty("jev.endpoint");
        String apiKey = System.getProperty("jev.apiKey");
        String model = System.getProperty("jev.model", "jev-latest");
        List<LogLine> all = load(Path.of(System.getProperty("jev.bgl")));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        DataStream<LogLine> candidates = env.fromData(all)
                .keyBy(l -> l.templateId)
                .process(new CheapDetector());

        DecisionSpec spec = spec(model);
        DecisionEngineFactory engine = () -> SystemOneClient.create(spec, endpoint, apiKey, Duration.ofSeconds(60), 2);
        DataStream<Decided<LogLine>> decided = DecideAsyncFunction.apply(
                candidates, engine, LogLine::asState, null, 0, 600_000, Integer.getInteger("jev.capacity", 16));

        List<Decided<LogLine>> results = new ArrayList<>();
        long start = System.nanoTime();
        try (CloseableIterator<Decided<LogLine>> it = decided.executeAndCollect()) {
            it.forEachRemaining(results::add);
        }
        double seconds = (System.nanoTime() - start) / 1e9;

        long failures = results.stream().filter(Decided::failed).count();
        List<Decided<LogLine>> ok = results.stream().filter(d -> !d.failed()).toList();
        long totalAlerts = all.stream().filter(l -> l.alert).count();

        System.out.printf("%nBGL: %d lines, %d alerts. Cheap detector kept %d candidates (%d alerts). "
                + "Model calls: %d ok, %d failed, %.1fs total (%.0f ms/candidate).%n",
                all.size(), totalAlerts, results.size(), results.stream().filter(d -> d.event().alert).count(),
                ok.size(), failures, seconds, 1000 * seconds / Math.max(1, results.size()));
        report(ok, totalAlerts);

        String out = System.getProperty("jev.out");
        if (out != null) {
            List<String> rows = new ArrayList<>(List.of("p_failure,kind,alert,level,content"));
            for (var d : ok) {
                rows.add(String.format("%.4f,%s,%s,%s,\"%s\"", p(d), d.decisions().get("kind").value(), d.event().alert,
                        d.event().level, d.event().content.replace("\"", "'")));
            }
            Files.write(Path.of(out), rows);
        }
        assertFalse(ok.isEmpty());
        assertTrue(failures == 0, failures + " model calls failed");
    }

    static double p(Decided<LogLine> d) {
        return (double) d.decisions().get("is_failure").value();
    }

    static void report(List<Decided<LogLine>> ok, long totalAlerts) {
        long candAlerts = ok.stream().filter(d -> d.event().alert).count();
        System.out.printf("%-26s %6s %9s %7s%n", "stage", "flagged", "precision", "recall");
        System.out.printf("%-26s %6d %9.2f %7.2f%n", "cheap detector alone", ok.size(),
                (double) candAlerts / ok.size(), (double) candAlerts / totalAlerts);
        for (double t : new double[] {0.5, 0.7, 0.8, 0.9, 0.95}) {
            long flagged = ok.stream().filter(d -> p(d) >= t).count();
            long tp = ok.stream().filter(d -> p(d) >= t && d.event().alert).count();
            System.out.printf("%-26s %6d %9.2f %7.2f%n", "detector + model p>=" + t, flagged,
                    flagged == 0 ? 0.0 : (double) tp / flagged, (double) tp / totalAlerts);
        }
        System.out.printf("ROC AUC of p(failure) over candidates: %.3f%n", auc(ok));
        System.out.printf("mean p(failure): alerts %.2f, non-alerts %.2f%n",
                ok.stream().filter(d -> d.event().alert).mapToDouble(BglPipelineTest::p).average().orElse(0),
                ok.stream().filter(d -> !d.event().alert).mapToDouble(BglPipelineTest::p).average().orElse(0));
    }

    /** Rank-based AUC (Mann-Whitney U), ties get the average rank. */
    static double auc(List<Decided<LogLine>> ok) {
        List<Decided<LogLine>> sorted = new ArrayList<>(ok);
        sorted.sort(Comparator.comparingDouble(BglPipelineTest::p));
        double rankSumPos = 0;
        long pos = 0;
        for (int i = 0; i < sorted.size(); ) {
            int j = i;
            while (j < sorted.size() && p(sorted.get(j)) == p(sorted.get(i))) {
                j++;
            }
            double avgRank = (i + 1 + j) / 2.0;
            for (int k = i; k < j; k++) {
                if (sorted.get(k).event().alert) {
                    rankSumPos += avgRank;
                    pos++;
                }
            }
            i = j;
        }
        long neg = sorted.size() - pos;
        return pos == 0 || neg == 0 ? Double.NaN : (rankSumPos - pos * (pos + 1) / 2.0) / ((double) pos * neg);
    }
}
