# systemone-flink

[![CI](https://github.com/Karthiktiwari/systemone-flink/actions/workflows/ci.yml/badge.svg)](https://github.com/Karthiktiwari/systemone-flink/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

An Apache Flink async operator that sends candidate events, for example logs flagged by a cheap detector, to a System One compatible decision model (`POST /v1/systemone`) and attaches the typed answers: a choice, a score, or a yes/no probability.

<p align="center"><img src="docs/flow.svg" alt="Flow: logs, cheap detectors, DecideAsyncFunction, model server, decisions" width="100%"></p>

The endpoint, model name, questions, retries, cache and timeouts are all configuration. Timeouts and HTTP errors are emitted as `Decided` records with `failed()` set instead of failing the job. This is an independent project, not affiliated with TypeSafe or any model vendor.

## Usage

Requires Java 17+ and Flink 1.20. Not on Maven Central yet; install locally with `mvn -f java/pom.xml install`.

```xml
<dependency>
  <groupId>dev.systemoneflink</groupId>
  <artifactId>systemone-flink</artifactId>
  <version>0.2.0</version>
</dependency>
```

```java
DecisionSpec spec = DecisionSpec.builder()
    .model("my-model")
    .question("is_failure", new Question.Noul("This log line reports a real failure."))
    .question("subsystem", new Question.Choice("Which subsystem is affected?",
        new LinkedHashMap<>(Map.of("database", "connection pool problems", "benign", "routine noise"))))
    .question("severity", new Question.Score("How severe is this?", List.of("info", "warning", "error", "critical")))
    .build();

DecisionEngineFactory engine = () -> SystemOneClient.create(
    spec, "http://127.0.0.1:11434/v1/systemone", System.getenv("SYSTEMONE_API_KEY"),
    Duration.ofSeconds(5), 2 /* max retries */);

DataStream<Decided<Candidate>> decided = DecideAsyncFunction.apply(
    candidates, engine,
    c -> c.text(),         // state shown to the model
    c -> c.templateId(),   // cache key; null disables caching
    10_000,                // cache entries per task
    15_000,                // per-event budget incl. retries (ms)
    16);                   // max in-flight requests per task
```

`Decision.value()` is the chosen label (choice), the expected level index (score) or p(true) (yes/no). `probabilities()` and `confidence()` are also available. Cache keys must imply the same state, for example a template id. The model server sees up to `parallelism x capacity` concurrent requests.

## Model servers

Works with any server that implements `POST /v1/systemone`, such as [Ollama](https://ollama.com), OpenRouter and [Laya](https://huggingface.co/convaiinnovations/laya).

## Related work

- [HydroJEV](https://arxiv.org/abs/2610.02048): fault attribution with System One models.
- [Typed Decision Models under Prevalence Shift](https://arxiv.org/abs/2610.26325): calibration and base rates.
- [HSMLog](https://arxiv.org/abs/2608.29773): small model picks log candidates, a second stage validates.

## License

[Apache-2.0](LICENSE)