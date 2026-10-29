package dev.systemoneflink.model;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

/** What to ask: model name, typed questions and the state size limit. Says nothing about transport. */
public final class DecisionSpec implements Serializable {
    private final String model;
    private final int maxStateChars;
    private final LinkedHashMap<String, Question> questions;

    private DecisionSpec(Builder b) {
        if (b.questions.isEmpty()) {
            throw new IllegalArgumentException("at least one question is required");
        }
        model = b.model;
        maxStateChars = b.maxStateChars;
        questions = new LinkedHashMap<>(b.questions);
    }

    public String model() { return model; }

    public int maxStateChars() { return maxStateChars; }

    /** Questions in insertion order; unmodifiable. */
    public Map<String, Question> questions() { return java.util.Collections.unmodifiableMap(questions); }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String model = "jev-latest";
        private int maxStateChars = 6000;
        private final Map<String, Question> questions = new LinkedHashMap<>();

        public Builder model(String v) { model = v; return this; }
        /** Longer string states keep only their tail. */
        public Builder maxStateChars(int v) { maxStateChars = v; return this; }
        public Builder question(String id, Question q) { questions.put(id, q); return this; }
        public DecisionSpec build() { return new DecisionSpec(this); }
    }
}
