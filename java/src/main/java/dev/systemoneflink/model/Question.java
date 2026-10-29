package dev.systemoneflink.model;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** A typed question. Everything about a question is set explicitly by the caller. */
public sealed interface Question extends Serializable {

    Map<String, Object> toWire();

    /** Pick one label. {@code options} maps label to an optional description (may be null). */
    record Choice(String instructions, LinkedHashMap<String, String> options) implements Question {
        public Map<String, Object> toWire() {
            return Map.of("type", "choice", "instructions", instructions, "criteria", options);
        }
    }

    /** Ordered levels (2-10), lowest first. */
    record Score(String instructions, List<String> levels) implements Question {
        public Map<String, Object> toWire() {
            return Map.of("type", "score", "instructions", instructions, "criteria", levels);
        }
    }

    /** Yes/no statement; the answer is p(true). */
    record Noul(String instructions) implements Question {
        public Map<String, Object> toWire() {
            return Map.of("type", "noul", "instructions", instructions);
        }
    }
}
