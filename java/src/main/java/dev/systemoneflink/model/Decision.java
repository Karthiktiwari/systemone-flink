package dev.systemoneflink.model;

import java.io.Serializable;
import java.util.Map;

/**
 * Normalised answer to one question. A plain class (not a record) because Flink 1.x's Kryo cannot
 * serialize records.
 *
 * <p>value: choice is the chosen label (String); score is the expected level index (Double); noul is
 * p(true) (Double). confidence is null for noul or when the server does not report it.
 */
public final class Decision implements Serializable {
    private final String type;
    private final Object value;
    private final Map<String, Double> probabilities;
    private final Double confidence;

    public Decision(String type, Object value, Map<String, Double> probabilities, Double confidence) {
        this.type = type;
        this.value = value;
        this.probabilities = probabilities;
        this.confidence = confidence;
    }

    public String type() { return type; }
    public Object value() { return value; }
    public Map<String, Double> probabilities() { return probabilities; }
    public Double confidence() { return confidence; }

    @Override
    public String toString() {
        return "Decision{" + type + "=" + value + ", confidence=" + confidence + "}";
    }
}
