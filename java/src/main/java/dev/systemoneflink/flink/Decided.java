package dev.systemoneflink.flink;

import dev.systemoneflink.model.Decision;

import java.io.Serializable;
import java.util.Map;

/**
 * An input event with its decisions. On failure {@code decisions()} is null and {@code error()} is set.
 * A plain class (not a record) because Flink 1.x's Kryo cannot serialize records.
 */
public final class Decided<T> implements Serializable {
    private final T event;
    private final Map<String, Decision> decisions;
    private final String error;
    private final boolean cached;

    public Decided(T event, Map<String, Decision> decisions, String error, boolean cached) {
        this.event = event;
        this.decisions = decisions;
        this.error = error;
        this.cached = cached;
    }

    public T event() { return event; }
    public Map<String, Decision> decisions() { return decisions; }
    public String error() { return error; }
    public boolean cached() { return cached; }
    public boolean failed() { return decisions == null; }

    @Override
    public String toString() {
        return "Decided{" + event + ", " + (failed() ? "error=" + error : decisions) + ", cached=" + cached + "}";
    }
}
