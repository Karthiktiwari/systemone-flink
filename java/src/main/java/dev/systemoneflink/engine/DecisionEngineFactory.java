package dev.systemoneflink.engine;

import java.io.Serializable;

/** Creates an engine inside each Flink task (engines hold connections and are not serialized). */
@FunctionalInterface
public interface DecisionEngineFactory extends Serializable {
    DecisionEngine create();
}
