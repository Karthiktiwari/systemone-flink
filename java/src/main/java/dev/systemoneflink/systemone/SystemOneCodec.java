package dev.systemoneflink.systemone;

import dev.systemoneflink.engine.DecisionException;
import dev.systemoneflink.model.Decision;
import dev.systemoneflink.model.DecisionSpec;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/** Pure mapping between a {@link DecisionSpec} and the System One JSON wire format. */
final class SystemOneCodec {
    private static final ObjectMapper JSON = new ObjectMapper();

    private final DecisionSpec spec;

    SystemOneCodec(DecisionSpec spec) {
        this.spec = spec;
    }

    String encode(Object state) {
        if (state instanceof String s && s.length() > spec.maxStateChars()) {
            state = s.substring(s.length() - spec.maxStateChars());
        }
        Map<String, Object> questions = new LinkedHashMap<>();
        spec.questions().forEach((id, q) -> questions.put(id, q.toWire()));
        Map<String, Object> req = new LinkedHashMap<>();
        req.put("model", spec.model());
        req.put("state", state);
        req.put("questions", questions);
        try {
            return JSON.writeValueAsString(req);
        } catch (JsonProcessingException e) {
            throw new DecisionException("cannot serialize state", false, e);
        }
    }

    Map<String, Decision> decode(String json) {
        JsonNode answers;
        try {
            answers = JSON.readTree(json).path("answers");
        } catch (IOException e) {
            throw new DecisionException("invalid JSON response", false, e);
        }
        Map<String, Decision> out = new LinkedHashMap<>();
        for (String id : spec.questions().keySet()) {
            JsonNode a = answers.get(id);
            if (a == null) {
                throw new DecisionException("response has no answer for question '" + id + "'", false, null);
            }
            String type = a.path("type").asText();
            Object value = switch (type) {
                case "choice" -> a.path("choice").asText();
                case "score" -> a.path("score").asDouble();
                case "noul" -> a.path("noul").asDouble();
                default -> throw new DecisionException("question '" + id + "': unexpected type " + type, false, null);
            };
            Map<String, Double> probs = new LinkedHashMap<>();
            a.path("probabilities").fields().forEachRemaining(e -> probs.put(e.getKey(), e.getValue().asDouble()));
            Double confidence = a.hasNonNull("confidence") ? a.get("confidence").asDouble() : null;
            out.put(id, new Decision(type, value, probs, confidence));
        }
        return out;
    }
}
