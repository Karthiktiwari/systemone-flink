package dev.systemoneflink.engine;

/** Failure to obtain decisions. {@code retryable} hints whether a later attempt may succeed. */
public final class DecisionException extends RuntimeException {
    private final boolean retryable;

    public DecisionException(String message, boolean retryable, Throwable cause) {
        super(message, cause);
        this.retryable = retryable;
    }

    public boolean retryable() {
        return retryable;
    }
}
