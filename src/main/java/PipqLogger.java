/**
 * Logging hook for tracing PIPQ operations during debugging and tests. {@link NoopLogger} is
 * used on the course server; {@link ConcurrentLogger} is used
 * elsewhere to record events via log4j2.
 */
public interface PipqLogger {
    void log(String message, Long timestamp, int threadId);
    void log(String message, Long timestamp, int threadId, Long key, Object value);
}
