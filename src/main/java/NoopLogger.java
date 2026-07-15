// Logger used when running on the course server: both methods are no-ops.
public class NoopLogger implements PipqLogger {
    @Override
    public void log(String message, Long timestamp, int threadId) {
        // do nothing
    }

    @Override
    public void log(String message, Long timestamp, int threadId, Long key, Object value) {
        // still do nothing
    }
}
