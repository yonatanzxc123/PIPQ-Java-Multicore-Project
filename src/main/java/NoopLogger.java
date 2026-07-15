public class NoopLogger implements PipqLogger {
    // Logger used when running on server.
    @Override
    public void log(String message, Long timestamp, int threadId) {
        // do nothing.
    }

    @Override
    public void log(String message, Long timestamp, int threadId, Long key, Object value) {
        // still do nothing.
    }
}
