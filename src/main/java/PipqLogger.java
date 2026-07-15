public interface PipqLogger {
    void log(String message, Long timestamp, int threadId);
    void log(String message, Long timestamp, int threadId, Long key, Object value);
}
