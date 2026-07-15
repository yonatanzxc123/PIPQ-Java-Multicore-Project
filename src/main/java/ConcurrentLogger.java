import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * {@link PipqLogger} implementation that writes each event to log4j2 at debug level, with the
 * timestamp converted to milliseconds relative to when this class was first loaded. Intended
 * for local debugging and tests, not for the course server.
 */
public class ConcurrentLogger implements PipqLogger{
    private static final Logger log4j2Logger = LogManager.getLogger(ConcurrentLogger.class);
    private static final Long START_TIME = System.nanoTime();

    @Override
    public void log(String message, Long timestamp, int threadId) {
        log4j2Logger.debug("message: {}, timestamp: {}, threadId: {}", message, getRelativeMs(timestamp), threadId);
    }

    @Override
    public void log(String message, Long timestamp, int threadId, Long key, Object value) {
        log4j2Logger.debug("message: {}, timestamp: {}, threadId: {}, key: {}, value: {}",
                            message, getRelativeMs(timestamp), threadId, key, value);
    }

    // ms since START_TIME
    private double getRelativeMs(Long currentTime) {
        return (currentTime - START_TIME) / 1_000_000.0;
    }
}
