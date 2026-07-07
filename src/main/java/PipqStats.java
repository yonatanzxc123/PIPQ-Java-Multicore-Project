import java.util.concurrent.atomic.LongAdder;

public final class PipqStats {
    private final LongAdder totalInserts = new LongAdder();
    private final LongAdder totalDeleteMins = new LongAdder();
    private final LongAdder fastPathInserts = new LongAdder();
    private final LongAdder slowerPathInserts = new LongAdder();
    private final LongAdder slowestPathInserts = new LongAdder();
    private final LongAdder leaderInserts = new LongAdder();
    private final LongAdder leaderDeleteMins = new LongAdder();
    private final LongAdder leaderDeleteMaxByThread = new LongAdder();
    private final LongAdder workerHeapInserts = new LongAdder();
    private final LongAdder workerHeapDeleteMinPromotions = new LongAdder();

    void recordTotalInsert() {
        totalInserts.increment();
    }

    void recordTotalDeleteMin() {
        totalDeleteMins.increment();
    }

    void recordFastPathInsert() {
        fastPathInserts.increment();
    }

    void recordSlowerPathInsert() {
        slowerPathInserts.increment();
    }

    void recordSlowestPathInsert() {
        slowestPathInserts.increment();
    }

    void recordLeaderInsert() {
        leaderInserts.increment();
    }

    void recordLeaderDeleteMin() {
        leaderDeleteMins.increment();
    }

    void recordLeaderDeleteMaxByThread() {
        leaderDeleteMaxByThread.increment();
    }

    void recordWorkerHeapInsert() {
        workerHeapInserts.increment();
    }

    void recordWorkerHeapDeleteMinPromotion() {
        workerHeapDeleteMinPromotions.increment();
    }

    public long totalInserts() {
        return totalInserts.sum();
    }

    public long totalDeleteMins() {
        return totalDeleteMins.sum();
    }

    public long fastPathInserts() {
        return fastPathInserts.sum();
    }

    public long slowerPathInserts() {
        return slowerPathInserts.sum();
    }

    public long slowestPathInserts() {
        return slowestPathInserts.sum();
    }

    public long leaderInserts() {
        return leaderInserts.sum();
    }

    public long leaderDeleteMins() {
        return leaderDeleteMins.sum();
    }

    public long leaderDeleteMaxByThread() {
        return leaderDeleteMaxByThread.sum();
    }

    public long workerHeapInserts() {
        return workerHeapInserts.sum();
    }

    public long workerHeapDeleteMinPromotions() {
        return workerHeapDeleteMinPromotions.sum();
    }
}
