import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Course-server entry point and microbenchmark runner for the current original PIPQ baseline.
 *
 * <p>Keep this file dependency-free: the course server runs {@code MppRunner.main} from flat
 * Java 8 class files, with no Maven/JUnit/runtime extras available.</p>
 */
public final class MppRunner {
    private static final int[] THREAD_COUNTS = {1, 2, 4, 8, 16, 32};
    private static final long WARMUP_MILLIS = 1_000L;
    private static final long MEASURED_MILLIS = 5_000L;

    // Paper-like values with a real gap. Very small gaps are intentionally avoided here.
    private static final int CNTR_MIN = 10;
    private static final int CNTR_MAX = 100;

    private static final int RANDOM_KEY_BOUND = 1_000_000;
    private static final int MIXED_PREFILL_PER_THREAD = 10_000;
    private static final int DELETE_PREFILL_PER_THREAD = 50_000;
    private static final long BASE_SEED = 0x5eed1234L;

    private MppRunner() {
    }

    public static void main(String[] args) {
        List<String> output = new ArrayList<String>();

        try {
            for (int i = 0; i < Implementation.values().length; i++) {
                if (!runSanityCheck(Implementation.values()[i], output)) {
                    printAll(output);
                    System.out.println("DONE");
                    return;
                }
            }

            output.add(headerLine());
            for (int i = 0; i < Implementation.values().length; i++) {
                Implementation implementation = Implementation.values()[i];
                for (int w = 0; w < Workload.values().length; w++) {
                    Workload workload = Workload.values()[w];
                    for (int t = 0; t < THREAD_COUNTS.length; t++) {
                        int threadCount = THREAD_COUNTS[t];
                        try {
                            runPhase(implementation, workload, threadCount, WARMUP_MILLIS, false);
                            BenchmarkResult result = runPhase(implementation, workload, threadCount, MEASURED_MILLIS, true);
                            output.add(result.toCsvLine());
                        } catch (Throwable error) {
                            output.add(errorLine(implementation, workload, threadCount, error));
                        }
                    }
                }
            }
        } catch (Throwable fatal) {
            output.add("ERROR,FATAL," + sanitize(fatal.getClass().getSimpleName())
                    + "," + sanitize(fatal.getMessage()));
        }

        printAll(output);
        System.out.println("DONE");
    }

    private static boolean runSanityCheck(Implementation implementation, List<String> output) {
        Pipq<Integer> queue = implementation.createQueue(3);
        long[] keys = {9L, 1L, 7L, 2L, 5L, 3L};
        for (int i = 0; i < keys.length; i++) {
            queue.insert(keys[i], Integer.valueOf(i), i % queue.threadCount());
        }

        long previous = Long.MIN_VALUE;
        int removed = 0;
        Optional<Node<Integer>> current;
        while ((current = queue.deleteMin(0)).isPresent()) {
            long key = current.get().key();
            if (key < previous) {
                output.add("ERROR,SANITY," + implementation.name()
                        + ",deleteMin returned decreasing keys,previous=" + previous + ",current=" + key);
                return false;
            }
            previous = key;
            removed++;
        }

        if (removed != keys.length) {
            output.add("ERROR,SANITY," + implementation.name()
                    + ",expectedRemoved=" + keys.length + ",actualRemoved=" + removed);
            return false;
        }

        for (int tid = 0; tid < queue.threadCount(); tid++) {
            if (queue.leaderCounter(tid) < 0) {
                output.add("ERROR,SANITY," + implementation.name() + ",negativeLeaderCounter,tid=" + tid);
                return false;
            }
        }

        output.add("SANITY,OK," + implementation.name() + ",removed=" + removed
                + ",validationMethods=not_exposed");
        return true;
    }

    private static BenchmarkResult runPhase(Implementation implementation, Workload workload, int threadCount,
                                            long millis, boolean measured) throws Exception {
        final Pipq<Integer> queue = implementation.createQueue(threadCount);
        prefill(queue, workload, threadCount);
        StatsSnapshot statsBefore = measured ? StatsSnapshot.capture(queue.stats()) : null;

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<ThreadCounts>> futures = new ArrayList<Future<ThreadCounts>>();
        long durationNanos = millis * 1_000_000L;

        for (int tid = 0; tid < threadCount; tid++) {
            futures.add(executor.submit(new Worker(queue, workload, tid, start, durationNanos, measured)));
        }

        long startNanos = System.nanoTime();
        start.countDown();

        ThreadCounts totals = new ThreadCounts();
        try {
            for (int i = 0; i < futures.size(); i++) {
                totals.add(futures.get(i).get());
            }
        } finally {
            executor.shutdownNow();
        }

        long elapsedNanos = System.nanoTime() - startNanos;
        if (!measured) {
            return null;
        }

        return new BenchmarkResult(implementation, workload, threadCount, totals, elapsedNanos, queue, statsBefore);
    }

    private static void prefill(Pipq<Integer> queue, Workload workload, int threadCount) {
        int perThread = workload.prefillPerThread();
        if (perThread <= 0) {
            return;
        }

        for (int tid = 0; tid < threadCount; tid++) {
            Random random = new Random(BASE_SEED + workload.ordinal() * 10_000L + tid);
            for (int i = 0; i < perThread; i++) {
                queue.insert(random.nextInt(RANDOM_KEY_BOUND), Integer.valueOf(i), tid);
            }
        }
    }

    private static String headerLine() {
        return "RESULT,implementation,workload,threads,totalOps,insertOps,deleteOps,emptyDeleteOps,"
                + "seconds,throughputOpsPerSec,fastPathInserts,slowerPathInserts,slowestPathInserts,"
                + "leaderInserts,leaderDeleteMins,leaderDeleteMaxByThread,workerHeapInserts,"
                + "workerHeapDeleteMinPromotions,helpUpserts,fastPathUpserts,leaderSize,"
                + "minLeaderCounter,postCheck";
    }

    private static String errorLine(Implementation implementation, Workload workload, int threadCount, Throwable error) {
        Throwable cause = error;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        return "ERROR," + implementation.name() + "," + workload.name() + "," + threadCount + ","
                + sanitize(cause.getClass().getSimpleName()) + "," + sanitize(cause.getMessage());
    }

    private static String sanitize(String text) {
        if (text == null) {
            return "";
        }
        return text.replace(',', ';').replace('\n', ' ').replace('\r', ' ');
    }

    private static void printAll(List<String> output) {
        for (int i = 0; i < output.size(); i++) {
            System.out.println(output.get(i));
        }
    }

    private enum Workload {
        INSERT_100(100, 0),
        INSERT_95_DELETE_5(95, MIXED_PREFILL_PER_THREAD),
        INSERT_50_DELETE_50(50, MIXED_PREFILL_PER_THREAD),
        PREFILL_DELETE_100(0, DELETE_PREFILL_PER_THREAD);

        private final int insertPercent;
        private final int prefillPerThread;

        Workload(int insertPercent, int prefillPerThread) {
            this.insertPercent = insertPercent;
            this.prefillPerThread = prefillPerThread;
        }

        boolean shouldInsert(Random random) {
            return insertPercent > 0 && random.nextInt(100) < insertPercent;
        }

        int prefillPerThread() {
            return prefillPerThread;
        }
    }

    private enum Implementation {
        OG_PIPQ,
        HEAP_PIPQ;

        Pipq<Integer> createQueue(int threadCount) {
            if (this == HEAP_PIPQ) {
                return Pipq.withCombiningHeapLeader(threadCount, CNTR_MIN, CNTR_MAX);
            }
            return new Pipq<Integer>(threadCount, CNTR_MIN, CNTR_MAX);
        }
    }

    private static final class Worker implements Callable<ThreadCounts> {
        private final Pipq<Integer> queue;
        private final Workload workload;
        private final int tid;
        private final CountDownLatch start;
        private final long durationNanos;
        private final boolean measured;

        Worker(Pipq<Integer> queue, Workload workload, int tid, CountDownLatch start,
               long durationNanos, boolean measured) {
            this.queue = queue;
            this.workload = workload;
            this.tid = tid;
            this.start = start;
            this.durationNanos = durationNanos;
            this.measured = measured;
        }

        @Override
        public ThreadCounts call() throws Exception {
            ThreadCounts counts = new ThreadCounts();
            Random random = new Random(BASE_SEED
                    + workload.ordinal() * 1_000_000L
                    + tid * 7_919L
                    + (measured ? 17L : 31L));

            start.await();
            long endNanos = System.nanoTime() + durationNanos;
            long value = 0L;
            while (System.nanoTime() < endNanos) {
                if (workload.shouldInsert(random)) {
                    queue.insert(random.nextInt(RANDOM_KEY_BOUND), Integer.valueOf((int) value), tid);
                    counts.insertOps++;
                } else {
                    Optional<Node<Integer>> removed = queue.deleteMin(tid);
                    counts.deleteOps++;
                    if (!removed.isPresent()) {
                        counts.emptyDeleteOps++;
                        if (workload == Workload.PREFILL_DELETE_100) {
                            break;
                        }
                    }
                }
                value++;
            }

            return counts;
        }
    }

    private static final class ThreadCounts {
        long insertOps;
        long deleteOps;
        long emptyDeleteOps;

        void add(ThreadCounts other) {
            insertOps += other.insertOps;
            deleteOps += other.deleteOps;
            emptyDeleteOps += other.emptyDeleteOps;
        }

        long totalOps() {
            return insertOps + deleteOps;
        }
    }

    private static final class BenchmarkResult {
        private final Implementation implementation;
        private final Workload workload;
        private final int threadCount;
        private final ThreadCounts counts;
        private final long elapsedNanos;
        private final Pipq<Integer> queue;
        private final StatsSnapshot statsBefore;

        BenchmarkResult(Implementation implementation, Workload workload, int threadCount, ThreadCounts counts,
                        long elapsedNanos, Pipq<Integer> queue, StatsSnapshot statsBefore) {
            this.implementation = implementation;
            this.workload = workload;
            this.threadCount = threadCount;
            this.counts = counts;
            this.elapsedNanos = elapsedNanos;
            this.queue = queue;
            this.statsBefore = statsBefore;
        }

        String toCsvLine() {
            double seconds = elapsedNanos / 1_000_000_000.0;
            double throughput = seconds == 0.0 ? 0.0 : counts.totalOps() / seconds;
            StatsSnapshot stats = StatsSnapshot.capture(queue.stats()).minus(statsBefore);
            int minLeaderCounter = minLeaderCounter(queue);
            boolean postCheck = minLeaderCounter >= 0 && queue.leaderSize() >= 0;

            return "RESULT," + implementation.name()
                    + "," + workload.name()
                    + "," + threadCount
                    + "," + counts.totalOps()
                    + "," + counts.insertOps
                    + "," + counts.deleteOps
                    + "," + counts.emptyDeleteOps
                    + "," + formatDouble(seconds)
                    + "," + formatDouble(throughput)
                    + "," + stats.fastPathInserts
                    + "," + stats.slowerPathInserts
                    + "," + stats.slowestPathInserts
                    + "," + stats.leaderInserts
                    + "," + stats.leaderDeleteMins
                    + "," + stats.leaderDeleteMaxByThread
                    + "," + stats.workerHeapInserts
                    + "," + stats.workerHeapDeleteMinPromotions
                    + "," + stats.helpUpserts
                    + "," + stats.fastPathUpserts
                    + "," + queue.leaderSize()
                    + "," + minLeaderCounter
                    + "," + postCheck;
        }

        private static int minLeaderCounter(Pipq<Integer> queue) {
            int min = Integer.MAX_VALUE;
            for (int tid = 0; tid < queue.threadCount(); tid++) {
                min = Math.min(min, queue.leaderCounter(tid));
            }
            return min == Integer.MAX_VALUE ? 0 : min;
        }

        private static String formatDouble(double value) {
            return String.format(java.util.Locale.US, "%.6f", value);
        }
    }

    private static final class StatsSnapshot {
        final long fastPathInserts;
        final long slowerPathInserts;
        final long slowestPathInserts;
        final long leaderInserts;
        final long leaderDeleteMins;
        final long leaderDeleteMaxByThread;
        final long workerHeapInserts;
        final long workerHeapDeleteMinPromotions;
        final long helpUpserts;
        final long fastPathUpserts;

        StatsSnapshot(long fastPathInserts, long slowerPathInserts, long slowestPathInserts,
                      long leaderInserts, long leaderDeleteMins, long leaderDeleteMaxByThread,
                      long workerHeapInserts, long workerHeapDeleteMinPromotions,
                      long helpUpserts, long fastPathUpserts) {
            this.fastPathInserts = fastPathInserts;
            this.slowerPathInserts = slowerPathInserts;
            this.slowestPathInserts = slowestPathInserts;
            this.leaderInserts = leaderInserts;
            this.leaderDeleteMins = leaderDeleteMins;
            this.leaderDeleteMaxByThread = leaderDeleteMaxByThread;
            this.workerHeapInserts = workerHeapInserts;
            this.workerHeapDeleteMinPromotions = workerHeapDeleteMinPromotions;
            this.helpUpserts = helpUpserts;
            this.fastPathUpserts = fastPathUpserts;
        }

        static StatsSnapshot capture(PipqStats stats) {
            return new StatsSnapshot(
                    stats.fastPathInserts(),
                    stats.slowerPathInserts(),
                    stats.slowestPathInserts(),
                    stats.leaderInserts(),
                    stats.leaderDeleteMins(),
                    stats.leaderDeleteMaxByThread(),
                    stats.workerHeapInserts(),
                    stats.workerHeapDeleteMinPromotions(),
                    stats.helpUpserts(),
                    stats.fastPathUpserts());
        }

        StatsSnapshot minus(StatsSnapshot before) {
            if (before == null) {
                before = new StatsSnapshot(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L);
            }
            return new StatsSnapshot(
                    fastPathInserts - before.fastPathInserts,
                    slowerPathInserts - before.slowerPathInserts,
                    slowestPathInserts - before.slowestPathInserts,
                    leaderInserts - before.leaderInserts,
                    leaderDeleteMins - before.leaderDeleteMins,
                    leaderDeleteMaxByThread - before.leaderDeleteMaxByThread,
                    workerHeapInserts - before.workerHeapInserts,
                    workerHeapDeleteMinPromotions - before.workerHeapDeleteMinPromotions,
                    helpUpserts - before.helpUpserts,
                    fastPathUpserts - before.fastPathUpserts);
        }
    }
}
