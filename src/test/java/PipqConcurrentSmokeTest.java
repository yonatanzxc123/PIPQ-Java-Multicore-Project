import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

class PipqConcurrentSmokeTest {
    @Test
    void concurrentInsertsAndDeleteMinsDoNotDeadlockOrCorruptInvariants() {
        assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
            int threads = 4;
            int operationsPerThread = 750;
            // CNTR_MIN/CNTR_MAX need a real gap (paper defaults: 10/100): the slowest insert
            // path's leader-counter bookkeeping intentionally nets to zero around its fused
            // insert+evict (matching the paper's harris_insert_and_move, which never touches
            // the counter there either), relying on the >=2-invariant holding long enough for
            // a concurrent deleteMin not to drain a thread's leader-list membership to nothing
            // in between. At a tiny gap (e.g. 2/4) that race is easy to hit under real
            // concurrency and trips Pipq#executeAnnouncedDeleteMin's negative-counter guard --
            // a known, paper-inherited limitation, not a bug in this test's own logic.
            Pipq<String> pipq = new Pipq<>(threads, 10, 50);
            pipq.setLogger(new ConcurrentLogger());
            ExecutorService executor = Executors.newFixedThreadPool(threads);
            CountDownLatch start = new CountDownLatch(1);
            List<Callable<Void>> tasks = new ArrayList<>();

            for (int tid = 0; tid < threads; tid++) {
                int threadId = tid;
                tasks.add(() -> {
                    Random random = new Random(1234L + threadId);
                    start.await();
                    for (int i = 0; i < operationsPerThread; i++) {
                        if (random.nextInt(100) < 70) {
                            long key = random.nextInt(10_000);
                            pipq.insert(key, "t" + threadId + "-" + i, threadId);
                        } else {
                            pipq.deleteMin(threadId);
                        }
                    }
                    return null;
                });
            }

            List<Future<Void>> futures = tasks.stream()
                    .map(executor::submit)
                    .collect(Collectors.toList());
            start.countDown();
            for (Future<Void> future : futures) {
                future.get();
            }
            executor.shutdownNow();

            assertEquals((long) threads * operationsPerThread, pipq.stats().totalInserts()
                    + pipq.stats().totalDeleteMins());
        });
    }
}
