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
            Pipq<String> pipq = new Pipq<>(threads, 2, 4);
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
                            pipq.insert(threadId, key, "t" + threadId + "-" + i);
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
