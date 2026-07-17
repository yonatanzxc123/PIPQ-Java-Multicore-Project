import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

class PipqHeapLeaderTest {
    @Test
    void heapLeaderPipqReturnsGlobalPriorityOrderSequentially() {
        Pipq<String> pipq = Pipq.withIndexedHeapLeader(2, 2, 3);
        pipq.insert(10, "a10", 0);
        pipq.insert(20, "b20", 1);
        pipq.insert(30, "a30", 0);
        pipq.insert(40, "b40", 1);
        pipq.insert(50, "a50", 0);
        pipq.insert(60, "b60", 1);
        pipq.insert(70, "a70", 0);
        pipq.insert(80, "b80", 1);

        List<Long> keys = new ArrayList<Long>();
        Optional<Node<String>> node;
        while ((node = pipq.deleteMin(0)).isPresent()) {
            keys.add(node.get().key());
        }

        assertEquals(Arrays.asList(10L, 20L, 30L, 40L, 50L, 60L, 70L, 80L), keys);
    }

    @Test
    void ogAndHeapPipqAgreeOnControlledSequentialCase() {
        Pipq<String> og = new Pipq<String>(2, 2, 3);
        Pipq<String> heap = Pipq.withIndexedHeapLeader(2, 2, 3);
        long[] keys = {30, 10, 40, 20, 70, 50, 80, 60};

        for (int i = 0; i < keys.length; i++) {
            int tid = i % 2;
            String value = "v" + i;
            og.insert(keys[i], value, tid);
            heap.insert(keys[i], value, tid);
        }

        List<Long> ogKeys = drainKeys(og);
        List<Long> heapKeys = drainKeys(heap);
        assertEquals(ogKeys, heapKeys);
        assertEquals(Arrays.asList(10L, 20L, 30L, 40L, 50L, 60L, 70L, 80L), heapKeys);
    }

    @Test
    void heapLeaderPipqConcurrentSmokeDoesNotDeadlockOrThrow() {
        assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
            int threads = 4;
            int operationsPerThread = 500;
            Pipq<String> pipq = Pipq.withIndexedHeapLeader(threads, 10, 50);
            ExecutorService executor = Executors.newFixedThreadPool(threads);
            CountDownLatch start = new CountDownLatch(1);
            List<Callable<Void>> tasks = new ArrayList<Callable<Void>>();

            for (int tid = 0; tid < threads; tid++) {
                final int threadId = tid;
                tasks.add(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        Random random = new Random(5678L + threadId);
                        start.await();
                        for (int i = 0; i < operationsPerThread; i++) {
                            if (random.nextInt(100) < 70) {
                                pipq.insert(random.nextInt(10_000), "t" + threadId + "-" + i, threadId);
                            } else {
                                pipq.deleteMin(threadId);
                            }
                        }
                        return null;
                    }
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

    private static List<Long> drainKeys(Pipq<String> pipq) {
        List<Long> keys = new ArrayList<Long>();
        Optional<Node<String>> node;
        while ((node = pipq.deleteMin(0)).isPresent()) {
            keys.add(node.get().key());
        }
        return keys;
    }
}
