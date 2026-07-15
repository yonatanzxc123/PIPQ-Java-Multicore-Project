import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Concurrency smoke tests for {@link LeaderLinkedList} on its own, before it is wired into a
 * {@link Pipq}. These are structural checks only.
 *
 * <p>Maintaining the paper's >=2-elements-per-thread correctness invariant (Lemma 2/4 — see
 * {@link LeaderLayer}'s per-tid mutual-exclusion contract and {@code deleteMin()}'s javadoc) is
 * {@code Pipq}'s job, done through its leader counters — it isn't something the list itself
 * enforces. So none of the tests here ever run {@code deleteMin()} concurrently with {@code
 * deleteMaxByThread()}: doing so without that invariant in place could let the LOGDEL and
 * MOVING bits race on the same node, set through two different physical fields (the
 * predecessor's {@code next} versus the node's own {@code next}) — exactly the hazard the
 * invariant exists to prevent. See {@code PipqConcurrentSmokeTest} for combined coverage that
 * does respect the invariant, once a {@code Pipq} is managing the counters.</p>
 *
 * <p>Every test here uses one thread per {@code tid}: each thread only ever calls {@code
 * insert}/{@code maxByThread}/{@code deleteMaxByThread} for its own {@code tid}. That alone
 * trivially satisfies {@link LeaderLayer}'s per-tid mutual-exclusion contract, without needing
 * a {@code WorkerHeap} or any separate lock object.</p>
 */
class LeaderLinkedListConcurrentTest {
    @Test
    void concurrentInsertsAcrossThreadsPreserveAllElementsAndOrder() {
        assertTimeoutPreemptively(Duration.ofSeconds(15), () -> {
            int threadCount = 6;
            int insertsPerThread = 600;
            LeaderLinkedList<String> leader = new LeaderLinkedList<>(threadCount);
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            CountDownLatch start = new CountDownLatch(1);
            List<Callable<Void>> tasks = new ArrayList<>();

            for (int tid = 0; tid < threadCount; tid++) {
                int threadId = tid;
                tasks.add(() -> {
                    Random random = new Random(42L + threadId);
                    start.await();
                    for (int i = 0; i < insertsPerThread; i++) {
                        long key = random.nextInt(100_000);
                        leader.insert(new Node<>(key, threadId + "-" + i, threadId));
                    }
                    return null;
                });
            }

            List<Future<Void>> futures = tasks.stream().map(executor::submit).collect(Collectors.toList());
            start.countDown();
            for (Future<Void> f : futures) {
                f.get();
            }
            executor.shutdownNow();

            assertEquals(threadCount * insertsPerThread, leader.size());

            List<Long> drained = new ArrayList<>();
            Node<String> node;
            while ((node = leader.deleteMin()) != null) {
                drained.add(node.key());
            }

            assertEquals(threadCount * insertsPerThread, drained.size());
            for (int i = 1; i < drained.size(); i++) {
                assertTrue(drained.get(i - 1) <= drained.get(i));
            }
        });
    }

    @Test
    void concurrentInsertsAndSingleDrainerDoNotLoseOrDuplicateNodes() {
        assertTimeoutPreemptively(Duration.ofSeconds(15), () -> {
            int threadCount = 5;
            int insertsPerThread = 800;
            LeaderLinkedList<String> leader = new LeaderLinkedList<>(threadCount);
            ExecutorService executor = Executors.newFixedThreadPool(threadCount + 1);
            CountDownLatch start = new CountDownLatch(1);
            AtomicBoolean insertingDone = new AtomicBoolean(false);
            ConcurrentLinkedQueue<Node<String>> drained = new ConcurrentLinkedQueue<>();

            List<Callable<Void>> inserters = new ArrayList<>();
            for (int tid = 0; tid < threadCount; tid++) {
                int threadId = tid;
                inserters.add(() -> {
                    Random random = new Random(7L + threadId);
                    start.await();
                    for (int i = 0; i < insertsPerThread; i++) {
                        long key = random.nextInt(100_000);
                        leader.insert(new Node<>(key, threadId + "-" + i, threadId));
                    }
                    return null;
                });
            }

            // This is the only thread calling deleteMin() in this test -- deleteMin() is only
            // documented as safe when calls to it are serialized across the whole program
            // (see its javadoc).
            Callable<Void> drainer = () -> {
                start.await();
                while (!insertingDone.get()) {
                    Node<String> n = leader.deleteMin();
                    if (n != null) {
                        drained.add(n);
                    }
                }
                return null;
            };

            List<Future<Void>> insertFutures = inserters.stream().map(executor::submit).collect(Collectors.toList());
            Future<Void> drainFuture = executor.submit(drainer);
            start.countDown();

            for (Future<Void> f : insertFutures) {
                f.get();
            }
            insertingDone.set(true);
            drainFuture.get();
            executor.shutdownNow();

            // Drain whatever the concurrent drainer didn't get to. This runs single-threaded,
            // after all inserts have finished, so this tail is expected to come out sorted.
            List<Long> afterKeys = new ArrayList<>();
            Node<String> node;
            while ((node = leader.deleteMin()) != null) {
                afterKeys.add(node.key());
                drained.add(node);
            }
            for (int i = 1; i < afterKeys.size(); i++) {
                assertTrue(afterKeys.get(i - 1) <= afterKeys.get(i));
            }

            Set<String> seenValues = new HashSet<>();
            for (Node<String> n : drained) {
                assertTrue(seenValues.add(n.value()), "duplicate removal of " + n);
            }
            assertEquals(threadCount * insertsPerThread, drained.size());
            assertEquals(0, leader.size());
        });
    }

    @Test
    void concurrentInsertsAndPerThreadDeleteMaxDoNotCorruptStructure() {
        assertTimeoutPreemptively(Duration.ofSeconds(15), () -> {
            int threadCount = 5;
            int opsPerThread = 600;
            LeaderLinkedList<String> leader = new LeaderLinkedList<>(threadCount);
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            CountDownLatch start = new CountDownLatch(1);
            List<ConcurrentLinkedQueue<Node<String>>> removedByTid = new ArrayList<>();
            for (int i = 0; i < threadCount; i++) {
                removedByTid.add(new ConcurrentLinkedQueue<>());
            }

            List<Callable<Void>> tasks = new ArrayList<>();
            for (int tid = 0; tid < threadCount; tid++) {
                int threadId = tid;
                tasks.add(() -> {
                    Random random = new Random(99L + threadId);
                    start.await();
                    for (int i = 0; i < opsPerThread; i++) {
                        long key = random.nextInt(100_000);
                        leader.insert(new Node<>(key, threadId + "-" + i, threadId));
                        if (i % 3 == 0) {
                            Node<String> removed = leader.deleteMaxByThread(threadId);
                            if (removed != null) {
                                removedByTid.get(threadId).add(removed);
                            }
                        }
                    }
                    return null;
                });
            }

            List<Future<Void>> futures = tasks.stream().map(executor::submit).collect(Collectors.toList());
            start.countDown();
            for (Future<Void> f : futures) {
                f.get();
            }
            executor.shutdownNow();

            int totalRemovedDuring = removedByTid.stream().mapToInt(ConcurrentLinkedQueue::size).sum();
            int totalInserted = threadCount * opsPerThread;
            assertEquals(totalInserted - totalRemovedDuring, leader.size());

            Set<String> seenValues = new HashSet<>();
            for (ConcurrentLinkedQueue<Node<String>> queue : removedByTid) {
                for (Node<String> n : queue) {
                    assertTrue(seenValues.add(n.value()), "duplicate removal of " + n);
                }
            }

            // Final drain, single-threaded. Safe to compare against the deleteMaxByThread
            // history collected above, since no deleteMaxByThread calls are still running.
            List<Long> drained = new ArrayList<>();
            Node<String> node;
            while ((node = leader.deleteMin()) != null) {
                assertTrue(seenValues.add(node.value()), "node both deleteMaxByThread'd and deleteMin'd: " + node);
                drained.add(node.key());
            }
            assertEquals(totalInserted - totalRemovedDuring, drained.size());
            for (int i = 1; i < drained.size(); i++) {
                assertTrue(drained.get(i - 1) <= drained.get(i));
            }
        });
    }
}
