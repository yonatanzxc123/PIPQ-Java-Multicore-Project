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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Concurrency stress tests for {@link CombiningHeapLeader}, mirroring the structure of {@code
 * LeaderLinkedListConcurrentTest} (one thread per {@code tid}, satisfying {@link LeaderLayer}'s
 * per-tid mutual-exclusion contract by construction). These are the tests that actually exercise
 * the Flat Combining machinery: real combiner-election races, real spin-waiting on publication
 * slots, and the {@code maxHeapLocks[tid]} carve-out racing against the combiner's own use of the
 * same lock. Correctness here is checked structurally ({@link CombiningHeapLeader#validateInternal()})
 * plus no lost/duplicated nodes and globally sorted {@code deleteMin} output -- the same
 * properties the list-leader concurrent tests check.
 *
 * <p>{@code CombiningHeapLeader} assigns one publication slot per {@code tid}, plus two fixed
 * slots reserved for {@code deleteMin}/{@code validateInternal} (see the class javadoc there) --
 * slots are no longer tied to physical calling threads, so tests are free to reuse pool threads
 * for follow-up calls like {@code validateInternal()} or a final drain without any thread-count
 * bookkeeping. {@code size()} is exempt entirely -- it reads a plain published counter, not a
 * publication slot -- so it's called directly.</p>
 */
class CombiningHeapLeaderConcurrentTest {
    /**
     * Reproduces a poison-pill livelock: if {@code applyOp} throws while the combiner is
     * processing one caller's slot, that slot's {@code pending} flag is never cleared, so every
     * future {@code combine()} pass re-throws on that same slot before ever reaching slots at a
     * higher array index -- silently starving every other caller forever, not just the one whose
     * operation was actually invalid.
     *
     * <p>This needs two distinct calling threads (publication slots are assigned per calling
     * thread, lowest index first) but no actual race: thread P is run to completion (joined)
     * before thread Q ever touches the leader, so the reproduction is fully deterministic --
     * P's poisoned slot (index 0) is already stuck before Q (index 1) makes its first, entirely
     * valid, call.</p>
     */
    @Test
    void combinerExceptionOnOneSlotDoesNotBlockOtherSlots() throws Exception {
        CombiningHeapLeader<String> leader = new CombiningHeapLeader<>(2);
        Node<String> duplicate = new Node<>(1, "p1", 0);

        ExecutorService pExecutor = Executors.newFixedThreadPool(1);
        pExecutor.submit((Callable<Void>) () -> {
            leader.insert(duplicate); // slot 0, first call: succeeds.
            assertThrows(IllegalArgumentException.class, () -> leader.insert(duplicate)); // slot 0 again: legitimately invalid (duplicate node identity).
            return null;
        }).get();
        pExecutor.shutdown();

        ExecutorService qExecutor = Executors.newFixedThreadPool(1);
        try {
            // A completely separate, valid operation from a different calling thread (slot 1).
            // Must succeed -- it has nothing to do with P's earlier, already-resolved failure.
            qExecutor.submit((Callable<Void>) () -> {
                leader.insert(new Node<>(2, "q1", 1));
                return null;
            }).get();
        } finally {
            qExecutor.shutdown();
        }

        assertEquals(2, leader.size());
    }

    @Test
    void concurrentInsertsAcrossThreadsPreserveAllElementsAndOrder() {
        assertTimeoutPreemptively(Duration.ofSeconds(15), () -> {
            int threadCount = 6;
            int insertsPerThread = 600;
            CombiningHeapLeader<String> leader = new CombiningHeapLeader<>(threadCount);
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

            assertEquals(threadCount * insertsPerThread, leader.size());

            // validateInternal() and the drain below reuse one of the pool's threads (already
            // holding a publication slot) rather than running on this method's own thread -- see
            // class javadoc.
            boolean valid = executor.submit(leader::validateInternal).get();
            List<Long> drained = executor.submit(() -> {
                List<Long> keys = new ArrayList<Long>();
                Node<String> node;
                while ((node = leader.deleteMin()) != null) {
                    keys.add(node.key());
                }
                return keys;
            }).get();
            executor.shutdownNow();

            assertTrue(valid);
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
            // threadCount inserter threads + 1 dedicated drainer thread = threadCount + 1
            // distinct callers.
            CombiningHeapLeader<String> leader = new CombiningHeapLeader<>(threadCount + 1);
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

            // This is the only thread calling deleteMin() while inserts are in flight.
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

            // Drain whatever the concurrent drainer didn't get to. Runs on a reused pool thread
            // (see class javadoc) so it doesn't count as a new caller beyond threadCount + 1.
            List<Long> afterKeys = executor.submit(() -> {
                List<Long> keys = new ArrayList<Long>();
                Node<String> node;
                while ((node = leader.deleteMin()) != null) {
                    keys.add(node.key());
                    drained.add(node);
                }
                return keys;
            }).get();
            executor.shutdownNow();

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
            CombiningHeapLeader<String> leader = new CombiningHeapLeader<>(threadCount);
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
                        // Interleave maxByThread reads (the per-tid carve-out) with the mutating
                        // ops so the test actually exercises maxHeapLocks[tid] contention against
                        // the combiner, not just the combiner path alone.
                        leader.maxByThread(threadId);
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

            int totalRemovedDuring = removedByTid.stream().mapToInt(ConcurrentLinkedQueue::size).sum();
            int totalInserted = threadCount * opsPerThread;
            assertEquals(totalInserted - totalRemovedDuring, leader.size());

            boolean valid = executor.submit(leader::validateInternal).get();

            Set<String> seenValues = new HashSet<>();
            for (ConcurrentLinkedQueue<Node<String>> queue : removedByTid) {
                for (Node<String> n : queue) {
                    assertTrue(seenValues.add(n.value()), "duplicate removal of " + n);
                }
            }

            // Final drain, single-threaded (via a reused pool thread -- see class javadoc). Safe
            // to compare against the deleteMaxByThread history collected above, since no
            // deleteMaxByThread calls are still running.
            List<Long> drained = executor.submit(() -> {
                List<Long> keys = new ArrayList<Long>();
                Node<String> node;
                while ((node = leader.deleteMin()) != null) {
                    assertTrue(seenValues.add(node.value()), "node both deleteMaxByThread'd and deleteMin'd: " + node);
                    keys.add(node.key());
                }
                return keys;
            }).get();
            executor.shutdownNow();

            assertTrue(valid);
            assertEquals(totalInserted - totalRemovedDuring, drained.size());
            for (int i = 1; i < drained.size(); i++) {
                assertTrue(drained.get(i - 1) <= drained.get(i));
            }
        });
    }
}
