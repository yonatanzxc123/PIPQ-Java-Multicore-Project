import java.util.Optional;
import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * Faithful Java baseline of the original PIPQ algorithmic structure.
 *
 * <p>PIPQ has a worker level with one local min-heap per logical thread (own lock, see
 * {@link WorkerHeap}) and a leader level ({@link LeaderLinkedList}) containing the
 * highest-priority candidate elements in a lock-free sorted linked list. This class
 * simplifies away the paper's NUMA leader/coordinator combining mechanism by serializing
 * delete-min with a single lock.</p>
 *
 * <p>This class simplifies away the paper's NUMA leader/coordinator combining mechanism by
 * serializing delete-min with a single lock.</p>
 *
 * <p>This class satisfies {@link LeaderLayer}'s per-tid mutual-exclusion contract: every call
 * into the leader for a given {@code tid} runs while {@code workerHeaps[tid]}'s lock is held, no
 * matter which thread is executing it. That covers {@link #insert(long, Object, int)} (which
 * calls {@code maxByThread} and the fused {@code insertAndDeleteMaxByThread} on the leader), and
 * every path that promotes a worker-heap element up into the leader — {@link #helpUpsert} and
 * {@link #promoteFromWorkerIfCounterBelow} (the latter called from the coordinator, inside
 * {@link #executeAnnouncedDeleteMin}) — both of which acquire {@code workerHeaps[tid]}'s lock
 * before calling {@code insert} on the leader.</p>
 */
public final class Pipq<V> {
    private static final int DEFAULT_WORKER_HEAP_CAPACITY = 16;
    private static final int REQUIRED_LEADER_MINIMUM = 2;

    private final WorkerHeap<V>[] workerHeaps;
    private final LeaderLayer<V> leader;
    private final AtomicIntegerArray leaderCounters;
    private final AnnounceSlot<V>[] announce;
    private final int cntrMin;
    private final int cntrMax;
    private final CasLock coordinatorLock = new CasLock();
    private final PipqStats stats = new PipqStats();

    private PipqLogger logger = new NoopLogger();

    public void setLogger(PipqLogger logger) {
        this.logger = logger;
    }

    /**
     * One slot per thread (the paper's {@code AnnounceStruct}, Alg. 2). A thread announces a
     * pending delete-min by setting {@link #status} to true. The coordinator then fills in
     * {@link #result} and clears {@code status} to signal that the request has been served.
     * {@code status} follows the paper's convention: {@code true} means "still pending", {@code
     * false} means "already served". Each thread reuses its own slot across calls, which is safe
     * because a single thread never has two delete-mins in flight at once.
     */
    private static final class AnnounceSlot<V> {
        volatile boolean status;
        Node<V> result;
    }

    public Pipq(int numberOfThreads, int cntrMin, int cntrMax) {
        this(DEFAULT_WORKER_HEAP_CAPACITY, numberOfThreads, cntrMin, cntrMax);
    }

    @SuppressWarnings("unchecked")
    public Pipq(int initialWorkerHeapCapacity, int numberOfThreads, int cntrMin, int cntrMax) {
        this(initialWorkerHeapCapacity, numberOfThreads, cntrMin, cntrMax,
                createDefaultLeader(numberOfThreads));
    }

    public static <V> Pipq<V> withIndexedHeapLeader(int numberOfThreads, int cntrMin, int cntrMax) {
        return withIndexedHeapLeader(DEFAULT_WORKER_HEAP_CAPACITY, numberOfThreads, cntrMin, cntrMax);
    }

    public static <V> Pipq<V> withIndexedHeapLeader(int initialWorkerHeapCapacity, int numberOfThreads,
                                                     int cntrMin, int cntrMax) {
        return new Pipq<V>(initialWorkerHeapCapacity, numberOfThreads, cntrMin, cntrMax,
                new IndexedHeapLeader<V>(numberOfThreads));
    }

    public static <V> Pipq<V> withCombiningHeapLeader(int numberOfThreads, int cntrMin, int cntrMax) {
        return withCombiningHeapLeader(DEFAULT_WORKER_HEAP_CAPACITY, numberOfThreads, cntrMin, cntrMax);
    }

    public static <V> Pipq<V> withCombiningHeapLeader(int initialWorkerHeapCapacity, int numberOfThreads,
                                                       int cntrMin, int cntrMax) {
        return new Pipq<V>(initialWorkerHeapCapacity, numberOfThreads, cntrMin, cntrMax,
                new CombiningHeapLeader<V>(numberOfThreads));
    }

    @SuppressWarnings("unchecked")
    public Pipq(int initialWorkerHeapCapacity, int numberOfThreads, int cntrMin, int cntrMax,
                LeaderLayer<V> leader) {
        if (numberOfThreads <= 0) {
            throw new IllegalArgumentException("numberOfThreads must be positive");
        }
        if (initialWorkerHeapCapacity <= 0) {
            throw new IllegalArgumentException("initialWorkerHeapCapacity must be positive");
        }
        if (cntrMin < REQUIRED_LEADER_MINIMUM) {
            throw new IllegalArgumentException("CNTR_MIN must be at least 2");
        }
        if (cntrMax < cntrMin) {
            throw new IllegalArgumentException("CNTR_MAX must be greater than or equal to CNTR_MIN");
        }
        if (leader == null) {
            throw new NullPointerException("leader");
        }

        this.workerHeaps = (WorkerHeap<V>[]) new WorkerHeap<?>[numberOfThreads];
        for (int i = 0; i < numberOfThreads; i++) {
            workerHeaps[i] = new WorkerHeap<>(initialWorkerHeapCapacity);
        }
        this.leader = leader;
        this.leaderCounters = new AtomicIntegerArray(numberOfThreads);
        this.announce = (AnnounceSlot<V>[]) new AnnounceSlot<?>[numberOfThreads];
        for (int i = 0; i < numberOfThreads; i++) {
            announce[i] = new AnnounceSlot<>();
        }
        this.cntrMin = cntrMin;
        this.cntrMax = cntrMax;
    }

    private static <V> LeaderLayer<V> createDefaultLeader(int numberOfThreads) {
        if (numberOfThreads <= 0) {
            throw new IllegalArgumentException("numberOfThreads must be positive");
        }
        return new LeaderLinkedList<V>(numberOfThreads);
    }

    /**
     * PIPQ-Insert (Alg. 8). The branches below follow the paper's own order: the outer {@code
     * if} is the leader-vs-fast decision (line 7); nested inside it are the {@code CNTR_MAX}
     * check (line 9) and the fast/slowest split (line 10); the plain fast path (line 22) is the
     * trailing {@code else}.
     *
     * <p>The slowest path calls {@link LeaderLayer#insertAndDeleteMaxByThread}, which performs
     * insert and evict as one fused leader-level operation (the paper's {@code
     * harris_insert_and_move}) rather than as two separate calls ({@code insert} then {@code
     * deleteMaxByThread}). Fusing them matters because a concurrent coordinator delete-min could
     * otherwise drain {@code tid}'s nodes from the leader list in the gap between two separate
     * calls. See {@link LeaderLinkedList#insertAndDeleteMaxByThread} for how the fused operation
     * works, and the "nothing to evict" case handled below.</p>
     */
    public void insert(long key, V value, int tid) {
        validateTid(tid);
        stats.recordTotalInsert();

        Node<V> node = new Node<>(key, value, tid);
        WorkerHeap<V> heap = workerHeaps[tid];

        logger.log("INSERT: start", System.nanoTime(), tid, key, value);

        heap.lock(); // Alg. 8 lines 2-6: acquire this thread's worker-heap lock (the paper calls
                     // this variable lock_val; see CasLock for how the spin-CAS works).
        try {
            Node<V> heapMin = heap.peekMinUnlocked();
            if (heapMin == null || key < heapMin.key()) { // Alg. 8 line 7: key beats this thread's worker-min, so it must go to the leader level.
                if (leaderCounters.get(tid) == cntrMax) { // Alg. 8 line 9: leader list is already full for this thread.
                    Node<V> largest = leader.maxByThread(tid); // t_lead_largest: this thread's current worst node in the leader list.
                    if (largest != null && key >= largest.key()) { // Alg. 8 lines 10-12: new key isn't even better than our worst leader entry, so it's not worth promoting.
                        // Fast path: the new key doesn't earn a spot in the leader list, so it
                        // just goes into this thread's own worker heap.
                        insertIntoWorker(heap, node);
                        logger.log("INSERT: fast path taken", System.nanoTime(), tid);
                        stats.recordFastPathInsert();
                    } else { // Alg. 8 lines 13-17: slowest path (fused insert + evict, harris_insert_and_move).
                        // The leader counter for this thread is deliberately left unchanged here,
                        // matching the paper: neither harris_insert_and_move nor its caller
                        // (hier_insert_local) touches t_lead_counters in this branch. The reasoning
                        // is that one node goes in (this insert) and one node comes out (the evict
                        // below), so the count of this thread's nodes in the leader list is
                        // unchanged overall.
                        //
                        // That reasoning depends on the paper's >=2-per-thread invariant holding
                        // between the insert and the evict. It only holds reliably when
                        // CNTR_MIN/CNTR_MAX are far enough apart that a concurrent deleteMin
                        // cannot plausibly drain this thread's leader-list nodes to zero in that
                        // window. See LeaderLinkedList#insertAndDeleteMaxByThread and the
                        // negative-counter guard in Pipq#executeAnnouncedDeleteMin for the known,
                        // documented limitation when CNTR_MIN/CNTR_MAX are set too close together.
                        stats.recordLeaderInsert();
                        Node<V> movedDown = leader.insertAndDeleteMaxByThread(node, tid);
                        if (movedDown != null) {
                            stats.recordLeaderDeleteMaxByThread();
                            insertIntoWorker(heap, movedDown);
                            stats.recordSlowestPathInsert();
                        } else {
                            // Nothing was evictable: a concurrent delete-min must have drained this
                            // thread's leader-list nodes to zero since we read CNTR_MAX above. The
                            // C++ implementation asserts this can never happen
                            // (`assert(dem_val != EMPTY)`), which would be undefined behavior if it
                            // ever fired, since it would otherwise push garbage into the worker
                            // heap. We instead handle it gracefully: simply don't move anything
                            // down. The leader counter stays untouched, for the same reason as
                            // above. We still record this as a slowest-path insert -
                            // the insert happened, just without a matching eviction - not as a
                            // slower-path completion.
                            stats.recordSlowestPathInsert();
                        }
                    }
                } else { // Alg. 8 lines 18-21: slower path — leader list has room, so just insert into it.
                    insertIntoLeader(node);
                    leaderCounters.incrementAndGet(tid);
                    logger.log("INSERT: slower path taken", System.nanoTime(), tid);
                    stats.recordSlowerPathInsert();
                }
            } else { // Alg. 8 lines 22-24: fast path — key is no better than our own worker-min.
                insertIntoWorker(heap, node);
                logger.log("INSERT: fast path taken", System.nanoTime(), tid);
                stats.recordFastPathInsert();

                // Proactive top-up toward CNTR_MIN, carried over from the C++ implementation's
                // hier_insert_local (not part of Alg. 8's pseudocode). Keeps the leader list from
                // running dry between delete-mins. The unlocked variant is used because
                // the worker heap's lock is already held above.
                if (upsertFromWorkerUnlocked(tid, cntrMin)) {
                    stats.recordFastPathUpsert();
                }
            }
        } finally {
            heap.unlock();
        }

        logger.log("INSERT: end", System.nanoTime(), tid);
    }

    /**
     * PIPQ-DeleteMin (Alg. 9). The calling thread announces its request, then competes to become
     * the coordinator. (The paper splits this into {@code TryCompeteCoordinator} and {@code
     * TryBecomeCoordinator}; here both are collapsed into a single race for one lock, {@link
     * #coordinatorLock}.) Whichever thread wins serves every pending request in one pass via
     * {@link #coordinate()} — this is the "combining" part of the algorithm, where one thread
     * pays the synchronization cost on behalf of many. Threads that lose the race help keep the
     * leader list topped up while they wait, and return as soon as some coordinator has served
     * their own slot.
     */
    public Optional<Node<V>> deleteMin(int tid) {
        validateTid(tid);
        stats.recordTotalDeleteMin();

        logger.log("DELETE MIN: start", System.nanoTime(), tid);

        AnnounceSlot<V> slot = announce[tid];

        logger.log("DELETE MIN: announced", System.nanoTime(), tid);

        slot.status = true; // Alg. 9 line 3: announce the operation.

        while (slot.status) {
            if (coordinatorLock.tryLock()) {
                try {
                    logger.log("DELETE MIN: about to coordinate", System.nanoTime(), tid);
                    coordinate(); // Drains all pending slots, including this thread's own.
                } finally {
                    coordinatorLock.unlock();
                }
                break;
            }

            logger.log("DELETE MIN: lost coordinator race", System.nanoTime(), tid);
            // Lost the race to become coordinator. Rather than block, help keep the leader list
            // topped up (Alg. 9 lines 19/34), then loop back and check whether a coordinator has
            // served this thread's request yet.
            helpUpsert(tid);
        }

        return Optional.ofNullable(slot.result);
    }

    /**
     * Coordinator role (Alg. 9's {@code Coordinate}), simplified to a single pass: drain every
     * pending announce slot in one sweep. This method only ever runs while {@link
     * #coordinatorLock} is held, which means it is the only place calling {@link
     * LeaderLinkedList#deleteMin()} — satisfying that method's requirement of a single caller at
     * a time.
     */
    private void coordinate() {
        for (int idx = 0; idx < announce.length; idx++) {
            AnnounceSlot<V> slot = announce[idx];
            if (slot.status) {
                executeAnnouncedDeleteMin(idx);
                slot.status = false; // Volatile write; publishes the result to the waiting thread.
            }
        }
    }

    /**
     * Execute-Announced-DeleteMin (Alg. 10). Pops the global minimum off the leader list,
     * decrements the leader counter of the thread that originally inserted it, and publishes the
     * removed node into that thread's announce slot. If the counter drops below two, this also
     * pulls an element up from that thread's worker heap to keep the leader list populated.
     */
    private void executeAnnouncedDeleteMin(int idx) {
        stats.recordLeaderDeleteMin();
        Node<V> removed = leader.deleteMin();
        AnnounceSlot<V> slot = announce[idx];

        if (removed == null) {
            slot.result = null; // The queue was empty.
            return;
        }

        int ownerTid = removed.tid();
        int after = leaderCounters.decrementAndGet(ownerTid);
        if (after < 0) {
            throw new IllegalStateException("leader counter for tid " + ownerTid + " went negative");
        }

        slot.result = removed;
        logger.log("COORD.DELETE MIN: removed min", System.nanoTime(), ownerTid, removed.key(), removed.value());

        if (after < REQUIRED_LEADER_MINIMUM) {
            logger.log("COORD.DELETE MIN: counter below min", System.nanoTime(), ownerTid);
            promoteFromWorkerIfCounterBelow(ownerTid, REQUIRED_LEADER_MINIMUM);
        }
    }

    /**
     * Paper-style helping hook: if this thread's leader count is below CNTR_MIN, move its
     * worker-heap minimum up into the leader list.
     *
     * <p>The simplified delete-min path here doesn't use the paper's NUMA-level waiting scheme,
     * but this method is kept as a separate, callable step to stay close to Alg. 11.</p>
     */
    public boolean helpUpsert(int tid) {
        validateTid(tid);
        if (leaderCounters.get(tid) >= cntrMin) {
            return false;
        }

        WorkerHeap<V> heap = workerHeaps[tid];
        if (!heap.tryLock()) {
            // Heap is busy (its owner is mid-insert or mid-delete-min) — don't block, just skip
            // this round of helping.
            return false;
        }
        try {
            if (upsertFromWorkerUnlocked(tid, cntrMin)) {
                logger.log("UPSERT: completed", System.nanoTime(), tid);
                stats.recordHelpUpsert();
                return true;
            }
            logger.log("UPSERT: failed", System.nanoTime(), tid);
            return false;
        } finally {
            heap.unlock();
        }
    }

    public PipqStats stats() {
        return stats;
    }

    public int threadCount() {
        return workerHeaps.length;
    }

    public int cntrMin() {
        return cntrMin;
    }

    public int cntrMax() {
        return cntrMax;
    }

    public int leaderCounter(int tid) {
        validateTid(tid);
        return leaderCounters.get(tid);
    }

    public int leaderSize() {
        return leader.size();
    }

    public int workerSize(int tid) {
        validateTid(tid);
        return workerHeaps[tid].size();
    }

    public Optional<Node<V>> workerMin(int tid) {
        validateTid(tid);
        return Optional.ofNullable(workerHeaps[tid].peekMin());
    }

    /**
     * If {@code tid}'s leader counter is below {@code threshold}, pops {@code tid}'s worker-heap
     * min and pushes it into the leader list. Assumes the caller already holds {@code
     * workerHeaps[tid]}'s lock — this method does not acquire it itself, since {@link CasLock} is
     * not reentrant.
     */
    private boolean upsertFromWorkerUnlocked(int tid, int threshold) {
        if (leaderCounters.get(tid) >= threshold) {
            return false;
        }

        Node<V> promoted = workerHeaps[tid].deleteMinUnlocked();
        if (promoted == null) {
            return false;
        }

        insertIntoLeader(promoted);
        leaderCounters.incrementAndGet(tid);
        logger.log("UPSERT: promoting", System.nanoTime(), tid, promoted.key(), promoted.value());
        return true;
    }

    private boolean promoteFromWorkerIfCounterBelow(int tid, int threshold) {
        WorkerHeap<V> heap = workerHeaps[tid];
        heap.lock();
        try {
            if (upsertFromWorkerUnlocked(tid, threshold)) {
                stats.recordWorkerHeapDeleteMinPromotion();
                return true;
            }
            return false;
        } finally {
            heap.unlock();
        }
    }

    private void insertIntoLeader(Node<V> node) {
        leader.insert(node);
        stats.recordLeaderInsert();
    }

    private void insertIntoWorker(WorkerHeap<V> heap, Node<V> node) {
        heap.insertUnlocked(node);
        stats.recordWorkerHeapInsert();
    }

    private void validateTid(int tid) {
        if (tid < 0 || tid >= workerHeaps.length) {
            throw new IllegalArgumentException("tid must be in [0, " + workerHeaps.length + ")");
        }
    }
}
