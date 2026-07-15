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
 * <p>The proposed heap-based leader-layer variation is not implemented here.</p>
 *
 * <p>Satisfies {@link LeaderLayer}'s per-tid mutual-exclusion contract: every call into the
 * leader for a given {@code tid} — {@code insert}/{@code maxByThread}/{@code
 * deleteMaxByThread} in {@link #insert(int, long, Object)}, and the coordinator-side promotion
 * in {@link #promoteFromWorkerIfCounterBelow} — runs with {@code workerHeaps[tid]}'s lock
 * held, regardless of which thread is executing it.</p>
 */
public final class Pipq<V> {
    private static final int DEFAULT_WORKER_HEAP_CAPACITY = 16;
    private static final int REQUIRED_LEADER_MINIMUM = 2;

    private final WorkerHeap<V>[] workerHeaps;
    private final LeaderLinkedList<V> leader;
    private final AtomicIntegerArray leaderCounters;
    private final AnnounceSlot<V>[] announce;
    private final int cntrMin;
    private final int cntrMax;
    private final CasLock coordinatorLock = new CasLock();
    private final PipqStats stats = new PipqStats();

    /**
     * One per thread (paper's {@code AnnounceStruct}, Algorithm 2). A thread announces a pending
     * delete-min by setting {@link #status} true; the coordinator publishes {@link #result} and
     * then clears {@code status} to signal completion. {@code status} keeps the paper's polarity
     * ({@code true} == pending, {@code false} == served). Reused across calls — a thread's
     * delete-mins never overlap.
     *
     * <p>Visibility: the coordinator writes {@code result} (plain) before {@code status = false}
     * (volatile); the announcer reads {@code status} (volatile) before {@code result}. The
     * coordinator always writes {@code result} in both branches of
     * {@link Pipq#executeAnnouncedDeleteMin}, so the announcer never observes a stale value.</p>
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

        this.workerHeaps = (WorkerHeap<V>[]) new WorkerHeap<?>[numberOfThreads];
        for (int i = 0; i < numberOfThreads; i++) {
            workerHeaps[i] = new WorkerHeap<>(initialWorkerHeapCapacity);
        }
        this.leader = new LeaderLinkedList<>(numberOfThreads);
        this.leaderCounters = new AtomicIntegerArray(numberOfThreads);
        this.announce = (AnnounceSlot<V>[]) new AnnounceSlot<?>[numberOfThreads];
        for (int i = 0; i < numberOfThreads; i++) {
            announce[i] = new AnnounceSlot<>();
        }
        this.cntrMin = cntrMin;
        this.cntrMax = cntrMax;
    }

    /**
     * PIPQ-Insert (Algorithm 8), in the paper's literal branch order: the leader-vs-fast decision
     * (line 7) is the outer {@code if}, with the {@code CNTR_MAX} check (line 9) and the
     * fast/slowest split (line 10) nested inside it, and the plain fast path (line 22) as the
     * trailing {@code else}.
     *
     * <p>The trailing fast path also carries the C++ implementation's proactive top-up toward
     * {@code CNTR_MIN} (from {@code hier_insert_local}; not present in the Algorithm 8 pseudocode
     * itself) — after a plain worker insert, if this thread's leader count is below {@code
     * CNTR_MIN}, its worker-min is popped and upserted into {@code L}. This keeps leader refill
     * from being solely delete-min's job.</p>
     */
    public void insert(int tid, long key, V value) {
        validateTid(tid);
        stats.recordTotalInsert();

        Node<V> node = new Node<>(key, value, tid);
        WorkerHeap<V> heap = workerHeaps[tid];

        heap.lock(); // Alg 8 lines 2-6: acquire lock_val (CasLock spins the CAS internally).
        try {
            Node<V> heapMin = heap.peekMinUnlocked();
            if (heapMin == null || key < heapMin.key()) { // Alg 8 line 7, must compare with leader level
                if (leaderCounters.get(tid) == cntrMax) { // Alg 8 line 9.
                    Node<V> largest = leader.maxByThread(tid); // t_lead_largest
                    if (largest != null && key >= largest.key()) { // Alg 8 lines 10-12: fast.
                        insertIntoWorker(heap, node);
                        stats.recordFastPathInsert();
                    } else { // Alg 8 lines 13-17: slowest.
                        insertIntoLeader(node);
                        Node<V> movedDown = leader.deleteMaxByThread(tid);
                        if (movedDown == null) {
                            throw new IllegalStateException("leader counter indicated a node for tid "
                                    + tid + " but deleteMaxByThread found none");
                        }
                        stats.recordLeaderDeleteMaxByThread();
                        insertIntoWorker(heap, movedDown);
                        stats.recordSlowestPathInsert();
                    }
                } else { // Alg 8 lines 18-21: slower.
                    insertIntoLeader(node);
                    leaderCounters.incrementAndGet(tid);
                    stats.recordSlowerPathInsert();
                }
            } else { // Alg 8 lines 22-24: fast.
                insertIntoWorker(heap, node);
                stats.recordFastPathInsert();

                // hier_insert_local top-up (C++ impl, not in Alg 8): keep L filled toward
                // CNTR_MIN. Unlocked variant used because CasLock isn't reentrant and heap's
                // lock is already held.
                if (upsertFromWorkerUnlocked(tid, cntrMin)) {
                    stats.recordFastPathUpsert();
                }
            }
        } finally {
            heap.unlock(); // Alg 8 line 26.
        }
    }

    public void insert(long key, V value, int tid) {
        insert(tid, key, value);
    }

    /**
     * PIPQ-DeleteMin (Algorithm 9). The thread announces its request, then competes to become the
     * coordinator (paper's {@code TryCompeteCoordinator} + {@code TryBecomeCoordinator}, collapsed
     * to a single zone: one {@link #coordinatorLock}). The winning coordinator serves every pending
     * request via {@link #coordinate()} (combining); losers help maintain {@code L} while waiting,
     * and return as soon as some coordinator has completed their own slot.
     */
    public Optional<Node<V>> deleteMin(int tid) {
        validateTid(tid);
        stats.recordTotalDeleteMin();

        AnnounceSlot<V> slot = announce[tid];
        slot.status = true; // Algorithm 9 line 3: announce the operation.

        while (slot.status) {
            if (coordinatorLock.tryLock()) {
                try {
                    coordinate(); // Drains all pending slots, including ours.
                } finally {
                    coordinatorLock.unlock();
                }
                break;
            }

            // Lost the race for the coordinator lock — help maintain L instead of blocking
            // (Algorithm 9 lines 19/34), then re-check whether a coordinator has served us.
            helpUpsert(tid);
        }

        return Optional.ofNullable(slot.result);
    }

    /**
     * Coordinator role (Algorithm 9 {@code Coordinate}), single-zone: drain every pending announce
     * slot in one pass. Runs with {@link #coordinatorLock} held, so it is the only thread calling
     * {@link LeaderLinkedList#deleteMin()} — satisfying that method's single-caller contract.
     */
    private void coordinate() {
        for (int idx = 0; idx < announce.length; idx++) {
            AnnounceSlot<V> slot = announce[idx];
            if (slot.status) {
                executeAnnouncedDeleteMin(idx);
                slot.status = false; // Volatile write publishes result to the waiting thread.
            }
        }
    }

    /**
     * Execute-Announced-DeleteMin (Algorithm 10). Pops the global minimum, decrements the removed
     * element's owning thread's leader counter, publishes the result into that thread's announce
     * slot, and — if the counter dropped below two — pulls an element up from the owner's worker
     * heap to keep {@code L} populated.
     */
    private void executeAnnouncedDeleteMin(int idx) {
        stats.recordLeaderDeleteMin();
        Node<V> removed = leader.deleteMin();
        AnnounceSlot<V> slot = announce[idx];

        if (removed == null) {
            slot.result = null; // EMPTY — the queue was empty.
            return;
        }

        int ownerTid = removed.tid();
        int after = leaderCounters.decrementAndGet(ownerTid); // add_and_fetch(cntr, -1)
        if (after < 0) {
            throw new IllegalStateException("leader counter for tid " + ownerTid + " went negative");
        }

        slot.result = removed;

        if (after < REQUIRED_LEADER_MINIMUM) {
            promoteFromWorkerIfCounterBelow(ownerTid, REQUIRED_LEADER_MINIMUM);
        }
    }

    /**
     * Paper-style helping hook: if this thread's leader count is below CNTR_MIN,
     * move the minimum local worker element up to the leader list.
     *
     * <p>The simplified delete-min path does not use NUMA waiting, but exposing
     * this method keeps the baseline close to Algorithm 11.</p>
     */
    public boolean helpUpsert(int tid) {
        validateTid(tid);
        if (leaderCounters.get(tid) >= cntrMin) {
            return false;
        }

        WorkerHeap<V> heap = workerHeaps[tid];
        if (!heap.tryLock()) {
            // Heap busy (owner mid-insert/delete-min) — don't block, just skip this round.
            return false;
        }
        try {
            if (upsertFromWorkerUnlocked(tid, cntrMin)) {
                stats.recordHelpUpsert();
                return true;
            }
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
     * Pops {@code tid}'s worker-min and upserts it into the leader list, if {@code tid}'s leader
     * counter is below {@code threshold}. Assumes {@code workerHeaps[tid]}'s lock is already held
     * by the caller — does not acquire it itself, since {@link CasLock} is not reentrant.
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
