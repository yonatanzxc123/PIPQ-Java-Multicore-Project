import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;

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
    private final int[] leaderCounters;
    private final int cntrMin;
    private final int cntrMax;
    private final ReentrantLock deleteMinLock = new ReentrantLock();
    private final PipqStats stats = new PipqStats();

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
        this.leaderCounters = new int[numberOfThreads];
        this.cntrMin = cntrMin;
        this.cntrMax = cntrMax;
    }

    public void insert(int tid, long key, V value) {
        validateTid(tid);
        stats.recordTotalInsert();

        Node<V> node = new Node<>(key, value, tid);
        WorkerHeap<V> heap = workerHeaps[tid];

        heap.lock();
        try {
            Node<V> heapMin = heap.peekMinUnlocked();
            if (heapMin != null && key >= heapMin.key()) {
                insertIntoWorker(heap, node);
                stats.recordFastPathInsert();
                return;
            }

            if (leaderCounters[tid] < cntrMax) {
                insertIntoLeader(node);
                leaderCounters[tid]++;
                stats.recordSlowerPathInsert();
                return;
            }

            Node<V> worstLeaderForTid = leader.maxByThread(tid);
            if (worstLeaderForTid != null && key >= worstLeaderForTid.key()) {
                insertIntoWorker(heap, node);
                stats.recordFastPathInsert();
                return;
            }

            insertIntoLeader(node);
            Node<V> movedDown = leader.deleteMaxByThread(tid);
            if (movedDown == null) {
                throw new IllegalStateException("leader counter indicated a node for tid " + tid
                        + " but deleteMaxByThread found none");
            }
            stats.recordLeaderDeleteMaxByThread();
            insertIntoWorker(heap, movedDown);
            stats.recordSlowestPathInsert();
        } finally {
            heap.unlock();
        }
    }

    public void insert(long key, V value, int tid) {
        insert(tid, key, value);
    }

    public Optional<Node<V>> deleteMin(int tid) {
        validateTid(tid);
        stats.recordTotalDeleteMin();

        deleteMinLock.lock();
        try {
            Node<V> removed;
            int ownerTid;

            stats.recordLeaderDeleteMin();
            removed = leader.deleteMin();
            if (removed == null) {
                return Optional.empty();
            }

            ownerTid = removed.tid();
            if (leaderCounters[ownerTid] <= 0) {
                throw new IllegalStateException("leader counter for tid " + ownerTid + " is already zero");
            }
            leaderCounters[ownerTid]--;

            promoteFromWorkerIfCounterBelow(ownerTid, REQUIRED_LEADER_MINIMUM);
            return Optional.of(removed);
        } finally {
            deleteMinLock.unlock();
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
        return promoteFromWorkerIfCounterBelow(tid, cntrMin);
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
        return leaderCounters[tid];
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

    private boolean promoteFromWorkerIfCounterBelow(int tid, int threshold) {
        WorkerHeap<V> heap = workerHeaps[tid];
        heap.lock();
        try {
            if (leaderCounters[tid] >= threshold) {
                return false;
            }

            Node<V> promoted = heap.deleteMinUnlocked();
            if (promoted == null) {
                return false;
            }

            insertIntoLeader(promoted);
            leaderCounters[tid]++;
            stats.recordWorkerHeapDeleteMinPromotion();
            return true;
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
