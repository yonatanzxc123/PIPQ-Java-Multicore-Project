import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Faithful Java baseline of the original PIPQ algorithmic structure.
 *
 * <p>PIPQ has a worker level with one local min-heap per logical thread and a
 * leader level containing the highest-priority candidate elements in a sorted
 * linked list. This class intentionally uses Java locks instead of the paper's
 * low-level C++ lock-free linked-list implementation. It also simplifies away
 * the paper's NUMA leader/coordinator combining mechanism by serializing
 * delete-min with a single lock.</p>
 *
 * <p>The proposed heap-based leader-layer variation is not implemented here.</p>
 */
public final class Pipq<V> {
    private static final int DEFAULT_WORKER_HEAP_CAPACITY = 16;
    private static final int REQUIRED_LEADER_MINIMUM = 2;

    private final WorkerHeap<V>[] workerHeaps;
    private final SortedLinkedListLeader<V> leader;
    private final int[] leaderCounters;
    private final int cntrMin;
    private final int cntrMax;
    private final AtomicLong sequence = new AtomicLong();
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
        this.leader = new SortedLinkedListLeader<>();
        this.leaderCounters = new int[numberOfThreads];
        this.cntrMin = cntrMin;
        this.cntrMax = cntrMax;
    }

    public void insert(int tid, long key, V value) {
        validateTid(tid);
        stats.recordTotalInsert();

        Node<V> node = new Node<>(key, value, tid, sequence.getAndIncrement());
        WorkerHeap<V> heap = workerHeaps[tid];

        heap.lock();
        try {
            Node<V> heapMin = heap.peekMinUnlocked();
            if (heapMin != null && key >= heapMin.key()) {
                insertIntoWorker(heap, node);
                stats.recordFastPathInsert();
                return;
            }

            leader.lock();
            try {
                if (leaderCounters[tid] < cntrMax) {
                    insertIntoLeader(node);
                    leaderCounters[tid]++;
                    stats.recordSlowerPathInsert();
                    return;
                }

                Node<V> worstLeaderForTid = leader.maxByThreadUnlocked(tid);
                if (worstLeaderForTid != null && key >= worstLeaderForTid.key()) {
                    insertIntoWorker(heap, node);
                    stats.recordFastPathInsert();
                    return;
                }

                insertIntoLeader(node);
                Node<V> movedDown = leader.deleteMaxByThreadUnlocked(tid);
                if (movedDown == null) {
                    throw new IllegalStateException("leader counter indicated a node for tid " + tid
                            + " but deleteMaxByThread found none");
                }
                stats.recordLeaderDeleteMaxByThread();
                insertIntoWorker(heap, movedDown);
                stats.recordSlowestPathInsert();
            } finally {
                leader.unlock();
            }
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

            leader.lock();
            try {
                stats.recordLeaderDeleteMin();
                removed = leader.deleteMinUnlocked();
                if (removed == null) {
                    return Optional.empty();
                }

                ownerTid = removed.tid();
                if (leaderCounters[ownerTid] <= 0) {
                    throw new IllegalStateException("leader counter for tid " + ownerTid + " is already zero");
                }
                leaderCounters[ownerTid]--;
            } finally {
                leader.unlock();
            }

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

    public boolean validateLeaderSorted() {
        return leader.validateSorted();
    }

    public boolean validateLeaderCounters() {
        lockAllWorkers();
        try {
            leader.lock();
            try {
                return Arrays.equals(leaderCounters, leader.countsByThreadUnlocked(workerHeaps.length));
            } finally {
                leader.unlock();
            }
        } finally {
            unlockAllWorkersReverse();
        }
    }

    public boolean validateInvariant() {
        lockAllWorkers();
        try {
            leader.lock();
            try {
                if (!leader.validateSortedUnlocked()) {
                    return false;
                }
                if (!Arrays.equals(leaderCounters, leader.countsByThreadUnlocked(workerHeaps.length))) {
                    return false;
                }

                List<Node<V>> nodes = leader.snapshotUnlocked();
                for (Node<V> leaderNode : nodes) {
                    Node<V> workerMin = workerHeaps[leaderNode.tid()].peekMinUnlocked();
                    if (workerMin != null && leaderNode.key() > workerMin.key()) {
                        return false;
                    }
                }

                return true;
            } finally {
                leader.unlock();
            }
        } finally {
            unlockAllWorkersReverse();
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
        leader.lock();
        try {
            return leaderCounters[tid];
        } finally {
            leader.unlock();
        }
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

    public List<Node<V>> leaderSnapshot() {
        return leader.snapshot();
    }

    private boolean promoteFromWorkerIfCounterBelow(int tid, int threshold) {
        WorkerHeap<V> heap = workerHeaps[tid];
        heap.lock();
        try {
            leader.lock();
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
                leader.unlock();
            }
        } finally {
            heap.unlock();
        }
    }

    private void insertIntoLeader(Node<V> node) {
        leader.insertUnlocked(node);
        stats.recordLeaderInsert();
    }

    private void insertIntoWorker(WorkerHeap<V> heap, Node<V> node) {
        heap.insertUnlocked(node);
        stats.recordWorkerHeapInsert();
    }

    private void lockAllWorkers() {
        for (WorkerHeap<V> heap : workerHeaps) {
            heap.lock();
        }
    }

    private void unlockAllWorkersReverse() {
        for (int i = workerHeaps.length - 1; i >= 0; i--) {
            workerHeaps[i].unlock();
        }
    }

    private void validateTid(int tid) {
        if (tid < 0 || tid >= workerHeaps.length) {
            throw new IllegalArgumentException("tid must be in [0, " + workerHeaps.length + ")");
        }
    }
}
