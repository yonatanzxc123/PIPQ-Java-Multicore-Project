import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Heap-based implementation of PIPQ's leader layer, synchronized with Flat Combining (Hendler,
 * Incze, Shavit, Tzafrir, SPAA 2010 -- see {@code PIPQ_Project.docx} 2.2).
 *
 * <p>Per {@link LeaderLayer}'s contract, {@code insert}/{@code deleteMaxByThread}/{@code
 * maxByThread} for a given {@code tid} are already externally serialized by the caller (in
 * {@code Pipq}, by holding {@code workerHeaps[tid]}'s lock). The only operation that can touch
 * an arbitrary tid's {@code perThreadMax} concurrently with a same-tid {@code maxByThread} read
 * is {@link #deleteMin()} (exempt from the per-tid rule, single-caller elsewhere), when it
 * happens to evict that tid's node. {@link #maxByThread(int)} is therefore carved out of the
 * combiner entirely: it reads {@code perThreadMax[tid]} directly under {@code
 * maxHeapLocks[tid]}, and the combiner takes that same per-tid lock around any step that mutates
 * that tid's {@code perThreadMax}, always after the combiner-election lock, never before ->
 * strict one-directional nesting, so no deadlock.
 */
public final class CombiningHeapLeader<V> implements LeaderLayer<V> {
    private static final int DEFAULT_CAPACITY = 16;

    private final ThreadMaxHeap<V>[] perThreadMax;
    private final Map<Node<V>, Entry<V>> entries = new IdentityHashMap<Node<V>, Entry<V>>();

    private Entry<V>[] globalMinHeap;
    private int globalSize;
    private long nextSequence;

    // Published copy of globalSize, refreshed by the combiner at the end of each combine() pass.
    // size() reads this directly instead of going through the combiner.
    private volatile int globalSizePublished;

    // ---- Flat Combining machinery. ----

    private enum Op {
        INSERT, DELETE_MIN, DELETE_MAX_BY_THREAD, INSERT_AND_DELETE_MAX_BY_THREAD, VALIDATE
    }

    /**
     * One publication slot per caller. A caller fills in {@link #op}/{@link #argNode}/{@link
     * #argTid}, then sets {@link #pending} true (a volatile write, published last) to announce
     * the request. The combiner reads a pending slot, applies the operation,
     * writes {@link #resultNode}/{@link #resultBool}/{@link #resultException} first, then
     * sets {@link #pending} false last (volatile write) to publish the result back.
     */
    private static final class Publication<V> {
        volatile boolean pending;
        Op op;
        Node<V> argNode;
        int argTid;
        Node<V> resultNode;
        boolean resultBool;
        RuntimeException resultException;
    }

    private final Publication<V>[] announceSlots;
    private final CasLock combineLock = new CasLock();
    private final CasLock[] maxHeapLocks;
    private final ThreadLocal<Integer> slotIndex;
    private final AtomicInteger nextSlotIndex = new AtomicInteger(0);
    private final int threadCount;

    @SuppressWarnings("unchecked")
    public CombiningHeapLeader(int threadCount) {
        if (threadCount <= 0) {
            throw new IllegalArgumentException("threadCount must be positive");
        }
        this.threadCount = threadCount;
        this.globalMinHeap = (Entry<V>[]) new Entry<?>[DEFAULT_CAPACITY];
        this.perThreadMax = (ThreadMaxHeap<V>[]) new ThreadMaxHeap<?>[threadCount];
        for (int i = 0; i < threadCount; i++) {
            perThreadMax[i] = new ThreadMaxHeap<>();
        }
        this.announceSlots = (Publication<V>[]) new Publication<?>[threadCount];
        this.maxHeapLocks = new CasLock[threadCount];
        for (int i = 0; i < threadCount; i++) {
            announceSlots[i] = new Publication<>();
            maxHeapLocks[i] = new CasLock();
        }
        this.slotIndex = ThreadLocal.withInitial(() -> {
            int idx = nextSlotIndex.getAndIncrement();
            if (idx >= CombiningHeapLeader.this.threadCount) {
                throw new IllegalStateException(
                        "more calling threads than threadCount=" + CombiningHeapLeader.this.threadCount);
            }
            return idx;
        });
    }

    @Override
    public void insert(Node<V> node) {
        if (node == null) {
            throw new NullPointerException("node");
        }
        Publication<V> slot = mySlot();
        slot.op = Op.INSERT;
        slot.argNode = node;
        slot.argTid = node.tid();
        submit(slot);
    }

    @Override
    public Node<V> deleteMin() {
        Publication<V> slot = mySlot();
        slot.op = Op.DELETE_MIN;
        submit(slot);
        return slot.resultNode;
    }

    @Override
    public Node<V> deleteMaxByThread(int tid) {
        validateTid(tid);
        Publication<V> slot = mySlot();
        slot.op = Op.DELETE_MAX_BY_THREAD;
        slot.argTid = tid;
        submit(slot);
        return slot.resultNode;
    }

    @Override
    public Node<V> insertAndDeleteMaxByThread(Node<V> node, int tid) {
        validateTid(tid);
        if (node == null) {
            throw new NullPointerException("node");
        }
        if (node.tid() != tid) {
            throw new IllegalArgumentException("node tid does not match requested tid");
        }
        Publication<V> slot = mySlot();
        slot.op = Op.INSERT_AND_DELETE_MAX_BY_THREAD;
        slot.argNode = node;
        slot.argTid = tid;
        submit(slot);
        return slot.resultNode;
    }

    @Override
    public Node<V> maxByThread(int tid) {
        validateTid(tid);
        // Carve-out: bypass the combiner entirely, see class javadoc.
        maxHeapLocks[tid].lock();
        try {
            Entry<V> max = perThreadMax[tid].peek();
            return max == null ? null : max.node;
        } finally {
            maxHeapLocks[tid].unlock();
        }
    }

    public int sizeByThread(int tid) {
        validateTid(tid);
        maxHeapLocks[tid].lock();
        try {
            return perThreadMax[tid].size();
        } finally {
            maxHeapLocks[tid].unlock();
        }
    }

    @Override
    public int size() {
        return globalSizePublished;
    }

    boolean validateInternal() {
        Publication<V> slot = mySlot();
        slot.op = Op.VALIDATE;
        submit(slot);
        return slot.resultBool;
    }

    private Publication<V> mySlot() {
        return announceSlots[slotIndex.get()];
    }

    /**
     * Announces {@code slot} (already filled in by the caller) and blocks until some combiner
     * has served it, either this call itself, by winning election, or a concurrent combiner
     * that swept this slot while serving its own request.
     */
    private void submit(Publication<V> slot) {
        slot.resultException = null;
        slot.pending = true; // volatile write, published last: op/args above are visible once this is seen.
        while (slot.pending) {
            if (combineLock.tryLock()) {
                try {
                    combine();
                } finally {
                    combineLock.unlock();
                }
                // combine() serves every pending slot including this one, so slot.pending is now
                // false; the loop condition re-checks and exits.
            }
            // Lost the election: spin. A combiner is making progress on our behalf.
        }
        RuntimeException failure = slot.resultException;
        if (failure != null) {
            slot.resultException = null;
            throw failure;
        }
    }

    /**
     * Single combining pass: sweep every slot once, apply each pending request to the sequential
     * core, publish its result, and clear it. Only ever runs while {@link #combineLock} is held,
     * so only one thread mutates {@link #globalMinHeap}/{@link #perThreadMax}/{@link #entries} at
     * a time.
     *
     * <p>{@code applyOp}'s {@code RuntimeException}s (e.g. a duplicate-node insert, or an
     * out-of-range tid) are caught per-slot rather than left to propagate.
     */
    private void combine() {
        for (Publication<V> slot : announceSlots) {
            if (!slot.pending) {
                continue;
            }
            try {
                applyOp(slot);
            } catch (RuntimeException e) {
                slot.resultException = e;
            } finally {
                slot.pending = false; // volatile write, published last: results above are now visible.
            }
        }
        globalSizePublished = globalSize; // Refresh the size() counter once per pass (see field javadoc).
    }

    private void applyOp(Publication<V> slot) {
        switch (slot.op) {
            case INSERT: {
                int tid = slot.argTid;
                maxHeapLocks[tid].lock();
                try {
                    insertUnlocked(slot.argNode);
                } finally {
                    maxHeapLocks[tid].unlock();
                }
                return;
            }
            case DELETE_MIN: {
                Entry<V> min = removeGlobalAt(0);
                if (min == null) {
                    slot.resultNode = null;
                    return;
                }
                int tid = min.node.tid();
                maxHeapLocks[tid].lock();
                try {
                    perThreadMax[tid].removeAt(min.perThreadIndex);
                } finally {
                    maxHeapLocks[tid].unlock();
                }
                entries.remove(min.node);
                slot.resultNode = min.node;
                return;
            }
            case DELETE_MAX_BY_THREAD: {
                int tid = slot.argTid;
                Entry<V> max;
                maxHeapLocks[tid].lock();
                try {
                    max = perThreadMax[tid].removeRoot();
                } finally {
                    maxHeapLocks[tid].unlock();
                }
                if (max == null) {
                    slot.resultNode = null;
                    return;
                }
                removeGlobalAt(max.globalIndex);
                entries.remove(max.node);
                slot.resultNode = max.node;
                return;
            }
            case INSERT_AND_DELETE_MAX_BY_THREAD: {
                int tid = slot.argTid;
                Entry<V> max;
                maxHeapLocks[tid].lock();
                try {
                    insertUnlocked(slot.argNode);
                    max = perThreadMax[tid].removeRoot();
                } finally {
                    maxHeapLocks[tid].unlock();
                }
                if (max == null) {
                    slot.resultNode = null;
                    return;
                }
                removeGlobalAt(max.globalIndex);
                entries.remove(max.node);
                slot.resultNode = max.node;
                return;
            }
            case VALIDATE: {
                slot.resultBool = validateUnlocked();
                return;
            }
            default:
                throw new IllegalStateException("unknown op " + slot.op);
        }
    }

    private boolean validateUnlocked() {
        if (entries.size() != globalSize) {
            return false;
        }
        int perThreadTotal = 0;
        for (int i = 0; i < perThreadMax.length; i++) {
            if (!perThreadMax[i].validate(i)) {
                return false;
            }
            perThreadTotal += perThreadMax[i].size();
        }
        if (perThreadTotal != globalSize) {
            return false;
        }
        for (int i = 0; i < globalSize; i++) {
            Entry<V> entry = globalMinHeap[i];
            if (entry == null || entry.globalIndex != i || entries.get(entry.node) != entry) {
                return false;
            }
            int left = leftChild(i);
            int right = left + 1;
            if (left < globalSize && compareMin(globalMinHeap[i], globalMinHeap[left]) > 0) {
                return false;
            }
            if (right < globalSize && compareMin(globalMinHeap[i], globalMinHeap[right]) > 0) {
                return false;
            }
        }
        return true;
    }

    private void insertUnlocked(Node<V> node) {
        validateTid(node.tid());
        if (entries.containsKey(node)) { // literally same object, not just equal key. because entries is IdentityHashMap.
            throw new IllegalArgumentException("node is already present in this leader");
        }

        Entry<V> entry = new Entry<>(node, nextSequence++);
        entries.put(node, entry);
        addGlobal(entry);
        perThreadMax[node.tid()].add(entry);
    }

    private void addGlobal(Entry<V> entry) {
        ensureGlobalCapacity(globalSize + 1);
        int idx = globalSize++;
        while (idx > 0) {
            int parent = parent(idx);
            Entry<V> parentEntry = globalMinHeap[parent];
            if (compareMin(parentEntry, entry) <= 0) {
                break;
            }
            setGlobal(idx, parentEntry);
            idx = parent;
        }
        setGlobal(idx, entry);
    }

    private Entry<V> removeGlobalAt(int idx) {
        if (idx < 0 || idx >= globalSize) {
            return null;
        }

        Entry<V> removed = globalMinHeap[idx];
        Entry<V> last = globalMinHeap[--globalSize];
        globalMinHeap[globalSize] = null;

        if (idx < globalSize) {
            setGlobal(idx, last);
            repairGlobalAt(idx);
        }

        removed.globalIndex = -1;
        return removed;
    }

    private void repairGlobalAt(int idx) {
        if (idx > 0) {
            int parent = parent(idx);
            if (compareMin(globalMinHeap[idx], globalMinHeap[parent]) < 0) {
                siftGlobalUp(idx);
                return;
            }
        }
        siftGlobalDown(idx);
    }

    private void siftGlobalUp(int idx) {
        Entry<V> entry = globalMinHeap[idx];
        while (idx > 0) {
            int parent = parent(idx);
            Entry<V> parentEntry = globalMinHeap[parent];
            if (compareMin(parentEntry, entry) <= 0) {
                break;
            }
            setGlobal(idx, parentEntry);
            idx = parent;
        }
        setGlobal(idx, entry);
    }

    private void siftGlobalDown(int idx) {
        Entry<V> entry = globalMinHeap[idx];
        int half = globalSize / 2;
        while (idx < half) {
            int left = leftChild(idx);
            int right = left + 1;
            int smaller = left;
            if (right < globalSize && compareMin(globalMinHeap[right], globalMinHeap[left]) < 0) {
                smaller = right;
            }
            Entry<V> child = globalMinHeap[smaller];
            if (compareMin(entry, child) <= 0) {
                break;
            }
            setGlobal(idx, child);
            idx = smaller;
        }
        setGlobal(idx, entry);
    }

    private void setGlobal(int idx, Entry<V> entry) {
        globalMinHeap[idx] = entry;
        entry.globalIndex = idx;
    }

    @SuppressWarnings("unchecked")
    private void ensureGlobalCapacity(int needed) {
        if (needed <= globalMinHeap.length) {
            return;
        }
        int newCapacity = Math.max(needed, globalMinHeap.length * 2);
        Entry<V>[] copy = (Entry<V>[]) new Entry<?>[newCapacity];
        System.arraycopy(globalMinHeap, 0, copy, 0, globalSize);
        globalMinHeap = copy;
    }

    private void validateTid(int tid) {
        if (tid < 0 || tid >= perThreadMax.length) {
            throw new IllegalArgumentException("tid must be in [0, " + perThreadMax.length + ")");
        }
    }

    private static int compareMin(Entry<?> left, Entry<?> right) {
        int byKey = Long.compare(left.node.key(), right.node.key());
        if (byKey != 0) {
            return byKey;
        }
        return Long.compare(left.sequence, right.sequence);
    }

    private static int compareMax(Entry<?> left, Entry<?> right) {
        int byKey = Long.compare(right.node.key(), left.node.key());
        if (byKey != 0) {
            return byKey;
        }
        return Long.compare(right.sequence, left.sequence);
    }

    private static int parent(int idx) {
        return (idx - 1) >>> 1;
    }

    private static int leftChild(int idx) {
        return (idx << 1) + 1;
    }

    private static final class Entry<V> {
        final Node<V> node;
        final long sequence; // for deterministic tie-breaking of equal-key nodes, to preserve order of insertion for tests
        int globalIndex = -1;
        int perThreadIndex = -1;

        Entry(Node<V> node, long sequence) {
            this.node = node;
            this.sequence = sequence;
        }
    }

    private static final class ThreadMaxHeap<V> {
        private Entry<V>[] heap;
        private int size;

        @SuppressWarnings("unchecked")
        ThreadMaxHeap() {
            this.heap = (Entry<V>[]) new Entry<?>[DEFAULT_CAPACITY];
        }

        void add(Entry<V> entry) {
            ensureCapacity(size + 1);
            int idx = size++;
            while (idx > 0) {
                int parent = parent(idx);
                Entry<V> parentEntry = heap[parent];
                if (compareMax(parentEntry, entry) <= 0) {
                    break;
                }
                set(idx, parentEntry);
                idx = parent;
            }
            set(idx, entry);
        }

        Entry<V> peek() {
            return size == 0 ? null : heap[0];
        }

        Entry<V> removeRoot() {
            return removeAt(0);
        }

        Entry<V> removeAt(int idx) {
            if (idx < 0 || idx >= size) {
                return null;
            }

            Entry<V> removed = heap[idx];
            Entry<V> last = heap[--size];
            heap[size] = null;

            if (idx < size) {
                set(idx, last);
                repairAt(idx);
            }

            removed.perThreadIndex = -1;
            return removed;
        }

        int size() {
            return size;
        }

        boolean validate(int tid) {
            for (int i = 0; i < size; i++) {
                Entry<V> entry = heap[i];
                if (entry == null || entry.perThreadIndex != i || entry.node.tid() != tid) {
                    return false;
                }
                int left = leftChild(i);
                int right = left + 1;
                if (left < size && compareMax(heap[i], heap[left]) > 0) {
                    return false;
                }
                if (right < size && compareMax(heap[i], heap[right]) > 0) {
                    return false;
                }
            }
            return true;
        }

        private void repairAt(int idx) {
            if (idx > 0) {
                int parent = parent(idx);
                if (compareMax(heap[idx], heap[parent]) < 0) {
                    siftUp(idx);
                    return;
                }
            }
            siftDown(idx);
        }

        private void siftUp(int idx) {
            Entry<V> entry = heap[idx];
            while (idx > 0) {
                int parent = parent(idx);
                Entry<V> parentEntry = heap[parent];
                if (compareMax(parentEntry, entry) <= 0) {
                    break;
                }
                set(idx, parentEntry);
                idx = parent;
            }
            set(idx, entry);
        }

        private void siftDown(int idx) {
            Entry<V> entry = heap[idx];
            int half = size / 2;
            while (idx < half) {
                int left = leftChild(idx);
                int right = left + 1;
                int larger = left;
                if (right < size && compareMax(heap[right], heap[left]) < 0) {
                    larger = right;
                }
                Entry<V> child = heap[larger];
                if (compareMax(entry, child) <= 0) {
                    break;
                }
                set(idx, child);
                idx = larger;
            }
            set(idx, entry);
        }

        private void set(int idx, Entry<V> entry) {
            heap[idx] = entry;
            entry.perThreadIndex = idx;
        }

        @SuppressWarnings("unchecked")
        private void ensureCapacity(int needed) {
            if (needed <= heap.length) {
                return;
            }
            int newCapacity = Math.max(needed, heap.length * 2);
            Entry<V>[] copy = (Entry<V>[]) new Entry<?>[newCapacity];
            System.arraycopy(heap, 0, copy, 0, size);
            heap = copy;
        }
    }
}
