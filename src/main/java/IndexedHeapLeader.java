import java.util.IdentityHashMap;
import java.util.Map;

/**
 * Heap-based implementation of PIPQ's leader layer.
 *
 * <p>The original PIPQ leader is a sorted linked list. This proposed variation keeps the
 * same abstract operations but stores leader nodes in a global indexed min-heap and in one
 * auxiliary max-heap per tid. The global heap serves {@link #deleteMin()}, while the per-thread
 * heaps serve {@link #deleteMaxByThread(int)}. Each internal entry stores both heap indexes, so
 * removing a node chosen by one heap from the other heap is efficient.</p>
 *
 * <p>This class intentionally uses one internal lock. It is not a lock-free heap and does not
 * attempt to reproduce the linked-list pointer-marking algorithm.</p>
 */
public final class IndexedHeapLeader<V> implements LeaderLayer<V> {
    private static final int DEFAULT_CAPACITY = 16;

    private final CasLock lock = new CasLock();
    private final ThreadMaxHeap<V>[] perThreadMax;
    private final Map<Node<V>, Entry<V>> entries = new IdentityHashMap<Node<V>, Entry<V>>();

    private Entry<V>[] globalMinHeap;
    private int globalSize;
    private long nextSequence;

    @SuppressWarnings("unchecked")
    public IndexedHeapLeader(int threadCount) {
        if (threadCount <= 0) {
            throw new IllegalArgumentException("threadCount must be positive");
        }
        this.globalMinHeap = (Entry<V>[]) new Entry<?>[DEFAULT_CAPACITY];
        this.perThreadMax = (ThreadMaxHeap<V>[]) new ThreadMaxHeap<?>[threadCount];
        for (int i = 0; i < threadCount; i++) {
            perThreadMax[i] = new ThreadMaxHeap<V>();
        }
    }

    @Override
    public void insert(Node<V> node) {
        lock.lock();
        try {
            insertUnlocked(node);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Node<V> deleteMin() {
        lock.lock();
        try {
            Entry<V> min = removeGlobalAt(0);
            if (min == null) {
                return null;
            }
            perThreadMax[min.node.tid()].removeAt(min.perThreadIndex);
            entries.remove(min.node);
            return min.node;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Node<V> deleteMaxByThread(int tid) {
        validateTid(tid);
        lock.lock();
        try {
            Entry<V> max = perThreadMax[tid].removeRoot();
            if (max == null) {
                return null;
            }
            removeGlobalAt(max.globalIndex);
            entries.remove(max.node);
            return max.node;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Node<V> insertAndDeleteMaxByThread(Node<V> node, int tid) {
        validateTid(tid);
        if (node.tid() != tid) {
            throw new IllegalArgumentException("node tid does not match requested tid");
        }

        lock.lock();
        try {
            insertUnlocked(node);
            Entry<V> max = perThreadMax[tid].removeRoot();
            if (max == null) {
                return null;
            }
            removeGlobalAt(max.globalIndex);
            entries.remove(max.node);
            return max.node;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Node<V> maxByThread(int tid) {
        validateTid(tid);
        lock.lock();
        try {
            Entry<V> max = perThreadMax[tid].peek();
            return max == null ? null : max.node;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int size() {
        lock.lock();
        try {
            return globalSize;
        } finally {
            lock.unlock();
        }
    }

    public int sizeByThread(int tid) {
        validateTid(tid);
        lock.lock();
        try {
            return perThreadMax[tid].size();
        } finally {
            lock.unlock();
        }
    }

    boolean validateInternal() {
        lock.lock();
        try {
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
        } finally {
            lock.unlock();
        }
    }

    private void insertUnlocked(Node<V> node) {
        if (node == null) {
            throw new NullPointerException("node");
        }
        validateTid(node.tid());
        if (entries.containsKey(node)) {
            throw new IllegalArgumentException("node is already present in this leader");
        }

        Entry<V> entry = new Entry<V>(node, nextSequence++);
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
        final long sequence;
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
