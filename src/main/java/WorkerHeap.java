import java.util.Arrays;
import java.util.Objects;

/**
 * Custom array-backed binary min-heap used for the PIPQ worker level.
 *
 * <p>Each logical thread owns one heap. The owning thread performs most inserts locally
 * without touching any shared state. Occasionally, when a delete-min needs to refill the
 * leader list, another thread ("coordinator") briefly accesses this heap to move its minimum element up into
 * the leader layer - that's why the heap has its own lock rather than assuming single-thread
 * access.</p>
 */
public final class WorkerHeap<V> {
    private static final int DEFAULT_CAPACITY = 16;

    private final CasLock lock = new CasLock();
    private Node<V>[] heap;
    private int size;

    public WorkerHeap() {
        this(DEFAULT_CAPACITY);
    }

    @SuppressWarnings("unchecked")
    public WorkerHeap(int initialCapacity) {
        if (initialCapacity <= 0) {
            throw new IllegalArgumentException("initialCapacity must be positive");
        }
        this.heap = (Node<V>[]) new Node<?>[initialCapacity];
    }

    public void lock() {
        lock.lock();
    }

    public void unlock() {
        lock.unlock();
    }

    public boolean tryLock() {
        return lock.tryLock();
    }

    public void insert(Node<V> node) {
        lock();
        try {
            insertUnlocked(node);
        } finally {
            unlock();
        }
    }

    public Node<V> deleteMin() {
        lock();
        try {
            return deleteMinUnlocked();
        } finally {
            unlock();
        }
    }

    public Node<V> peekMin() {
        lock();
        try {
            return peekMinUnlocked();
        } finally {
            unlock();
        }
    }

    public int size() {
        lock();
        try {
            return size;
        } finally {
            unlock();
        }
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    public void insertUnlocked(Node<V> node) {
        Objects.requireNonNull(node, "node");
        // Do not touch node.next here. WorkerHeap is array-based and never reads that field
        // itself, but the same Node object can still be reachable from a concurrent
        // LeaderLinkedList traversal — for example a search()/searchDelete() on another thread
        // that is lagging behind — if this node was just evicted from the leader list and
        // handed straight to a worker heap (this happens on Pipq's slowest-path and promotion
        // paths). Overwriting next here would corrupt that in-flight lock-free traversal.
        ensureCapacity(size + 1);

        int idx = size;
        while (idx > 0) {
            int parent = parentIdx(idx);
            Node<V> parentNode = heap[parent];
            if (Node.compare(parentNode, node) <= 0) {
                break;
            }
            heap[idx] = parentNode;
            idx = parent;
        }

        heap[idx] = node;
        size++;
    }

    public Node<V> deleteMinUnlocked() {
        if (size == 0) {
            return null;
        }

        Node<V> min = heap[0];
        Node<V> last = heap[--size];
        heap[size] = null;

        if (size > 0) {
            siftDown(last);
        }

        // min.next is deliberately left untouched here, for the same reason explained in
        // insertUnlocked above.
        return min;
    }

    public Node<V> peekMinUnlocked() {
        return size == 0 ? null : heap[0];
    }

    public int sizeUnlocked() {
        return size;
    }

    private void siftDown(Node<V> node) {
        int idx = 0;
        int half = size / 2;

        while (idx < half) {
            int left = leftChildIdx(idx);
            int right = left + 1;
            int smallerChild = left;

            if (right < size && Node.compare(heap[right], heap[left]) < 0) {
                smallerChild = right;
            }

            Node<V> child = heap[smallerChild];
            if (Node.compare(node, child) <= 0) {
                break;
            }

            heap[idx] = child;
            idx = smallerChild;
        }

        heap[idx] = node;
    }

    private void ensureCapacity(int needed) {
        if (needed <= heap.length) {
            return;
        }
        int newCapacity = Math.max(needed, heap.length * 2);
        heap = Arrays.copyOf(heap, newCapacity);
    }

    private static int parentIdx(int idx) {
        return (idx - 1) >>> 1;
    }

    private static int leftChildIdx(int idx) {
        return (idx << 1) + 1;
    }
}
