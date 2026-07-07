import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Custom array-backed binary min-heap used for the PIPQ worker level.
 *
 * <p>Each logical thread owns one heap. The owning thread performs most inserts
 * locally, while delete-min repair may briefly access another thread's heap to
 * promote its minimum element back into the leader layer.</p>
 */
public final class WorkerHeap<V> {
    private static final int DEFAULT_CAPACITY = 16;

    private final ReentrantLock lock = new ReentrantLock();
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

    void insertUnlocked(Node<V> node) {
        Objects.requireNonNull(node, "node");
        node.next = null;
        ensureCapacity(size + 1);

        int idx = size;
        while (idx > 0) {
            int parent = parent(idx);
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

    Node<V> deleteMinUnlocked() {
        if (size == 0) {
            return null;
        }

        Node<V> min = heap[0];
        Node<V> last = heap[--size];
        heap[size] = null;

        if (size > 0) {
            siftDown(last);
        }

        min.next = null;
        return min;
    }

    Node<V> peekMinUnlocked() {
        return size == 0 ? null : heap[0];
    }

    int sizeUnlocked() {
        return size;
    }

    private void siftDown(Node<V> node) {
        int idx = 0;
        int half = size / 2;

        while (idx < half) {
            int left = leftChild(idx);
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

    private static int parent(int idx) {
        return (idx - 1) >>> 1;
    }

    private static int leftChild(int idx) {
        return (idx << 1) + 1;
    }
}
