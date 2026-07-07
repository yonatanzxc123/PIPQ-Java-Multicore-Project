import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;

/**
 * PIPQ leader level implemented as the original paper's sorted linked list.
 *
 * <p>The paper implements this layer with a specialized lock-free linked list
 * using pointer marking. This Java baseline keeps the same sorted-list behavior
 * and operations, but protects the list with a normal lock so the code remains
 * understandable and suitable for correctness testing.</p>
 */
public final class SortedLinkedListLeader<V> implements LeaderLayer<V> {
    private final ReentrantLock lock = new ReentrantLock();
    private final Node<V> head = Node.sentinel(Long.MIN_VALUE, Long.MIN_VALUE);
    private final Node<V> tail = Node.sentinel(Long.MAX_VALUE, Long.MAX_VALUE);
    private int size;

    public SortedLinkedListLeader() {
        head.next = tail;
    }

    @Override
    public void insert(Node<V> node) {
        lock();
        try {
            insertUnlocked(node);
        } finally {
            unlock();
        }
    }

    @Override
    public Node<V> deleteMin() {
        lock();
        try {
            return deleteMinUnlocked();
        } finally {
            unlock();
        }
    }

    @Override
    public Node<V> deleteMaxByThread(int tid) {
        lock();
        try {
            return deleteMaxByThreadUnlocked(tid);
        } finally {
            unlock();
        }
    }

    @Override
    public Node<V> maxByThread(int tid) {
        lock();
        try {
            return maxByThreadUnlocked(tid);
        } finally {
            unlock();
        }
    }

    @Override
    public int sizeByThread(int tid) {
        lock();
        try {
            return sizeByThreadUnlocked(tid);
        } finally {
            unlock();
        }
    }

    @Override
    public int size() {
        lock();
        try {
            return size;
        } finally {
            unlock();
        }
    }

    @Override
    public List<Node<V>> snapshot() {
        lock();
        try {
            return snapshotUnlocked();
        } finally {
            unlock();
        }
    }

    @Override
    public boolean validateSorted() {
        lock();
        try {
            return validateSortedUnlocked();
        } finally {
            unlock();
        }
    }

    void lock() {
        lock.lock();
    }

    void unlock() {
        lock.unlock();
    }

    void insertUnlocked(Node<V> node) {
        Objects.requireNonNull(node, "node");

        Node<V> prev = head;
        Node<V> cur = head.next;
        while (cur != tail && Node.compare(cur, node) < 0) {
            prev = cur;
            cur = cur.next;
        }

        node.next = cur;
        prev.next = node;
        size++;
    }

    Node<V> deleteMinUnlocked() {
        Node<V> first = head.next;
        if (first == tail) {
            return null;
        }

        head.next = first.next;
        first.next = null;
        size--;
        return first;
    }

    Node<V> deleteMaxByThreadUnlocked(int tid) {
        Node<V> maxPrev = null;
        Node<V> maxNode = null;
        Node<V> prev = head;
        Node<V> cur = head.next;

        while (cur != tail) {
            if (cur.tid() == tid) {
                maxPrev = prev;
                maxNode = cur;
            }
            prev = cur;
            cur = cur.next;
        }

        if (maxNode == null) {
            return null;
        }

        maxPrev.next = maxNode.next;
        maxNode.next = null;
        size--;
        return maxNode;
    }

    Node<V> maxByThreadUnlocked(int tid) {
        Node<V> maxNode = null;
        Node<V> cur = head.next;
        while (cur != tail) {
            if (cur.tid() == tid) {
                maxNode = cur;
            }
            cur = cur.next;
        }
        return maxNode;
    }

    int sizeByThreadUnlocked(int tid) {
        int count = 0;
        Node<V> cur = head.next;
        while (cur != tail) {
            if (cur.tid() == tid) {
                count++;
            }
            cur = cur.next;
        }
        return count;
    }

    int[] countsByThreadUnlocked(int threadCount) {
        int[] counts = new int[threadCount];
        Node<V> cur = head.next;
        while (cur != tail) {
            if (cur.tid() >= 0 && cur.tid() < threadCount) {
                counts[cur.tid()]++;
            }
            cur = cur.next;
        }
        return counts;
    }

    List<Node<V>> snapshotUnlocked() {
        List<Node<V>> nodes = new ArrayList<>(size);
        Node<V> cur = head.next;
        while (cur != tail) {
            nodes.add(cur);
            cur = cur.next;
        }
        return nodes;
    }

    boolean validateSortedUnlocked() {
        Node<V> cur = head.next;
        Node<V> prev = null;
        int seen = 0;

        while (cur != tail) {
            if (prev != null && Node.compare(prev, cur) > 0) {
                return false;
            }
            prev = cur;
            cur = cur.next;
            seen++;
            if (seen > size) {
                return false;
            }
        }

        return seen == size;
    }
}
