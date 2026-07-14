import java.util.Objects;
import java.util.concurrent.atomic.AtomicStampedReference;

/**
 * Element stored by both PIPQ levels.
 *
 * <p>The paper stores key/value pairs in worker heaps and key/value/tid tuples in
 * the leader list. The Java baseline keeps the original inserting thread id on
 * every node so elements can move between levels without losing ownership.
 * Equal-key nodes compare equal (paper: duplicates in undefined order).</p>
 */
public final class Node<V> implements Comparable<Node<V>> {
    private final long key;
    private final V value;
    private final int tid;

    public AtomicStampedReference<Node<V>> next;

    public Node(long key, V value, int tid) {
        this(key, value, tid, false);
    }

    private Node(long key, V value, int tid, boolean sentinel) {
        if (!sentinel && tid < 0) {
            throw new IllegalArgumentException("tid must be non-negative");
        }
        this.key = key;
        this.value = value;
        this.tid = tid;
    }

    static <V> Node<V> sentinel(long key) {
        return new Node<>(key, null, -1, true);
    }

    public long key() {
        return key;
    }

    public V value() {
        return value;
    }

    public int tid() {
        return tid;
    }

    // whether THIS node is moved
    public boolean movedBit()
    {
        int[] stampHolder = new int[1];
        Node<V> n = next.get(stampHolder);
        return (stampHolder[0] & 2) != 0;
    }

    // whether NEXT node is logically deleted
    public boolean delBit()
    {
        int[] stampHolder = new int[1];
        Node<V> n = next.get(stampHolder);
        return (stampHolder[0] & 1) != 0;
    }

    @Override
    public int compareTo(Node<V> other) {
        return compare(this, other);
    }

    static int compare(Node<?> left, Node<?> right) {
        return Long.compare(left.key, right.key);
    }

    @Override
    public String toString() {
        return "Node{"
                + "key=" + key
                + ", value=" + value
                + ", tid=" + tid
                + '}';
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Node)) {
            return false;
        }
        Node<?> other = (Node<?>) obj;
        return key == other.key
                && tid == other.tid
                && Objects.equals(value, other.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value, tid);
    }
}
