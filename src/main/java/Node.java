import java.util.Objects;

/**
 * Element stored by both PIPQ levels.
 *
 * <p>The paper stores key/value pairs in worker heaps and key/value/tid tuples in
 * the leader list. The Java baseline keeps the original inserting thread id and
 * a unique sequence number on every node so elements can move between levels
 * without losing ownership or deterministic tie-breaking.</p>
 */
public final class Node<V> implements Comparable<Node<V>> {
    private final long key;
    private final V value;
    private final int tid;
    private final long sequence;

    Node<V> next;

    public Node(long key, V value, int tid, long sequence) {
        this(key, value, tid, sequence, false);
    }

    private Node(long key, V value, int tid, long sequence, boolean sentinel) {
        if (!sentinel && tid < 0) {
            throw new IllegalArgumentException("tid must be non-negative");
        }
        this.key = key;
        this.value = value;
        this.tid = tid;
        this.sequence = sequence;
    }

    static <V> Node<V> sentinel(long key, long sequence) {
        return new Node<>(key, null, -1, sequence, true);
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

    public long sequence() {
        return sequence;
    }

    @Override
    public int compareTo(Node<V> other) {
        return compare(this, other);
    }

    static int compare(Node<?> left, Node<?> right) {
        int byKey = Long.compare(left.key, right.key);
        if (byKey != 0) {
            return byKey;
        }
        return Long.compare(left.sequence, right.sequence);
    }

    @Override
    public String toString() {
        return "Node{"
                + "key=" + key
                + ", value=" + value
                + ", tid=" + tid
                + ", sequence=" + sequence
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
                && sequence == other.sequence
                && Objects.equals(value, other.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value, tid, sequence);
    }
}
