import java.util.Objects;
import java.util.concurrent.atomic.AtomicStampedReference;

/**
 * Element stored at both PIPQ levels (worker heap and leader list).
 *
 * <p>The paper stores plain key/value pairs in worker heaps, and key/value/tid triples in the
 * leader list. This class simplifies that by keeping the original inserting thread's
 * id ({@code tid}) on every node, in both places, so an element can move between levels
 * without losing track of which thread it belongs to. Nodes with equal keys compare as equal;
 * the paper leaves the order among such duplicates undefined, so this is a safe choice.</p>
 * <ul>
 *   <li>bit 0, {@code logdel} — set on a node's {@code next} pointer to mean "this node's
 *       successor has been logically deleted" (Linden-style: the flag lives on the
 *       predecessor, not on the deleted node itself).</li>
 *   <li>bit 1, {@code moving} — set on a node's own {@code next} pointer to mean "this node
 *       is currently being moved by a concurrent by-thread delete" (Harris-style: the flag
 *       lives on the node being removed).</li>
 * </ul>
 */
public final class Node<V> implements Comparable<Node<V>> {
    public static final int LOGDEL_BIT = 1;
    public static final int MOVING_BIT = 2;

    private final long key;
    private final V value;
    private final int tid;

    private AtomicStampedReference<Node<V>> next;

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

    public static <V> Node<V> sentinel(long key) {
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

    public AtomicStampedReference<Node<V>> next() {
        return next;
    }

    public void setNext(Node<V> nextNode, int stamp) {
        this.next = new AtomicStampedReference<>(nextNode, stamp);
    }

    // True if this node's successor has been logically deleted (the LOGDEL bit, set on this
    // node's next pointer to mark the *next* node as removed). applies to deleteMin
    public boolean delBit()
    {
        int[] stampHolder = new int[1];
        next.get(stampHolder);
        return (stampHolder[0] & 1) != 0;
    }

    // True if this node itself is currently being moved by a concurrent deleteMaxByThread
    // (the MOVING bit, set on the node's own next pointer).
    public boolean movedBit()
    {
        int[] stampHolder = new int[1];
        next.get(stampHolder);
        return (stampHolder[0] & 2) != 0;
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
