import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicStampedReference;

/**
 * Lock-free port of PIPQ's leader-level sorted linked list (Harris/Linden hybrid).
 *
 * <p>Ported from the original C++ ({@code harris.h}/{@code harris.cc}). Each node's
 * {@code next} pointer carries a two-bit stamp: bit 0 ({@code logdel}) means the
 * successor of this node is logically deleted (set on the predecessor, Linden-style);
 * bit 1 ({@code moving}) means this node itself is being moved by a concurrent
 * by-thread delete (set on the node itself, Harris-style). {@code search}, {@code insert}
 * (with {@code lead_largest} tracking), and {@code maxByThread} are implemented here;
 * {@code deleteMin}/{@code deleteMaxByThread} are ported in later passes — note
 * {@code maxByThread} will go stale once {@code deleteMaxByThread} exists unless that
 * pass also resets/recomputes {@code maxPerTid[tid]}.</p>
 */
public class LeaderLinkedList<V> implements LeaderLayer<V> {
    private static final int LOGDEL_BIT = 1;
    private static final int MOVING_BIT = 2;

    private final Node<V> head = Node.sentinel(Long.MIN_VALUE, Long.MIN_VALUE);
    private final Node<V> tail = Node.sentinel(Long.MAX_VALUE, Long.MAX_VALUE);
    private final Node<V>[] maxPerTid;
    private final AtomicInteger size = new AtomicInteger(0);

    static class Window<V> {
        private Node<V> leftNode;
        private Node<V> rightNode;

        Window(Node<V> left, Node<V> right) {
            this.leftNode = left;
            this.rightNode = right;
        }

        Node<V> left() {
            return leftNode;
        }

        Node<V> right() {
            return rightNode;
        }
    }

    @SuppressWarnings("unchecked")
    public LeaderLinkedList(int threadCount) {
        this.maxPerTid = (Node<V>[]) new Node<?>[threadCount];
        head.next = new AtomicStampedReference<>(tail, 0);
    }

    @Override
    public void insert(Node<V> node) {
        Objects.requireNonNull(node, "node");
        while (true) {
            Window<V> window = search(node);
            Node<V> left = window.left();
            Node<V> right = window.right();

            node.next = new AtomicStampedReference<>(right, 0);

            // search() guarantees left.next == (right, 0) [no logdel/moving bits] at the
            // moment it returns; mirrors harris_insert's CAS(&left->next, right, newnode)
            // on the raw (unmarked) pointer. If it has since changed, CAS fails and we
            // retry the whole search from head, exactly like the C++ do/while.
            if (left.next.compareAndSet(right, node, 0, 0)) {
                size.incrementAndGet();
                updateLargestByThread(node);
                return;
            }
        }
    }

    /**
     * Ported from {@code L-Insert} lines 6-7 (paper pseudocode) / {@code harris_insert}'s
     * {@code last_ptr->largest_ptr} update (harris.cc:383). Each tid's slot is only ever
     * written by that tid's own insert calls, so a plain array write is safe — same
     * single-writer assumption the paper's {@code LeaderLargest} struct relies on.
     */
    private void updateLargestByThread(Node<V> node) {
        Node<V> currentLargest = maxPerTid[node.tid()];
        if (currentLargest == null || node.key() > currentLargest.key()) {
            maxPerTid[node.tid()] = node;
        }
    }

    @Override
    public Node<V> deleteMin() {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Node<V> deleteMaxByThread(int tid) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Node<V> maxByThread(int tid) {
        return maxPerTid[tid];
    }

    @Override
    public int size() {
        return size.get();
    }

    @Override
    public List<Node<V>> snapshot() {
        List<Node<V>> nodes = new ArrayList<>();
        Node<V> cur = head.next.getReference();
        while (cur != tail) {
            nodes.add(cur);
            cur = cur.next.getReference();
        }
        return nodes;
    }

    /**
     * Validation-only helper (not part of {@link LeaderLayer}): scans the list to count
     * live nodes per thread. Mirrors the old {@code SortedLinkedListLeader}'s counting
     * used by {@code Pipq.validateLeaderCounters}/{@code validateInvariant} — the real
     * per-thread counters are tracked O(1) in {@code Pipq.leaderCounters}, this is only
     * for cross-checking that count in tests.
     */
    int[] countsByThread(int threadCount) {
        int[] counts = new int[threadCount];
        for (Node<V> node : snapshot()) {
            if (node.tid() >= 0 && node.tid() < threadCount) {
                counts[node.tid()]++;
            }
        }
        return counts;
    }

    @Override
    public boolean validateSorted() {
        List<Node<V>> nodes = snapshot();
        for (int i = 1; i < nodes.size(); i++) {
            if (Node.compare(nodes.get(i - 1), nodes.get(i)) > 0) {
                return false;
            }
        }
        return true;
    }

    // ============== Utility ==============

    /**
     * Ported from {@code harris_search} (harris.cc:293-332). Finds the window
     * {@code (left, right)} such that {@code left < target <= right} in list order,
     * physically unlinking any logically-deleted nodes it passes along the way.
     *
     * <p>{@code left} is only ever recorded at a non-moving node, so a concurrent
     * {@code deleteMaxByThread}-style move can never race the unlink CAS below it.</p>
     */
    Window<V> search(Node<V> target) {
        searchAgain:
        while (true) {
            Node<V> left = head;
            int[] leftNextStamp = new int[1];
            Node<V> leftNext = head.next.get(leftNextStamp);
            leftNextStamp[0] &= ~LOGDEL_BIT;

            Node<V> x = head;
            int[] xNextStamp = new int[1];
            Node<V> xNext = x.next.get(xNextStamp);

            boolean prevLogdel;
            do {
                if ((xNextStamp[0] & MOVING_BIT) == 0) {
                    left = x;
                    leftNext = xNext;
                    leftNextStamp[0] = xNextStamp[0] & ~LOGDEL_BIT;
                }
                x = xNext;
                if (x == tail) {
                    break;
                }
                prevLogdel = (xNextStamp[0] & LOGDEL_BIT) != 0;
                xNext = x.next.get(xNextStamp);
            } while (Node.compare(x, target) < 0 || (xNextStamp[0] & MOVING_BIT) != 0 || prevLogdel);

            Node<V> right = x;

            if (leftNext == right) {
                if (isRightMovingOrLeftLogdel(left, right)) {
                    continue searchAgain;
                }
                return new Window<>(left, right);
            }

            if (left.next.compareAndSet(leftNext, right, leftNextStamp[0], leftNextStamp[0])) {
                if (isRightMovingOrLeftLogdel(left, right)) {
                    continue searchAgain;
                }
                return new Window<>(left, right);
            }
            // CAS lost the race (left's stamp/reference changed) -> retry from head.
        }
    }

    private boolean isRightMovingOrLeftLogdel(Node<V> left, Node<V> right) {
        boolean rightMoving = right != tail && right.movedBit();
        boolean leftLogdel = left.delBit();
        return rightMoving || leftLogdel;
    }
}
