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
 * (with {@code lead_largest} tracking), {@code maxByThread}, {@code deleteMaxByThread}
 * (with its {@code searchDelete}/{@code searchPhysDel} helpers), and {@code deleteMin} are all
 * implemented here.</p>
 */
public class LeaderLinkedList<V> implements LeaderLayer<V> {
    private static final int LOGDEL_BIT = 1;
    private static final int MOVING_BIT = 2;

    /**
     * Batched-physical-deletion threshold for {@link #deleteMin}, ported from the paper's
     * Lindén-Jonsson-style {@code MAX_OFFSET} (Algorithm 5 line 14). Once more than this many
     * logically-deleted nodes have accumulated at the front of the list, {@code deleteMin}
     * snips the whole prefix in one {@code head.next} store instead of leaving them for the
     * next traversal to skip one-by-one.
     */
    private static final int MAX_OFFSET = 32;

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

            if (left.next.compareAndSet(right, node, 0, 0)) {
                size.incrementAndGet();
                updateLargestByThread(node);
                return;
            }
        }
    }

    private void updateLargestByThread(Node<V> node) {
        Node<V> currentLargest = maxPerTid[node.tid()];
        if (currentLargest == null || node.key() > currentLargest.key()) {
            maxPerTid[node.tid()] = node;
        }
    }

    /**
     * Ported from {@code L-DeleteMin} (Algorithm 5, paper p.13). Removes and returns the
     * global minimum — {@code head}'s first live successor. Returns {@code null} if the list
     * is empty ({@code EMPTY} in the paper).
     *
     * <p>Relies on delete-min being globally serialized by the caller (paper: "only one thread
     * is ever performing a delete-min operation at the time"; our baseline enforces this via
     * {@code Pipq.deleteMinLock}). Only {@code LOGDEL} is ever checked/set here — never
     * {@code MOVING} — because the ≥2-elements-per-thread invariant (Lemma 2/4) guarantees the
     * global min can never simultaneously be some thread's {@code deleteMaxByThread} target,
     * matching the paper's own omission of moving-bit checks in this algorithm.</p>
     *
     * <p>{@code maxPerTid} is deliberately left untouched, exactly as in the paper: if the
     * removed node happened to be some tid's {@code maxPerTid[tid]}, that pointer only goes
     * stale in the narrow window before it self-heals via that tid's next {@code insert}
     * ({@link #updateLargestByThread}) or next {@code deleteMaxByThread} call (which
     * re-derives it in {@link #searchDelete}).</p>
     */
    @Override
    public Node<V> deleteMin() {
        Node<V> x = head;
        int offset = 0;

        while (true) {
            int[] xNextStamp = new int[1];
            Node<V> xNextRef = x.next.get(xNextStamp);

            // Paper line 7: get_notlogdel_ref(x_next) == list.tail -> list is empty. The
            // MOVING bit never applies to tail (it's a sentinel, never anyone's move target),
            // so a plain reference check already matches "notlogdel_ref".
            if (xNextRef == tail) {
                return null;
            }

            if ((xNextStamp[0] & LOGDEL_BIT) != 0) {
                // Paper lines 9-10: x's successor was already logically deleted by an
                // earlier delete-min call and not yet physically unlinked -> skip past it
                // and keep looking for the true (live) min.
                offset++;
                x = xNextRef;
                continue;
            }

            // Paper line 11: x_next <- fetch_and_or(&x.next, LOGDEL_BIT). Real hardware
            // fetch-and-or always succeeds unconditionally in one step; Java's
            // AtomicStampedReference has no atomic bitwise-or, so this is emulated with a
            // CAS that only ever changes the stamp (reference held fixed at xNextRef).
            // Unlike real fetch_and_or this CAS *can* fail -- e.g. a concurrent insert()
            // splicing a new, smaller-key node in ahead of xNextRef -- so on failure just
            // retry with a fresh read of x.next (x itself doesn't move). This is a
            // Java-translation detail with no counterpart in the paper, so it does not
            // advance offset.
            if (!x.next.compareAndSet(xNextRef, xNextRef, xNextStamp[0], xNextStamp[0] | LOGDEL_BIT)) {
                continue;
            }

            // This CAS success is the linearization point: xNextRef is now the removed
            // global minimum (paper's reassigned "x" at line 12 / new_head at line 13).
            offset++;
            Node<V> newHead = xNextRef;
            size.decrementAndGet();

            if (offset > MAX_OFFSET) {
                // Paper lines 14-15: batched physical deletion, Linden-Jonsson style --
                // swing head straight past the whole logically-deleted prefix in one
                // store, abandoning every skipped dead node at once. A plain store (not a
                // CAS) is both faithful to the paper and safe: every other mutator's
                // relink CAS (insert's front CAS at head.next, and search's /
                // searchDelete's / searchPhysDel's unlink CASes) requires a
                // logdel-cleared expected stamp on head.next and simply fails/retries
                // against a logdel'd head -- and per search()'s own guard
                // (isRightMovingOrLeftLogdel), no insert can ever complete using a
                // logdel'd node as its left predecessor, so nothing can have spliced
                // itself into the prefix being collapsed here. Combined with delete-min's
                // own global serialization, no concurrent writer can be racing this store.
                head.next.set(newHead, LOGDEL_BIT);
            }

            return newHead;
        }
    }

    /**
     * Ported from {@code L-DeleteMaxP} (Algorithm 4 lines 1-13, paper p.12). Removes and
     * returns the node tagged {@code tid} with the largest key currently in the list — used
     * by the slowest insert path to make room for a new node before pushing it into the list.
     * Returns {@code null} if {@code tid} currently has no node in the list.
     *
     * <p>Correctness relies on {@code maxPerTid[tid]} being single-writer (only thread
     * {@code tid} ever inserts/removes its own tid-tagged nodes) — see class javadoc and
     * {@link #updateLargestByThread}. {@code startLeadLargest} is captured once, up front,
     * and reused across every {@code searchDelete} retry (mirrors the paper's fixed
     * {@code start_lead_largest} local, reused across the {@code repeat} at line 3).</p>
     */
    @Override
    public Node<V> deleteMaxByThread(int tid) {
        Node<V> startLeadLargest = maxPerTid[tid];
        if (startLeadLargest == null) {
            return null;
        }

        Node<V> lNode;
        Node<V> rNode;
        Node<V> rNodeNextRef;
        while (true) {
            Window<V> w = searchDelete(tid, startLeadLargest);
            lNode = w.left();
            rNode = w.right();

            int[] rNextStamp = new int[1];
            rNodeNextRef = rNode.next.get(rNextStamp);
            if (!isMarked(rNextStamp[0])
                    && rNode.next.compareAndSet(rNodeNextRef, rNodeNextRef, rNextStamp[0], rNextStamp[0] | MOVING_BIT)) {
                break;
            }
            // r_node was already marked (moving/logdel) by someone else, or lost the CAS
            // race -> retry the whole searchDelete to find the current target.
        }

        size.decrementAndGet();

        int[] lNextStamp = new int[1];
        lNode.next.get(lNextStamp);
        int expectedLStamp = lNextStamp[0] & ~LOGDEL_BIT;
        if (!lNode.next.compareAndSet(rNode, rNodeNextRef, expectedLStamp, expectedLStamp)) {
            searchPhysDel(rNode);
        }
        return rNode;
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

    private static boolean isMarked(int stamp) {
        return (stamp & (LOGDEL_BIT | MOVING_BIT)) != 0;
    }

    /**
     * Ported from {@code searchDelete} (Algorithm 4 lines 14-47, paper p.12), the by-thread
     * variant of {@link #search}. Walks from {@code head}, tracking the running non-moving
     * predecessor candidate exactly like {@code search()}'s {@code left}/{@code leftNext}
     * (here {@code curLNode}/{@code curLNodeNext}/{@code curLNodeNextStamp}), and additionally
     * remembers the most recently seen {@code tid}-tagged node as {@code rNode} (with its own
     * non-moving predecessor snapshot as {@code lNode}/{@code lNodeNext}/{@code lNodeNextStamp})
     * until the walk reaches {@code startLeadLargest} — the node {@link #deleteMaxByThread}
     * intends to remove. {@code newLeadLargest} accumulates the tid-tagged node found just
     * before {@code rNode}, i.e. what becomes tid's new largest once {@code rNode} is gone; it
     * is written into {@code maxPerTid[tid]} before returning, mirroring the paper's
     * {@code lead_largest ← new_lead_largest} (line 35).
     *
     * <p>{@code maxPerTid[tid]} is only ever written by thread {@code tid} itself (here, and
     * in {@link #updateLargestByThread} on insert), so the plain array write is safe — same
     * single-writer assumption as elsewhere in this class.</p>
     */
    Window<V> searchDelete(int tid, Node<V> startLeadLargest) {
        while (true) {
            Node<V> curLNode = head;
            int[] curLNodeNextStamp = new int[1];
            Node<V> curLNodeNext = head.next.get(curLNodeNextStamp);
            curLNodeNextStamp[0] &= ~LOGDEL_BIT;

            Node<V> lNode = null;
            Node<V> lNodeNext = null;
            int lNodeNextStamp = 0;
            Node<V> rNode = null;
            Node<V> newLeadLargest = null;

            Node<V> x = head;
            int[] xNextStamp = new int[1];
            Node<V> xNext = x.next.get(xNextStamp);

            while (true) {
                if ((xNextStamp[0] & MOVING_BIT) == 0) {
                    curLNode = x;
                    curLNodeNext = xNext;
                    curLNodeNextStamp[0] = xNextStamp[0] & ~LOGDEL_BIT;
                }
                x = xNext;
                if (x == tail) {
                    break;
                }
                xNext = x.next.get(xNextStamp);
                if ((xNextStamp[0] & MOVING_BIT) == 0 && x.tid() == tid) {
                    newLeadLargest = rNode;
                    lNode = curLNode;
                    lNodeNext = curLNodeNext;
                    lNodeNextStamp = curLNodeNextStamp[0];
                    rNode = x;
                    if (x == startLeadLargest) {
                        break;
                    }
                }
            }

            maxPerTid[tid] = newLeadLargest;

            if (lNodeNext == rNode) {
                return new Window<>(lNode, rNode);
            }

            if (lNode.next.compareAndSet(lNodeNext, rNode, lNodeNextStamp, lNodeNextStamp)) {
                return new Window<>(lNode, rNode);
            }
            // CAS lost the race (l_node's stamp/reference changed) -> retry from head.
        }
    }

    /**
     * Ported from {@code searchPhysDel} (Algorithm 4 lines 48-73, paper p.12). Physically
     * unlinks {@code searchNode} once its stable (non-moving, non-logically-deleted)
     * predecessor is located — the fallback {@link #deleteMaxByThread} calls when its direct
     * {@code l_node}/{@code r_node} unlink CAS loses a race. If {@code searchNode} is no
     * longer found in the list, another thread has already physically unlinked it and this is
     * a no-op, matching the paper's "search_node has been removed by another thread" case.
     */
    void searchPhysDel(Node<V> searchNode) {
        while (true) {
            Node<V> lNode = head;
            int[] lNodeNextStamp = new int[1];
            Node<V> lNodeNext = head.next.get(lNodeNextStamp);
            lNodeNextStamp[0] &= ~LOGDEL_BIT;

            boolean found = false;
            boolean prevLogdel;

            Node<V> x = head;
            int[] xNextStamp = new int[1];
            Node<V> xNext = x.next.get(xNextStamp);

            do {
                if ((xNextStamp[0] & MOVING_BIT) == 0) {
                    lNode = x;
                    lNodeNext = xNext;
                    lNodeNextStamp[0] = xNextStamp[0] & ~LOGDEL_BIT;
                }
                x = xNext;
                if (x == tail) {
                    break;
                }
                if (x == searchNode) {
                    found = true;
                }
                prevLogdel = (xNextStamp[0] & LOGDEL_BIT) != 0;
                xNext = x.next.get(xNextStamp);
            } while (!(found && (xNextStamp[0] & MOVING_BIT) == 0 && !prevLogdel));

            if (!found) {
                return;
            }

            Node<V> rNode = x;

            if (lNode.next.compareAndSet(lNodeNext, rNode, lNodeNextStamp[0], lNodeNextStamp[0])) {
                return;
            }
            // CAS lost the race -> retry from head.
        }
    }
}
