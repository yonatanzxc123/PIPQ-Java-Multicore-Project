import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Lock-free port of PIPQ's leader-level sorted linked list (a Harris/Linden hybrid).
 *
 * <p>Ported from the original C++ ({@code harris.h}/{@code harris.cc}). Each node's {@code
 * next} pointer carries a two-bit stamp with two independent flags. See {@link Node}.
 * This class implements {@code search}, {@code insert} (which also maintains {@code
 * maxPerTid}, this class's per-thread "largest node in the list" pointer — the paper calls the
 * same concept {@code lead_largest}), {@code maxByThread}, {@code deleteMaxByThread} (with
 * the {@code searchDelete}/{@code searchPhysDel} helpers), and {@code deleteMin}.</p>
 */
public class LeaderLinkedList<V> implements LeaderLayer<V> {
    private static final int LOGDEL_BIT = Node.LOGDEL_BIT;
    private static final int MOVING_BIT = Node.MOVING_BIT;

    /**
     * How many logically-deleted nodes {@link #deleteMin} lets pile up at the front of the list
     * before physically unlinking them all at once, instead of leaving them for the next
     * traversal to step over one by one. Ported from the paper's Lindén-Jonsson-style {@code
     * MAX_OFFSET} (Alg. 5 line 14).
     */
    private static final int MAX_OFFSET = 32;

    private final Node<V> head = Node.sentinel(Long.MIN_VALUE);
    private final Node<V> tail = Node.sentinel(Long.MAX_VALUE);
    private final Node<V>[] maxPerTid;
    private final AtomicInteger size = new AtomicInteger(0);

    public static class Window<V> {
        private final Node<V> leftNode;
        private final Node<V> rightNode;

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
        head.setNext(tail, 0);
    }

    /**
     * Ported from {@code L-Insert} (Alg. 3, paper p.11).
     * Inserts new node into the correct place in list.
     */
    @Override
    public void insert(Node<V> node) {
        Objects.requireNonNull(node, "node");
        while (true) {
            Window<V> window = search(node);
            Node<V> left = window.left();
            Node<V> right = window.right();

            node.setNext(right, 0);

            if (left.next().compareAndSet(right, node, 0, 0)) {
                size.incrementAndGet();
                updateLargestByThread(node);
                return;
            }
        }
    }

    // A plain (non-atomic) array write is safe here.
    // callers serialize access per tid - see the explanation in LeaderLinkedList javadoc.
    private void updateLargestByThread(Node<V> node) {
        Node<V> currentLargest = maxPerTid[node.tid()];
        if (currentLargest == null || node.key() > currentLargest.key()) {
            maxPerTid[node.tid()] = node;
        }
    }

    /**
     * Ported from {@code L-DeleteMin} (Alg. 5, paper p.13). Removes and returns the global
     * minimum — {@code head}'s first live successor. Returns {@code null} if the list is empty.
     *
     * <p><b>Requires a single caller at a time.</b> The paper states "only one thread is ever
     * performing a delete-min operation at the time", and this enforces that through
     * {@code Pipq.coordinatorLock}: only the thread holding that lock ever calls this method,
     * whether it's serving its own delete-min request or combining several requests together.
     *
     * <p><b>Why this method only ever checks/sets {@code LOGDEL}, never {@code MOVING}.</b> The
     * paper's ≥2-elements-per-thread invariant (Lemma 2/4) guarantees that the current global
     * minimum can never, at the same time, be the node some other thread's {@code
     * deleteMaxByThread}/{@code insertAndDeleteMaxByThread} is trying to remove.
     *
     * <p><b>{@code maxPerTid} is deliberately left untouched here</b>, exactly as in the paper. If
     * the node this method just removed happened to be some thread's recorded {@code
     * maxPerTid[tid]}, that entry goes briefly stale — but it self-heals the next time that
     * thread calls {@code insert} (via {@link #updateLargestByThread}) or the next time {@code
     * deleteMaxByThread}/{@code insertAndDeleteMaxByThread} runs for it (which recomputes the
     * value fresh inside {@link #searchDelete}).</p>
     */
    @Override
    public Node<V> deleteMin() {
        Node<V> x = head;
        int offset = 0;

        while (true) {
            int[] xNextStamp = new int[1];
            Node<V> xNextRef = x.next().get(xNextStamp);

            // Paper line 7: if x's successor is the tail sentinel, the list is empty. We don't
            // need to separately check the MOVING bit here, because MOVING never applies to
            // tail — it's a sentinel and can never be anyone's evict target — so a plain
            // reference comparison is enough.
            if (xNextRef == tail) {
                return null;
            }

            if ((xNextStamp[0] & LOGDEL_BIT) != 0) {
                // Paper lines 9-10: x's successor was already logically deleted by an earlier
                // delete-min call and just hasn't been physically unlinked yet. Skip past it and
                // keep looking for the actual (live) minimum.
                offset++;
                x = xNextRef;
                continue;
            }

            // Paper line 11: fetch_and_or(&x.next, LOGDEL_BIT) — atomically set the LOGDEL bit
            // on x's successor pointer. On real hardware, fetch-and-or always succeeds in one
            // step. Java's AtomicStampedReference has no atomic bitwise-or, so this is emulated
            // with a compare-and-set that changes only the stamp, keeping the reference fixed at
            // xNextRef. Unlike a true fetch-and-or, this CAS can fail - for example if a
            // concurrent insert() has just spliced in a new, smaller-key node ahead of
            // xNextRef — in which case we simply retry with a fresh read of x.next (x itself
            // doesn't move). This retry is a detail of the Java translation with no counterpart
            // in the paper, so it does not advance offset.
            if (!x.next().compareAndSet(xNextRef, xNextRef, xNextStamp[0], xNextStamp[0] | LOGDEL_BIT)) {
                continue;
            }

            // This CAS succeeding is the linearization point of the operation: xNextRef is now
            // the removed global minimum (the paper's reassigned "x" at line 12, also called
            // new_head at line 13).
            offset++;
            Node<V> newHead = xNextRef;
            size.decrementAndGet();

            if (offset > MAX_OFFSET) {
                // Paper lines 14-15: batched physical deletion, Lindén-Jonsson style. Once too
                // many logically-deleted nodes have piled up at the front, take head straight
                // past the whole dead prefix.
                //
                // A plain store (not a CAS) is both what the paper does and safe to do here:
                // every other mutator that could touch head.next — insert's CAS at the front of
                // the list, and the unlink CASes inside search / searchDelete / searchPhysDel -
                // requires a LOGDEL-clear expected stamp on head.next, so any of them racing a
                // LOGDEL'd head simply fails and retries. On top of that, search()'s own guard
                // (isRightMovingOrLeftLogdel) means no insert can ever complete using a
                // LOGDEL'd node as its left neighbor, so nothing could have spliced itself into
                // the prefix being collapsed here. Combined with the fact that only one thread
                // ever runs delete-min at a time, no concurrent writer can be racing this store.
                head.next().set(newHead, LOGDEL_BIT);
            }

            return newHead;
        }
    }

    /**
     * Ported from {@code L-DeleteMaxP} (Alg. 4 lines 1-13, paper p.12). Removes and returns the
     * node tagged {@code tid} with the largest key currently in the list - this is used by the
     * slowest insert path to make room for a new node before pushing it in. Returns {@code null}
     * if {@code tid} currently has no node in the list.
     *
     * <p>Correctness here depends on the caller-enforced per-tid mutual exclusion documented on
     * {@link LeaderLayer}, not on any single Java thread permanently owning {@code tid} - {@code
     * Pipq}'s coordinator/promotion path can also write {@code maxPerTid[tid]} on {@code tid}'s
     * behalf (see {@link #updateLargestByThread}). {@code startLeadLargest} is captured once, up
     * front, and reused across every {@code searchDelete} retry — this mirrors the paper's fixed
     * {@code start_lead_largest} local, which is likewise reused across the {@code repeat} loop
     * at line 3.</p>
     */
    @Override
    public Node<V> deleteMaxByThread(int tid) {
        Node<V> startLeadLargest = maxPerTid[tid];
        if (startLeadLargest == null) {
            return null;
        }
        return claimAndUnlinkMax(tid, startLeadLargest);
    }

    /**
     * A fused {@code L-Insert} + {@code L-DeleteMaxP}, ported from {@code harris_insert_and_move}
     * / {@code harris_search_ins_move} in the paper's C++ implementation ({@code
     * microbench/harris.cc:242-439}). This combined operation isn't in the Alg. 4/8
     * pseudocode, which presents insert and evict as two separate calls. Here, {@code node} is
     * inserted and then {@code tid}'s worst node is evicted, both as one operation. Fusing them
     * closes a window that would otherwise be open if the caller instead made two separate calls
     * (insert, then {@link #deleteMaxByThread}): a concurrent {@code deleteMin} could drain {@code
     * tid}'s nodes in the gap between those two calls, leaving nothing for the evict to find.
     * <p>
     * This returns {@code null} instead of throwing in either of two
     * cases: {@code tid} had no node in the list before this call (nothing to evict), or a
     * concurrent {@code deleteMin} removed all of {@code tid}'s nodes before the evict phase ran.
     * Both match the paper's own handling in {@code harris_insert_and_move}, which likewise
     * returns EMPTY when {@code harris_search_ins_move} can't find a target.</p>
     */
    @Override
    public Node<V> insertAndDeleteMaxByThread(Node<V> node, int tid) {
        Node<V> startLeadLargest = maxPerTid[tid]; // starting_last_ptr in harris_insert_and_move: snapshot taken before the insert.
        insert(node);

        if (startLeadLargest == null) {
            return null; // tid had nothing to evict.
        }
        return claimAndUnlinkMax(tid, startLeadLargest);
    }

    /**
     * Shared claim-and-unlink loop used by both {@link #deleteMaxByThread} and {@link
     * #insertAndDeleteMaxByThread}: repeatedly runs {@code searchDelete} toward {@code
     * startLeadLargest}, claims the node it finds by setting the {@code MOVING} bit, and unlinks
     * it. Returns {@code null} if a {@code searchDelete} walk can't find a live window for {@code
     * tid} at all — for example, because a concurrent {@code deleteMin} removed every one of
     * {@code tid}'s nodes since the caller took its snapshot — instead of dereferencing a {@code
     * null} window, which used to cause a {@code NullPointerException} before this check existed.
     */
    private Node<V> claimAndUnlinkMax(int tid, Node<V> startLeadLargest) {
        Node<V> lNode;
        Node<V> rNode;
        Node<V> rNodeNextRef;
        while (true) {
            Window<V> w = searchDelete(tid, startLeadLargest);
            lNode = w.left();
            rNode = w.right();

            if (lNode == null || rNode == null) {
                return null; // tid's nodes were fully drained while this method was running.
            }

            int[] rNextStamp = new int[1];
            rNodeNextRef = rNode.next().get(rNextStamp);
            if (!isMarked(rNextStamp[0])
                    && rNode.next().compareAndSet(rNodeNextRef, rNodeNextRef, rNextStamp[0], rNextStamp[0] | MOVING_BIT)) {
                break;
            }
            // rNode was already claimed by someone else (moving or logically deleted), or we
            // lost the CAS race — either way, retry the whole searchDelete to find the current
            // target.
        }

        size.decrementAndGet();

        int[] lNextStamp = new int[1];
        lNode.next().get(lNextStamp);
        int expectedLStamp = lNextStamp[0] & ~LOGDEL_BIT;
        if (!lNode.next().compareAndSet(rNode, rNodeNextRef, expectedLStamp, expectedLStamp)) {
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

    // ============== Utility ==============

    /**
     * Ported from {@code harris_search} ({@code harris.cc:293-332}). Finds the window {@code
     * (left, right)} such that {@code left < target <= right} in list order, physically
     * unlinking any logically-deleted nodes it passes along the way.
     *
     * <p>{@code left} is only ever recorded at a non-moving node, so a concurrent {@code
     * deleteMaxByThread}-style move can never race with the unlink CAS performed below.</p>
     */
    Window<V> search(Node<V> target) {
        searchAgain:
        while (true) {
            Node<V> left = head;
            int[] leftNextStamp = new int[1];
            Node<V> leftNext = head.next().get(leftNextStamp);
            leftNextStamp[0] &= ~LOGDEL_BIT; // Note: removes logdel bit off the copy of the stamp

            Node<V> x = head;
            int[] xNextStamp = new int[1];
            Node<V> xNext = x.next().get(xNextStamp);

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
                xNext = x.next().get(xNextStamp);
            } while (Node.compare(x, target) < 0 || (xNextStamp[0] & MOVING_BIT) != 0 || prevLogdel);
            // Keep walking while: x isn't big enough yet, OR
            // x is itself mid-eviction (moving), OR
            // x was logically deleted. In all three cases x is unfit to be r_node.

            Node<V> right = x;

            // no nodes in between -> only check freshness
            if (leftNext == right) {
                if (isRightMovingOrLeftLogdel(left, right)) {
                    continue searchAgain;
                }
                return new Window<>(left, right);
            }

            // otherwise physically unlink in between nodes, and check freshness
            if (left.next().compareAndSet(leftNext, right, leftNextStamp[0], leftNextStamp[0])) {
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
     * Ported from {@code searchDelete} (Alg. 4 lines 14-47, paper p.12) - the by-thread variant
     * of {@link #search}. It walks from {@code head}, tracking the running non-moving
     * predecessor candidate exactly as {@code search()} does with {@code left}/{@code leftNext}
     * (here named {@code curLNode}/{@code curLNodeNext}/{@code curLNodeNextStamp}). On top of
     * that, it remembers the most recently seen node tagged with {@code tid} as {@code rNode}
     * (along with that node's own non-moving predecessor, snapshotted as {@code lNode}/{@code
     * lNodeNext}/{@code lNodeNextStamp}) until the walk reaches {@code startLeadLargest} — the
     * node {@link #deleteMaxByThread} wants removed. {@code newLeadLargest} accumulates the
     * previous {@code tid}-tagged node seen before {@code rNode}, i.e. what will become {@code
     * tid}'s new largest node once {@code rNode} is removed; this value is written into {@code
     * maxPerTid[tid]} before the method returns, mirroring the paper's {@code lead_largest ←
     * new_lead_largest} (line 35).
     *
     * <p>The write to {@code maxPerTid[tid]} here relies on the same per-tid mutual exclusion,
     * documented on {@link LeaderLayer}, that {@link #updateLargestByThread} relies on. It's
     * safe because the caller — always {@code Pipq}, holding {@code workerHeaps[tid]}'s lock —
     * serializes all access to this slot, not because any single Java thread owns {@code
     * tid}.</p>
     */
    Window<V> searchDelete(int tid, Node<V> startLeadLargest) {
        while (true) {
            Node<V> curLNode = head;
            int[] curLNodeNextStamp = new int[1];
            Node<V> curLNodeNext = head.next().get(curLNodeNextStamp);
            curLNodeNextStamp[0] &= ~LOGDEL_BIT;

            Node<V> lNode = null;
            Node<V> lNodeNext = null;
            int lNodeNextStamp = 0;
            Node<V> rNode = null;
            Node<V> newLeadLargest = null;

            Node<V> x = head;
            int[] xNextStamp = new int[1];
            Node<V> xNext = x.next().get(xNextStamp);

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
                xNext = x.next().get(xNextStamp);
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

            if (lNode.next().compareAndSet(lNodeNext, rNode, lNodeNextStamp, lNodeNextStamp)) {
                return new Window<>(lNode, rNode);
            }
            // CAS lost the race (l_node's stamp/reference changed) -> retry from head.
        }
    }

    /**
     * Ported from {@code searchPhysDel} (Alg. 4 lines 48-73, paper p.12). Locates a stable
     * (non-moving, not logically deleted) predecessor for {@code searchNode} and physically
     * unlinks it. This is the fallback path used by {@link #claimAndUnlinkMax} — the shared
     * helper behind both {@link #deleteMaxByThread} and {@link #insertAndDeleteMaxByThread} -
     * when its direct unlink CAS (on {@code lNode}/{@code rNode}) loses a race. If {@code
     * searchNode} can no longer be found in the list, another thread has already physically
     * unlinked it, so this method does nothing — matching the paper's "search_node has been
     * removed by another thread" case.
     */
    void searchPhysDel(Node<V> searchNode) {
        while (true) {
            Node<V> lNode = head;
            int[] lNodeNextStamp = new int[1];
            Node<V> lNodeNext = head.next().get(lNodeNextStamp);
            lNodeNextStamp[0] &= ~LOGDEL_BIT;

            boolean found = false;
            boolean prevLogdel;

            Node<V> x = head;
            int[] xNextStamp = new int[1];
            Node<V> xNext = x.next().get(xNextStamp);

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
                xNext = x.next().get(xNextStamp);
            } while (!(found && (xNextStamp[0] & MOVING_BIT) == 0 && !prevLogdel));

            if (!found) {
                return;
            }

            Node<V> rNode = x;

            if (lNode.next().compareAndSet(lNodeNext, rNode, lNodeNextStamp[0], lNodeNextStamp[0])) {
                return;
            }
            // CAS lost the race -> retry from head.
        }
    }
}
