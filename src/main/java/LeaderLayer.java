/**
 * Concurrency contract that every implementation of this interface (list-based or heap-based)
 * must follow.
 *
 * <p>For a given {@code tid}, calls to {@link #insert(Node)} (where {@code node.tid() ==
 * tid}), {@link #maxByThread(int)}, and {@link #deleteMaxByThread(int)} must be externally
 * serialized per {@code tid} by the caller. In practice this means the caller must hold that
 * tid's own mutual-exclusion lock (in {@code Pipq}, this is {@code workerHeaps[tid]}'s lock)
 * for the whole duration of the call. Because of this externally-enforced serialization,
 * implementations are free to track per-tid leader-side state — for example a "largest node
 * this thread has in the leader list" pointer — as a plain, non-synchronized field. That's
 * safe not because any one Java thread "owns" {@code tid} forever, but because only one thread
 * at a time is allowed to touch tid's state. This mirrors the original paper's own
 * discipline: both thread's own {@code insert} and {@code helpUpsert} or coordinator's
 * promotion on that thread's behalf acquire {@code worker_heap_locks[tid]} before touching
 * {@code lead_largest_tid}.</p>
 *
 * <p>{@link #deleteMin()} is exempt from this rule — it never touches any per-tid leader-side
 * state (see the implementation notes on {@code deleteMin} for why).</p>
 */
public interface LeaderLayer<V> {
    void insert(Node<V> node);

    Node<V> deleteMin();

    Node<V> deleteMaxByThread(int tid);

    /**
     * A fused {@code L-Insert} + {@code L-DeleteMaxP} (the paper's {@code
     * harris_insert_and_move}). Inserts {@code node} (tagged {@code tid}), then removes and
     * returns {@code tid}'s worst (largest-key) node — or {@code null} if {@code tid} has
     * nothing left to evict (for example because a concurrent {@code deleteMin} drained {@code
     * tid} mid-operation). Fusing the insert and the evict into one call, rather than calling
     * {@link #insert} and then {@link #deleteMaxByThread} separately, eliminates possibility in
     * which a concurrent {@code deleteMin} could otherwise observe {@code tid} briefly holding
     * more than {@code CNTR_MAX} nodes, or could race with the evict target. The same per-tid
     * mutual-exclusion contract documented above applies here too.
     */
    Node<V> insertAndDeleteMaxByThread(Node<V> node, int tid);

    Node<V> maxByThread(int tid);

    int size();
}
