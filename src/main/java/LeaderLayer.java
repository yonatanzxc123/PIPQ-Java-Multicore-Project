import java.util.List;

/**
 * Concurrency contract (applies to every implementation, list or heap):
 *
 * <p>For a given {@code tid}, calls to {@link #insert(Node)} (where {@code node.tid() ==
 * tid}), {@link #maxByThread(int)}, and {@link #deleteMaxByThread(int)} must be externally
 * serialized per {@code tid} by the caller — i.e. the caller must hold that tid's own
 * mutual-exclusion domain (in {@code Pipq}, {@code workerHeaps[tid]}'s lock) for the duration
 * of the call. Implementations are free to track per-tid leader-side state (e.g. a
 * {@code largest-in-leader} pointer) as plain, non-synchronized fields precisely because of
 * this externally-enforced single-domain access — not because any one Java thread "owns"
 * {@code tid} forever. This mirrors the original paper's own discipline: both a thread's own
 * insert and a coordinator's {@code Help-Upsert}/promotion on that thread's behalf acquire
 * {@code worker_heap_locks[tid]} before touching {@code lead_largest_tid}.</p>
 *
 * <p>{@link #deleteMin()} is exempt — it never touches per-tid leader-side state (see
 * implementation notes).</p>
 */
public interface LeaderLayer<V> {
    void insert(Node<V> node);

    Node<V> deleteMin();

    Node<V> deleteMaxByThread(int tid);

    /**
     * Fused {@code L-Insert} + {@code L-DeleteMaxP} (paper's {@code harris_insert_and_move}).
     * Inserts {@code node} (tagged {@code tid}), then removes and returns {@code tid}'s worst
     * (largest-key) node, or {@code null} (EMPTY) if {@code tid} has nothing evictable — e.g. a
     * concurrent {@code deleteMin} drained {@code tid} mid-operation. Fusing the two steps (rather
     * than calling {@link #insert} then {@link #deleteMaxByThread} separately) closes the window
     * in which a concurrent {@code deleteMin} could observe {@code tid} transiently over {@code
     * CNTR_MAX} or race the evict target — the same per-tid mutual-exclusion contract documented
     * above applies.
     */
    Node<V> insertAndDeleteMaxByThread(Node<V> node, int tid);

    Node<V> maxByThread(int tid);

    int size();
}
