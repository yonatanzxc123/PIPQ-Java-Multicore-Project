import java.util.Optional;

/**
 * Server entry point required by the course multicore runner.
 *
 * <p>The real project tests live under Maven/JUnit. This class is intentionally
 * small: it performs a smoke run of the baseline PIPQ and prints a clear final
 * marker so the server output file shows that execution completed.</p>
 */
public final class MppRunner {
    private MppRunner() {
    }

    public static void main(String[] args) {
        long startNanos = System.nanoTime();

        Pipq<Integer> queue = new Pipq<Integer>(4, 2, 4);
        for (int tid = 0; tid < queue.threadCount(); tid++) {
            for (int i = 0; i < 100; i++) {
                long key = (long) tid * 1000L + i;
                queue.insert(tid, key, Integer.valueOf(i));
            }
        }

        int removed = 0;
        boolean sorted = true;
        long previous = Long.MIN_VALUE;
        Optional<Node<Integer>> current;
        while ((current = queue.deleteMin(0)).isPresent()) {
            long key = current.get().key();
            if (key < previous) {
                sorted = false;
            }
            previous = key;
            removed++;
        }

        long elapsedNanos = System.nanoTime() - startNanos;

        System.out.println("PIPQ baseline smoke run");
        System.out.println("removed=" + removed);
        System.out.println("sorted=" + sorted);
        System.out.println("elapsedNanos=" + elapsedNanos);
        System.out.println("DONE");
    }
}
