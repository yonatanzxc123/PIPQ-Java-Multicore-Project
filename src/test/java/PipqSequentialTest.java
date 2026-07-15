import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PipqSequentialTest {
    @Test
    void deleteMinReturnsGlobalPriorityOrderAcrossThreads() {
        Pipq<String> pipq = new Pipq<>(2, 2, 3);
        pipq.insert(10, "a10", 0);
        pipq.insert(20, "b20", 1);
        pipq.insert(30, "a30", 0);
        pipq.insert(40, "b40", 1);
        pipq.insert(50, "a50", 0);
        pipq.insert(60, "b60", 1);
        pipq.insert(70, "a70", 0);
        pipq.insert(80, "b80", 1);

        List<Long> keys = new ArrayList<>();
        Optional<Node<String>> node;
        while ((node = pipq.deleteMin(0)).isPresent()) {
            keys.add(node.get().key());
        }

        assertEquals(Arrays.asList(10L, 20L, 30L, 40L, 50L, 60L, 70L, 80L), keys);
    }

    @Test
    void cntrMaxForcesSlowestPathAndMovesWorstLeaderNodeDown() {
        Pipq<String> pipq = new Pipq<>(1, 2, 2);
        pipq.insert(10, "ten", 0);
        pipq.insert(20, "twenty", 0);

        pipq.insert(5, "five", 0);

        assertEquals(2, pipq.leaderCounter(0));
        assertEquals(2, pipq.leaderSize());
        assertEquals(1, pipq.workerSize(0));
        assertEquals(20, pipq.workerMin(0).orElseThrow(AssertionError::new).key());
        assertEquals(1, pipq.stats().slowestPathInserts());
        assertEquals(1, pipq.stats().leaderDeleteMaxByThread());
    }

    @Test
    void deleteMinPromotesFromWorkerWhenOwnerCounterDropsBelowTwo() {
        Pipq<String> pipq = new Pipq<>(1, 2, 2);
        pipq.insert(10, "ten", 0);
        pipq.insert(20, "twenty", 0);
        pipq.insert(5, "five", 0);

        Node<String> removed = pipq.deleteMin(0).orElseThrow(AssertionError::new);

        assertEquals(5, removed.key());
        assertEquals(2, pipq.leaderCounter(0));
        assertEquals(2, pipq.leaderSize());
        assertEquals(0, pipq.workerSize(0));
        assertEquals(1, pipq.stats().workerHeapDeleteMinPromotions());
    }
}
