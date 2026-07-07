import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PipqSequentialTest {
    @Test
    void deleteMinReturnsGlobalPriorityOrderAcrossThreads() {
        Pipq<String> pipq = new Pipq<>(2, 2, 3);
        pipq.insert(0, 10, "a10");
        pipq.insert(1, 20, "b20");
        pipq.insert(0, 30, "a30");
        pipq.insert(1, 40, "b40");
        pipq.insert(0, 50, "a50");
        pipq.insert(1, 60, "b60");
        pipq.insert(0, 70, "a70");
        pipq.insert(1, 80, "b80");

        List<Long> keys = new ArrayList<>();
        Optional<Node<String>> node;
        while ((node = pipq.deleteMin(0)).isPresent()) {
            keys.add(node.get().key());
        }

        assertEquals(Arrays.asList(10L, 20L, 30L, 40L, 50L, 60L, 70L, 80L), keys);
        assertTrue(pipq.validateInvariant());
        assertTrue(pipq.validateLeaderCounters());
    }

    @Test
    void cntrMaxForcesSlowestPathAndMovesWorstLeaderNodeDown() {
        Pipq<String> pipq = new Pipq<>(1, 2, 2);
        pipq.insert(0, 10, "ten");
        pipq.insert(0, 20, "twenty");

        pipq.insert(0, 5, "five");

        assertEquals(2, pipq.leaderCounter(0));
        assertEquals(1, pipq.workerSize(0));
        assertEquals(20, pipq.workerMin(0).orElseThrow(AssertionError::new).key());
        List<Long> leaderKeys = pipq.leaderSnapshot().stream()
                .map(Node::key)
                .collect(Collectors.toList());
        assertEquals(Arrays.asList(5L, 10L), leaderKeys);
        assertEquals(1, pipq.stats().slowestPathInserts());
        assertEquals(1, pipq.stats().leaderDeleteMaxByThread());
        assertTrue(pipq.validateInvariant());
    }

    @Test
    void deleteMinPromotesFromWorkerWhenOwnerCounterDropsBelowTwo() {
        Pipq<String> pipq = new Pipq<>(1, 2, 2);
        pipq.insert(0, 10, "ten");
        pipq.insert(0, 20, "twenty");
        pipq.insert(0, 5, "five");

        Node<String> removed = pipq.deleteMin(0).orElseThrow(AssertionError::new);

        assertEquals(5, removed.key());
        assertEquals(2, pipq.leaderCounter(0));
        assertEquals(0, pipq.workerSize(0));
        List<Long> leaderKeys = pipq.leaderSnapshot().stream()
                .map(Node::key)
                .collect(Collectors.toList());
        assertEquals(Arrays.asList(10L, 20L), leaderKeys);
        assertEquals(1, pipq.stats().workerHeapDeleteMinPromotions());
        assertTrue(pipq.validateInvariant());
    }
}
