import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SortedLinkedListLeaderTest {
    @Test
    void insertAndDeleteMinReturnNodesInPriorityOrder() {
        SortedLinkedListLeader<String> leader = new SortedLinkedListLeader<>();
        leader.insert(new Node<>(7, "seven", 0, 0));
        leader.insert(new Node<>(2, "two", 1, 1));
        leader.insert(new Node<>(5, "five", 0, 2));

        assertTrue(leader.validateSorted());
        assertEquals(2, leader.deleteMin().key());
        assertEquals(5, leader.deleteMin().key());
        assertEquals(7, leader.deleteMin().key());
        assertNull(leader.deleteMin());
    }

    @Test
    void deleteMaxByThreadRemovesOnlyLargestNodeForRequestedTid() {
        SortedLinkedListLeader<String> leader = new SortedLinkedListLeader<>();
        leader.insert(new Node<>(5, "tid0-small", 0, 0));
        leader.insert(new Node<>(99, "tid1-large", 1, 1));
        leader.insert(new Node<>(10, "tid0-large", 0, 2));

        Node<String> removed = leader.deleteMaxByThread(0);

        assertEquals(10, removed.key());
        assertEquals("tid0-large", removed.value());
        List<Long> remainingKeys = leader.snapshot().stream()
                .map(Node::key)
                .collect(Collectors.toList());
        assertEquals(Arrays.asList(5L, 99L), remainingKeys);
        assertEquals(99, leader.maxByThread(1).key());
        assertNull(leader.deleteMaxByThread(2));
    }
}
