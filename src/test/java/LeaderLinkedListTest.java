import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LeaderLinkedListTest {
    @Test
    void insertKeepsListSortedAscending() {
        LeaderLinkedList<String> leader = new LeaderLinkedList<>(4);
        long[] keys = {7, 2, 9, 5, 1, 8, 3, 6, 4, 0};

        for (long key : keys) {
            leader.insert(new Node<>(key, "v" + key, 0));
        }

        assertEquals(keys.length, leader.size());

        List<Long> drained = drainAll(leader);
        for (int i = 0; i < drained.size(); i++) {
            assertEquals((long) i, drained.get(i));
        }
        assertEquals(0, leader.size());
    }

    @Test
    void searchReturnsWindowStraddlingTarget() {
        LeaderLinkedList<String> leader = new LeaderLinkedList<>(1);
        leader.insert(new Node<>(10, "ten", 0));
        leader.insert(new Node<>(20, "twenty", 0));
        leader.insert(new Node<>(30, "thirty", 0));

        LeaderLinkedList.Window<String> below = leader.search(new Node<>(5, null, 0));
        assertEquals(10, below.right().key());

        LeaderLinkedList.Window<String> between = leader.search(new Node<>(15, null, 0));
        assertEquals(10, between.left().key());
        assertEquals(20, between.right().key());

        LeaderLinkedList.Window<String> exact = leader.search(new Node<>(20, null, 0));
        assertEquals(10, exact.left().key());
        assertEquals(20, exact.right().key());

        LeaderLinkedList.Window<String> above = leader.search(new Node<>(99, null, 0));
        assertEquals(30, above.left().key());
    }

    @Test
    void insertTracksLargestPerThread() {
        LeaderLinkedList<String> leader = new LeaderLinkedList<>(2);
        leader.insert(new Node<>(5, "tid0-small", 0));
        leader.insert(new Node<>(50, "tid1-large", 1));
        leader.insert(new Node<>(10, "tid0-large", 0));
        leader.insert(new Node<>(3, "tid1-small", 1));

        assertEquals(10, leader.maxByThread(0).key());
        assertEquals(50, leader.maxByThread(1).key());
    }

    @Test
    void deleteMaxByThreadRemovesOnlyThatThreadsWorstNode() {
        LeaderLinkedList<String> leader = new LeaderLinkedList<>(2);
        leader.insert(new Node<>(5, "tid0-a", 0));
        leader.insert(new Node<>(15, "tid0-b", 0));
        leader.insert(new Node<>(50, "tid1-large", 1));
        leader.insert(new Node<>(3, "tid1-small", 1));

        Node<String> removed = leader.deleteMaxByThread(0);

        assertEquals(15, removed.key());
        assertEquals(3, leader.size());
        assertEquals(5, leader.maxByThread(0).key());
        assertEquals(50, leader.maxByThread(1).key());
    }

    @Test
    void deleteMaxByThreadOnEmptyThreadReturnsNull() {
        LeaderLinkedList<String> leader = new LeaderLinkedList<>(2);
        leader.insert(new Node<>(5, "tid0", 0));

        assertNull(leader.deleteMaxByThread(1));
        assertEquals(1, leader.size());
    }

    @Test
    void duplicateKeysAreBothPresentAndRemovable() {
        LeaderLinkedList<String> leader = new LeaderLinkedList<>(2);
        leader.insert(new Node<>(5, "first", 0));
        leader.insert(new Node<>(5, "second", 1));

        assertEquals(2, leader.size());
        List<Long> drained = drainAll(leader);
        assertEquals(Arrays.asList(5L, 5L), drained);
    }

    @Test
    void deleteMinOnEmptyListReturnsNull() {
        LeaderLinkedList<String> leader = new LeaderLinkedList<>(1);
        assertNull(leader.deleteMin());
    }

    @Test
    void manyNodesSurviveBatchedPhysicalDeletion() {
        // Exceeds MAX_OFFSET (32) to exercise deleteMin()'s batched physical-deletion path.
        LeaderLinkedList<String> leader = new LeaderLinkedList<>(1);
        int count = 100;
        for (long key = 0; key < count; key++) {
            leader.insert(new Node<>(key, "v" + key, 0));
        }

        List<Long> drained = drainAll(leader);
        assertEquals(count, drained.size());
        for (int i = 0; i < drained.size(); i++) {
            assertEquals((long) i, drained.get(i));
        }
        assertTrue(leader.size() == 0);
    }

    private static List<Long> drainAll(LeaderLinkedList<String> leader) {
        List<Long> drained = new ArrayList<>();
        Node<String> node;
        while ((node = leader.deleteMin()) != null) {
            drained.add(node.key());
        }
        return drained;
    }
}
