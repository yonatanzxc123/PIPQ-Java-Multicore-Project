import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LeaderLinkedListTest {
    @Test
    void insertKeepsListSortedAscending() {
        LeaderLinkedList<String> leader = new LeaderLinkedList<>(4);
        long[] keys = {7, 2, 9, 5, 1, 8, 3, 6, 4, 0};

        long sequence = 0;
        for (long key : keys) {
            leader.insert(new Node<>(key, "v" + key, 0, sequence++));
        }

        assertTrue(leader.validateSorted());
        assertEquals(keys.length, leader.size());

        List<Node<String>> snapshot = leader.snapshot();
        for (int i = 0; i < snapshot.size(); i++) {
            assertEquals((long) i, snapshot.get(i).key());
        }
    }

    @Test
    void searchReturnsWindowStraddlingTarget() {
        LeaderLinkedList<String> leader = new LeaderLinkedList<>(1);
        leader.insert(new Node<>(10, "ten", 0, 0));
        leader.insert(new Node<>(20, "twenty", 0, 1));
        leader.insert(new Node<>(30, "thirty", 0, 2));

        LeaderLinkedList.Window<String> below = leader.search(new Node<>(5, null, 0, 100));
        assertEquals(10, below.right().key());

        LeaderLinkedList.Window<String> between = leader.search(new Node<>(15, null, 0, 101));
        assertEquals(10, between.left().key());
        assertEquals(20, between.right().key());

        LeaderLinkedList.Window<String> exact = leader.search(new Node<>(20, null, 0, 102));
        assertEquals(10, exact.left().key());
        assertEquals(20, exact.right().key());

        LeaderLinkedList.Window<String> above = leader.search(new Node<>(99, null, 0, 103));
        assertEquals(30, above.left().key());
    }

    @Test
    void insertTracksLargestPerThread() {
        LeaderLinkedList<String> leader = new LeaderLinkedList<>(2);
        leader.insert(new Node<>(5, "tid0-small", 0, 0));
        leader.insert(new Node<>(50, "tid1-large", 1, 1));
        leader.insert(new Node<>(10, "tid0-large", 0, 2));
        leader.insert(new Node<>(3, "tid1-small", 1, 3));

        assertEquals(10, leader.maxByThread(0).key());
        assertEquals(50, leader.maxByThread(1).key());
    }

    @Test
    void duplicateKeysBreakTiesBySequence() {
        LeaderLinkedList<String> leader = new LeaderLinkedList<>(2);
        leader.insert(new Node<>(5, "first", 0, 0));
        leader.insert(new Node<>(5, "second", 1, 1));

        List<Node<String>> snapshot = leader.snapshot();
        assertEquals(2, snapshot.size());
        assertEquals("first", snapshot.get(0).value());
        assertEquals("second", snapshot.get(1).value());
    }
}
