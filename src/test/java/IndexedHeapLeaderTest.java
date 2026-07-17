import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IndexedHeapLeaderTest {
    @Test
    void deleteMinReturnsNodesInNondecreasingKeyOrder() {
        IndexedHeapLeader<String> leader = new IndexedHeapLeader<String>(3);
        leader.insert(new Node<String>(7, "seven", 0));
        leader.insert(new Node<String>(2, "two", 1));
        leader.insert(new Node<String>(5, "five", 0));
        leader.insert(new Node<String>(1, "one", 2));
        leader.insert(new Node<String>(9, "nine", 1));

        List<Long> keys = new ArrayList<Long>();
        Node<String> node;
        while ((node = leader.deleteMin()) != null) {
            keys.add(node.key());
        }

        assertEquals(Arrays.asList(1L, 2L, 5L, 7L, 9L), keys);
        assertEquals(0, leader.size());
        assertTrue(leader.validateInternal());
    }

    @Test
    void deleteMaxByThreadRemovesOnlyThatThreadsWorstNode() {
        IndexedHeapLeader<String> leader = new IndexedHeapLeader<String>(3);
        leader.insert(new Node<String>(5, "tid0-small", 0));
        leader.insert(new Node<String>(20, "tid1-large", 1));
        leader.insert(new Node<String>(10, "tid0-large", 0));
        leader.insert(new Node<String>(3, "tid1-small", 1));

        Node<String> removed = leader.deleteMaxByThread(0);

        assertEquals(10, removed.key());
        assertEquals("tid0-large", removed.value());
        assertEquals(1, leader.sizeByThread(0));
        assertEquals(2, leader.sizeByThread(1));
        assertEquals(5, leader.maxByThread(0).key());
        assertEquals(20, leader.maxByThread(1).key());
        assertTrue(leader.validateInternal());
    }

    @Test
    void mixedOperationsKeepThreadIndexesConsistent() {
        IndexedHeapLeader<String> leader = new IndexedHeapLeader<String>(2);
        leader.insert(new Node<String>(10, "a", 0));
        leader.insert(new Node<String>(30, "b", 0));
        leader.insert(new Node<String>(20, "c", 1));

        assertEquals(10, leader.deleteMin().key());
        assertEquals(1, leader.sizeByThread(0));
        assertEquals(30, leader.maxByThread(0).key());

        leader.insert(new Node<String>(5, "d", 0));
        leader.insert(new Node<String>(40, "e", 1));

        assertEquals(40, leader.deleteMaxByThread(1).key());
        assertEquals(5, leader.deleteMin().key());
        assertEquals(30, leader.maxByThread(0).key());
        assertEquals(1, leader.sizeByThread(0));
        assertEquals(1, leader.sizeByThread(1));
        assertTrue(leader.validateInternal());
    }

    @Test
    void linkedListAndHeapLeaderAgreeOnDeterministicOperations() {
        LeaderLayer<String> listLeader = new LeaderLinkedList<String>(3);
        IndexedHeapLeader<String> heapLeader = new IndexedHeapLeader<String>(3);

        insertBoth(listLeader, heapLeader, 10, "a", 0);
        insertBoth(listLeader, heapLeader, 4, "b", 1);
        insertBoth(listLeader, heapLeader, 7, "c", 0);
        insertBoth(listLeader, heapLeader, 2, "d", 2);
        insertBoth(listLeader, heapLeader, 20, "e", 1);

        assertSameNodeShape(listLeader.deleteMin(), heapLeader.deleteMin());
        assertSameNodeShape(listLeader.deleteMaxByThread(1), heapLeader.deleteMaxByThread(1));

        insertBoth(listLeader, heapLeader, 1, "f", 1);
        assertSameNodeShape(listLeader.maxByThread(0), heapLeader.maxByThread(0));

        Node<String> listNode;
        while ((listNode = listLeader.deleteMin()) != null) {
            assertSameNodeShape(listNode, heapLeader.deleteMin());
        }
        assertNull(heapLeader.deleteMin());
        assertTrue(heapLeader.validateInternal());
    }

    private static void insertBoth(LeaderLayer<String> listLeader, IndexedHeapLeader<String> heapLeader,
                                   long key, String value, int tid) {
        listLeader.insert(new Node<String>(key, value, tid));
        heapLeader.insert(new Node<String>(key, value, tid));
    }

    private static void assertSameNodeShape(Node<String> expected, Node<String> actual) {
        assertEquals(expected.key(), actual.key());
        assertEquals(expected.tid(), actual.tid());
        assertEquals(expected.value(), actual.value());
    }
}
