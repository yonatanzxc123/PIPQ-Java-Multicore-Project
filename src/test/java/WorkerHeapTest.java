import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class WorkerHeapTest {
    @Test
    void insertAndDeleteMinReturnNodesInPriorityOrder() {
        WorkerHeap<String> heap = new WorkerHeap<>(2);
        heap.insert(new Node<>(5, "five", 0));
        heap.insert(new Node<>(1, "one", 0));
        heap.insert(new Node<>(3, "three", 0));
        heap.insert(new Node<>(1, "one-again", 0));

        List<Long> keys = new ArrayList<>();
        Set<String> tiedValues = new HashSet<>();
        Node<String> node;
        int popped = 0;
        while ((node = heap.deleteMin()) != null) {
            keys.add(node.key());
            if (popped < 2) {
                tiedValues.add(node.value());
            }
            popped++;
        }

        // The two nodes with key 1 compare as equal, and the paper leaves the order among
        // such ties undefined. So this test only asserts the overall key order below -- it
        // does not check which of "one"/"one-again" comes out first.
        assertEquals(Arrays.asList(1L, 1L, 3L, 5L), keys);
        assertEquals(new HashSet<>(Arrays.asList("one", "one-again")), tiedValues);
        assertNull(heap.deleteMin());
    }
}
