import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class WorkerHeapTest {
    @Test
    void insertAndDeleteMinReturnNodesInPriorityOrder() {
        WorkerHeap<String> heap = new WorkerHeap<>(2);
        heap.insert(new Node<>(5, "five", 0, 0));
        heap.insert(new Node<>(1, "one", 0, 1));
        heap.insert(new Node<>(3, "three", 0, 2));
        heap.insert(new Node<>(1, "one-again", 0, 3));

        List<String> values = new ArrayList<>();
        Node<String> node;
        while ((node = heap.deleteMin()) != null) {
            values.add(node.value());
        }

        assertEquals(Arrays.asList("one", "one-again", "three", "five"), values);
        assertNull(heap.deleteMin());
    }
}
