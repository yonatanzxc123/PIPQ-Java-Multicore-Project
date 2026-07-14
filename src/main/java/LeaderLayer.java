import java.util.List;

public interface LeaderLayer<V> {
    void insert(Node<V> node);

    Node<V> deleteMin();

    Node<V> deleteMaxByThread(int tid);

    Node<V> maxByThread(int tid);

    int size();
}
