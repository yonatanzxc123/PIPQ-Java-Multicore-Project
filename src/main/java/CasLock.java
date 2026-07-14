import java.util.concurrent.atomic.AtomicInteger;

/**
 * CAS-based mutual exclusion lock matching the paper's {@code lock_val} scheme: a version
 * counter where an even value means unlocked and an odd value means locked. Acquiring CASes
 * the counter from even {@code v} to {@code v + 1}; releasing bumps it again ({@code v + 1} to
 * {@code v + 2}, back to even). Not reentrant.
 */
public final class CasLock {
    private final AtomicInteger lockVal = new AtomicInteger(0);

    public boolean tryLock() {
        int v = lockVal.get();
        if ((v & 1) != 0) {
            return false;
        }
        return lockVal.compareAndSet(v, v + 1);
    }

    public void lock() {
        while (!tryLock()) {
            // busy-wait
        }
    }

    public void unlock() {
        lockVal.incrementAndGet();
    }

    public boolean isLocked() {
        return (lockVal.get() & 1) != 0;
    }
}
