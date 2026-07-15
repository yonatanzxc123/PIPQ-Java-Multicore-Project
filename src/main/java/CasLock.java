import java.util.concurrent.atomic.AtomicInteger;

/**
 * CAS-based mutual exclusion lock, matching the paper's {@code lock_val}: a single
 * counter where an even value means "unlocked" and an odd value means "locked". Acquiring the
 * lock does a compare-and-set from an even value {@code v} to {@code v + 1} (now odd, i.e.
 * locked). Releasing the lock bumps the counter again, from {@code v + 1} to {@code v + 2}
 * (back to even, i.e. unlocked). This lock is not reentrant.
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
            // Spin: keep retrying tryLock() until it succeeds.
        }
    }

    public void unlock() {
        lockVal.incrementAndGet();
    }

    public boolean isLocked() {
        return (lockVal.get() & 1) != 0;
    }
}
