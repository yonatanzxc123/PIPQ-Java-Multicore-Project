# PIPQ Java Multicore Project

This project implements the original PIPQ-style Java baseline from "PIPQ: Strict Insert-Optimized Concurrent Priority Queue" and an experimental heap-backed leader-layer variation.

## Prerequisites

- JDK 8 or newer
- Maven

## Run Tests

```bash
mvn test
```

## Build for the course server (default — no async logger)

The default `Pipq` logger is `NoopLogger` — it does nothing and needs no external jars, so this is the build to upload.

```bash
mvn -q compile
```

This produces flat `.class` files in `target/classes`. Upload **all** of them **except `ConcurrentLogger.class`** (and skip `log4j2.xml`). Do not add the `.setLogger(...)` line described below anywhere in code you upload — without it, `Pipq` never touches log4j and needs nothing beyond the JDK.

If compiling manually instead of using Maven, exclude `ConcurrentLogger.java` — it imports log4j2 classes that plain `javac` won't have on its classpath:

```bash
javac --release=8 -d server-classes $(ls src/main/java/*.java | grep -v ConcurrentLogger.java)
```

Upload only the resulting `.class` files from `server-classes` (again, all except `ConcurrentLogger.class`) to the shared OneDrive folder.

## Build/run locally with the async logger

For local debugging you can trace every `Pipq` operation through log4j2's async logger (backed by an LMAX Disruptor ring buffer) instead of the no-op default. This requires `log4j-api`, `log4j-core`, and `disruptor`, all already declared in `pom.xml` — use Maven, not plain `javac`, since `ConcurrentLogger` won't compile without those jars on the classpath.

To turn it on, add one line right after constructing your `Pipq`:

```java
Pipq<Integer> pipq = new Pipq<>(numberOfThreads, cntrMin, cntrMax);
pipq.setLogger(new ConcurrentLogger());
```

Logger configuration lives in `src/main/resources/log4j2.xml`; trace output is written to `target/pipq-debug.log`.

**This build is local-only.** The course server has no log4j or disruptor jars available, so any upload that both (a) contains `ConcurrentLogger.class` and (b) actually calls `setLogger(new ConcurrentLogger())` will fail on the server with `NoClassDefFoundError`. Before uploading to the server:
- remove the `pipq.setLogger(new ConcurrentLogger());` line from any code path that runs there, and
- exclude `ConcurrentLogger.class` from the uploaded `.class` files (see previous section).

## Class Map

- `Node` stores the key, value, and original inserting thread id.
- `WorkerHeap` is the worker level: one custom array-backed binary min-heap per logical thread, protected by its own lock.
- `LeaderLinkedList` is the original paper-style leader level: a shared sorted linked list using Java CAS/stamped references.
- `IndexedHeapLeader` is the proposed variation: a global indexed min-heap plus per-thread max-heaps.
- `Pipq` wires the worker and leader levels together with `CNTR_MIN`, `CNTR_MAX`, and per-thread leader counters. Its default constructors use `LeaderLinkedList`; `Pipq.withIndexedHeapLeader(...)` creates the heap variation.
- `PipqStats` exposes instrumentation for insert paths, leader operations, worker heap operations, and delete-min calls.
- `PipqLogger` is the logging hook interface used by `Pipq`.
- `NoopLogger` is the default, dependency-free logger implementation — safe for the course server.
- `ConcurrentLogger` is the log4j2-backed async logger implementation — local debugging only, see above.
- `MppRunner` benchmarks both `OG_PIPQ` and `HEAP_PIPQ`.

## Scope

The default `Pipq` behavior remains the original sorted linked-list leader baseline. The heap leader is added as a separate variation for comparison. The full NUMA coordinator hierarchy from the paper is still simplified to one coordinator lock in this Java project.

The heap leader is not lock-free: it uses one internal lock and custom array-backed heaps to preserve the same abstract leader operations.
