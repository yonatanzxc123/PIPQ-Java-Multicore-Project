# PIPQ Java Multicore Project

This project implements the original PIPQ-style Java baseline from "PIPQ: Strict Insert-Optimized Concurrent Priority Queue" and an experimental heap-backed leader-layer variation.

## Run Tests

```bash
mvn test
```

## Course Server Compatibility

The course server expects compiled Java 8 `.class` files in one flat folder:

```bash
mvn test
```

After Maven succeeds, upload only the `.class` files from `target/classes`.

If compiling manually instead of using Maven:

```bash
javac --release=8 -d server-classes src/main/java/*.java
```

Upload only the `.class` files from `server-classes` to the shared OneDrive folder. Do not upload source files, Maven files, folders, text files, or temporary files. The server runs `MppRunner.main`, so [MppRunner.java](src/main/java/MppRunner.java) is the required entry point.

## Class Map

- `Node` stores the key, value, and original inserting thread id.
- `WorkerHeap` is the worker level: one custom array-backed binary min-heap per logical thread, protected by its own lock.
- `LeaderLinkedList` is the original paper-style leader level: a shared sorted linked list using Java CAS/stamped references.
- `IndexedHeapLeader` is the proposed variation: a global indexed min-heap plus per-thread max-heaps.
- `Pipq` wires the worker and leader levels together with `CNTR_MIN`, `CNTR_MAX`, and per-thread leader counters. Its default constructors use `LeaderLinkedList`; `Pipq.withIndexedHeapLeader(...)` creates the heap variation.
- `PipqStats` exposes instrumentation for insert paths, leader operations, worker heap operations, and delete-min calls.
- `MppRunner` benchmarks both `OG_PIPQ` and `HEAP_PIPQ`.

## Scope

The default `Pipq` behavior remains the original sorted linked-list leader baseline. The heap leader is added as a separate variation for comparison. The full NUMA coordinator hierarchy from the paper is still simplified to one coordinator lock in this Java project.

The heap leader is not lock-free: it uses one internal lock and custom array-backed heaps to preserve the same abstract leader operations.
