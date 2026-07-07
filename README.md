# PIPQ Baseline

This project implements a Java baseline of the original PIPQ design from "PIPQ: Strict Insert-Optimized Concurrent Priority Queue".

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

- `Node` stores the key, value, original inserting thread id, and sequence number used to break ties.
- `WorkerHeap` is the worker level: one custom array-backed binary min-heap per logical thread, protected by its own lock.
- `SortedLinkedListLeader` is the original paper's leader level: a shared sorted linked list, not the later heap-based variation.
- `Pipq` wires the worker and leader levels together with `CNTR_MIN`, `CNTR_MAX`, and per-thread leader counters.
- `PipqStats` exposes instrumentation for insert paths, leader operations, worker heap operations, and delete-min calls.

## Baseline Scope

This is intentionally a faithful algorithmic Java baseline, not a low-level C++ lock-free port. It uses Java locks instead of pointer-bit marking, bit stealing, and lock-free list CAS loops. The NUMA coordinator and combining mechanism from the paper is simplified to a single delete-min lock, while preserving the core invariant: for each thread id, all leader nodes owned by that id have keys no larger than that worker heap's minimum key.

The heap-based leader-layer variation is not implemented in this baseline.
