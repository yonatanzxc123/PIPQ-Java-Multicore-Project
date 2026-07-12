# PIPQ Java Baseline vs Original (C++) — Comparison Notes

Research notes comparing our Java baseline (`src/main/java/`) against the original PIPQ
C++ implementation (`../pipq/pq_impl/`), scoped to identify fidelity gaps that are
independent of NUMA, plus one candidate reuse of an existing course artifact for the
leader-level data structure. See `CLAUDE.md` for the project's decided scope (no NUMA,
heap-leader variant as the course "variation", **both leader-level implementations —
sorted list and heap — pursued lock-free**, per the "Lock-free leader level" section
there).

## 1. How NUMA actually works in the original — and how to legally ignore it

Two-tier locking in `try_compete_coordinator` (zone-local `t_compete_coord_lock`) +
`try_become_coordinator` (global `coord_lock`):

- Tier 1: threads on the same NUMA zone race for **zone-leader** role via a zone-local
  lock (cheap, NUMA-local cache line).
- Tier 2: zone-leaders race for **global coordinator** via one global lock (expensive,
  cross-NUMA cache line — this is the part worth avoiding contention on).
- The winner's `Coordinate()` only drains `announce_coord[]` slots for **its own zone**
  (`t_num_workers` = zone worker count, not global). Other zone-leaders keep
  spinning/helping until they eventually win the global lock themselves and drain their
  own zone.

The point of the whole scheme is to reduce how often threads touch the *remote* global
cache line — a NUMA-locality optimization layered on top of a generic combining
algorithm, not a different algorithm.

Key fact: with `NUMA_ZONES = 1`, tier-1 and tier-2 collapse into the same lock — the
algorithm degenerates exactly to a flat single-tier combining funnel. So the correct
"ignore NUMA" move is to **implement as if there is exactly 1 zone**, not to strip
combining out entirely. That gives the closest-to-original semantics with zero NUMA
code:

- One global `announce[]` array, sized to total threads (not per-zone).
- One CAS/lock-guarded coordinator role.
- `Coordinate()` drains **all** pending announce slots (the paper's own single-zone
  case would do exactly this).
- Waiting threads call `help_upsert()` in a spin loop instead of blocking — itself
  NUMA-independent, and the reason the design tolerates lock contention at all.

The Java baseline currently has **none of this** — `Pipq.deleteMin()` just takes
`deleteMinLock`, does one `leader.deleteMinUnlocked()`, releases. No announce array, no
combining, no help-while-waiting. This is the single biggest structural gap versus the
original, and closing it requires no NUMA APIs at all.

## 2. Gap: `maxByThread(tid)` is O(n) scan, original is O(1)

The original maintains `LeaderLargest.largest_ptr` (`lead_largest_p` /
`t_largest_in_leader`) incrementally on every insert:

```c
if (!last_ptr->largest_ptr || key > largest_ptr->key) last_ptr->largest_ptr = newnode;
```

`SortedLinkedListLeader.maxByThreadUnlocked(tid)` instead does a full list walk every
call — invoked on every slowest-path insert. Doesn't break correctness under the global
lock, but is a cheap, in-scope fix: add a `Node<V>[] largestByTid` array (mirrors
`LeaderLargest`), maintained on insert and on removal of that tid's max.

## 3. Gap: fast path is missing proactive self-help (not documented in CLAUDE.md either)

The original `hier_insert_local`'s true fast-path branch (worker heap nonempty, key ≥
heap-min) does more than a plain insert:

```c
insert_worker(...);
if (t_lead_counters->count < COUNTER_THRESHOLD) {   // COUNTER_THRESHOLD == CNTR_MIN
    // pop own worker-min, try harris_insert into leader, bump counter
}
```

So leader-list refill isn't solely delete-min's job (`REQUIRED_LEADER_MINIMUM = 2`
promotion in Java) — every fast-path insert opportunistically tops the leader list back
up toward `CNTR_MIN`. Java's `insert()` fast path currently does nothing but the heap
insert. This changes leader occupancy dynamics and insert-path distribution under
sustained fast-path-heavy load — directly relevant to reproducing the paper's Figure 5.
Fix: reuse `promoteFromWorkerIfCounterBelow` after the fast-path insert.

## 4. Gap (confirmed, now in active scope): no pointer bit-stealing / mark bits at all in the Java leader list

**Scope update:** the project now pursues a lock-free leader level (both
`SortedLinkedListLeader` and `HeapLeader` — see `CLAUDE.md`'s "Lock-free leader level"
section). This section originally framed the absence documented below as intentional
and correctly skipped; it is now the primary implementation target instead. Framing
below left largely as-written for the "what's missing and why it matters" analysis,
with corrections inline where the lock-free scope changes the verdict.

Checked directly — grepped `src/main/java` for `AtomicMarkableReference`,
`AtomicStampedReference`, `volatile ... next`, `compareAndSet`: **zero hits**.
`SortedLinkedListLeader`'s `Node<V> next` field is a plain, non-atomic reference,
mutated only because the whole list sits behind one `ReentrantLock`. This is exactly
the gap to close.

The original's leader list steals the two low bits of each `node__t*` pointer
(`harris.h:52-64`, `get_logdel_ref`/`get_moving_ref`/`get_marked_reference`, raw
`uintptr_t` tagging, not a language-level wrapper type):

- **bit 0 — `logdel`**: node is logically deleted (claimed by an in-progress
  `L-DeleteMin`/`linden_delete_min`), still physically linked, to be unlinked later
  (batched via `max_offset`, see below).
- **bit 1 — `moving`**: node is mid-transfer by a `L-DeleteMaxP` /
  `harris_insert_and_move` (slowest insert path pulling a thread's worst node back
  down to its worker heap). A concurrent search must treat a `moving` node as
  temporarily untouchable but *not* the same as logically-deleted-and-gone.

Both bits are read/written together via `__sync_bool_compare_and_swap` /
`__sync_fetch_and_or` directly on the pointer word. Under the *current* coarse-locked
Java list, no thread ever observes a partially-mutated node, so there was nothing for
a mark bit to protect against — that was the correct read while the project was
lock-based. Now that lock-freedom is committed scope, the two-bit scheme's cost is no
longer avoidable: `L-DeleteMin` and `L-DeleteMaxP` can genuinely execute concurrently
on adjacent/overlapping nodes once the lock is gone, which is exactly the hazard the
`moving` bit exists to prevent.

Java has no raw pointer tagging (JVM references aren't taggable), so the port needs
`java.util.concurrent.atomic.AtomicStampedReference<Node<V>>` (int stamp, enough room
for both `logdel` and `moving` as bit 0 / bit 1 of the stamp) — not
`AtomicMarkableReference<Node<V>>` (1 boolean mark, only enough for one of the two
bits). This is now a required building block, not a hypothetical — see §5 for the
starting skeleton and CLAUDE.md's "Lock-free leader level" section for the full task
breakdown.

Also worth noting, since §5 below independently investigates `LFListPriority.java`:
that file uses `AtomicMarkableReference` (1 mark bit) too — same insufficiency
relative to the original's 2 independent bits, for the same reason (only one kind of
remover, `removeMin`, ever existed there). It needs upgrading to
`AtomicStampedReference` as part of adapting it into `SortedLinkedListLeader`.

**Related items whose verdict flips once the list goes lock-free:**

- **Combined insert+evict-max single traversal** (`harris_insert_and_move`): previously
  filed as "meaningless under a lock — two sequential O(n) calls cost the same as one
  combined O(n) walk." That verdict only held under mutual exclusion. Lock-free, doing
  insert and evict-max as two *separate* CAS-based operations opens a real window: after
  the insert succeeds but before the evict-max runs, a concurrent `L-DeleteMin` could
  observe the list transiently over `CNTR_MAX` for that thread, or another thread's
  insert could interleave in a way the original's single combined traversal
  (`harris_search_ins_move`) was specifically designed to avoid. **Needs porting**, not
  skipping, once lock-free — flag this as a correctness-relevant technique to carry
  over, not just a perf optimization.
- **Lazy logical-delete + batched physical deletion** (`max_offset`): still optional.
  Immediate physical CAS-unlink is correct under Harris's algorithm with or without
  batching — batching is purely a contention-reduction tuning knob (fewer CAS retries
  under high delete-min churn), not a correctness requirement. Worth considering once
  the lock-free list is benchmarked and if `deleteMin` contention shows up as a
  bottleneck, but not required for a correct first lock-free implementation.
- **Block-linked-list heap storage** (`HeapList` chain of fixed arrays) vs Java's
  doubling array: unaffected by the lock-free decision — still memory-layout/allocator
  detail only, not algorithmic. Stays correctly skipped.

## 5. Candidate reuse: `Ex3/PriorityQ/LFListPriority.java` as a base for the leader level

Investigated `C:\Users\Dmytro\Akademit\Multicore\Ex3\PriorityQ\src\LFListPriority.java`
— a lock-free sorted linked-list priority queue from a prior course exercise, using
`AtomicMarkableReference` (single mark bit) with the standard Herlihy/Shavit
`find`/`add`/`removeMin` pattern (Michael's lock-free list applied to a PQ).

**What it already provides:**
- Real lock-free `add(item, priority)` and `removeMin()` on a sorted linked list —
  logical delete via the mark bit, physical unlink opportunistically during traversal
  (`find`). Conceptually the same family of algorithm as the original PIPQ's leader
  list (Harris-style marked-pointer list), just without PIPQ's extra per-node
  bookkeeping.

**What it is missing for PIPQ's leader-level contract (`LeaderLayer<V>`):**
- No `tid`/owner field on `Node` — trivial to add.
- No `deleteMaxByThread(tid)` / `maxByThread(tid)` — would need a new traversal
  analogous to Harris's `harris_search_idx`/`harris_delete_idx`; same O(n) cost
  characteristics our current `SortedLinkedListLeader` already has for these ops.
- No `largestByTid` / `LeaderLargest`-style O(1) tracking (see gap #2 above) — would
  need to be added regardless of base implementation.
- No per-thread counters (`leaderCounters[]` equivalent) — layered on top either way.
- Only a **single** mark bit, vs. the original's two independent bits (`logdel` +
  `moving`). The second bit exists specifically to let a node be safely claimed by a
  concurrent `L-DeleteMaxP` (evict-worst-for-tid, used by the slowest insert path)
  while `L-DeleteMin` is also racing over the same region of the list. This is exactly
  the hazard CLAUDE.md's "≥2 elements per thread" invariant is a correctness
  safeguard for. `LFListPriority` only ever has one kind of remover (`removeMin`), so
  it never had to solve this two-remover race — reusing it as-is for the leader level
  would reintroduce that hazard without the mechanism the paper built to prevent it.

**Verdict (updated — now adopted, not speculative):** `LFListPriority` is the chosen
starting *skeleton* for the lock-free `SortedLinkedListLeader` port, and it has the
advantage of being an already-understood, professor-vetted implementation from this
course. It is still **not** a drop-in replacement — building on it requires: tid
tagging, by-thread search/delete, largest-per-tid tracking, counters, and upgrading its
single `AtomicMarkableReference` mark to `AtomicStampedReference` for the
`logdel`+`moving` two-bit scheme, plus a correctness argument for the `L-DeleteMin` /
`L-DeleteMaxP` concurrent-remover race that bit exists to prevent (or a re-derived
equivalent). Previously flagged as reversing `CLAUDE.md`'s documented "no lock-free"
simplification and recommended as a discuss-first stretch goal — that discussion
happened; the decision is now to pursue it as committed scope, tracked in `CLAUDE.md`'s
"Lock-free leader level" section and "Next steps" list (§1 there covers the list,
before the harder heap-leader lock-free work).

## Priority recommendation (NUMA-free scope; lock-freedom is separately committed — see `CLAUDE.md`)

1. **Lock-free `SortedLinkedListLeader`** (§4–5 above; `CLAUDE.md` Next steps §1) — now
   the primary leader-level implementation target, base skeleton from
   `LFListPriority.java`, `AtomicStampedReference` for the two-bit scheme, ported
   combined-insert-and-move traversal (§4), re-derived ≥2-per-thread correctness
   argument under real concurrency.
2. **Announce + single-tier (N=1) coordinator combining for delete-min** — biggest
   NUMA-free fidelity win; this *is* the NUMA-free version of the real algorithm, not a
   workaround. Applies regardless of whether the leader list underneath is locked or
   lock-free.
3. **Incremental `largestByTid` tracking** — cheap, removes an O(n) scan from the hot
   slowest-path; also a prerequisite piece for the lock-free port (item 1).
4. **Proactive fast-path self-help at `cntrMin`** — cheap, changes measured path
   distribution to match the paper's model.
5. **Coarse-locked `HeapLeader` first, lock-free `HeapLeader` as a follow-up** — see
   `CLAUDE.md`'s "Lock-free leader level" section for candidate approaches and the
   paper's own negative finding on replacing the leader-level linked list with other
   data structures (Section 7) as a documented risk.
