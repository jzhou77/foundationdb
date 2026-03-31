# Project Ideas Journal

## Session: 2026-03-30

### High-Level Concept

**Project:** Transaction System Improvements for FoundationDB

**Problem:** FoundationDB's transaction system has scalability and commit latency issues that need solving.

**Goal:** Improve transaction throughput, reduce commit latency, and scale better under load.

**Why it matters:** FoundationDB is a critical distributed database. Transaction performance bottlenecks limit what it can do at scale.

---

## Idea Categories

### 🎯 Features & Ideas
- [ ] **Root causes identified:**
  - Excessive coordination/serialization at Sequencer, Resolvers, LogServers
  - Hard to tune tail latency due to serial execution
  - All-replica persistence requirement prevents quorum-based logging
  - Scalability capped at ~30 log servers
  - Availability paradox: more processes = more failures = less uptime

### 💡 Insights & Epiphanies
- [ ] **Current design trade-offs:**
  - Total ordering makes system easy to reason about and debug ✅
  - Predictable, stable performance ✅
  - Fast/simple recovery algorithm ✅
  - These strengths create the weaknesses

- [ ] **Proposed solutions from critique:**
  - Move from total ordering → partial ordering where possible
  - Implement true quorum-based logging to decouple tail latency
  - Partition transaction system for independent scaling
  - Learn from past partitioning attempts that failed

- [ ] **Recovery as "catch-all":**
  - FDB uses recovery frequently because of all-replica durability
  - Simple recovery is both strength and weakness
  - Cannot use quorum without complicating recovery

### 💡 Aurora DSQL Ideas to Borrow

**Key architectural components:**
1. **Relay/Connectivity** — Client routing, request distribution, seamless communication
2. **Compute/Databases** — Query execution, PostgreSQL-compatible processing
3. **Transaction Log & Concurrency Control** — Atomicity, isolation, cross-node consistency
4. **User Storage** — Data durability, multi-AZ replication

**Critical design insights for transaction system:**

- [ ] **Locally processed, lazily checked:**
  - Transactions processed locally in originating region
  - Cross-region concurrency checks happen **at commit time only**
  - This avoids per-transaction cross-region coordination overhead

- [ ] **Active-parallelism:**
  - Multi-region: active-active mode
  - Both regions handle reads AND writes concurrently
  - Independent endpoints for each region
  - Data synchronously replicated (strong consistency)

- [ ] **Decoupled scaling:**
  - Scale reads, writes, and storage **independently**
  - Automatic scaling of compute, I/O, storage
  - Virtually unlimited horizontal scaling
  - No sharding or instance upgrades needed

- [ ] **Self-healing control plane:**
  - Control plane orchestrates coordination & redundancy
  - Self-healing capabilities built-in
  - Infrastructure failures don't impact database ops
  - Automatic failure recovery (no manual failover)

**Potential implications for FDB improvements:**

1. **Quorum + Local Processing:** Could we do locally-processed transactions with quorum-based logging? The async part handles replicas; sync part only for ordering.

2. **Commit-time consistency checks:** Instead of pre-transaction coordination, delay global consistency checks until commit phase.

3. **Partial ordering per shard:** Within each region/shard, allow parallel commits; only coordinate across shards at commit.

4. **Decouple logging from ordering:** Ordering can be global; logging can be quorum-based for scalability.

5. **Separate control plane:** The control plane handles coordination; data plane handles actual work independently.

### 📄 Current FDB Log System (from FDB SIGMOD '21 Paper)

**LogServer Role & Function:**
- **Durability anchor** — Last step in transaction commit path
- **WAL storage** — Persist transaction mutations before commit confirmation  
- **Decoupled from storage** — LogServer durability ≠ data apply latency
- **Fast reads enabled** — StorageServers aggressively pull logs, can serve recent data from memory

**Configuration & Replication:**
- **Replication model:** `k = f + 1` LogServers (tolerates `f` failures)
  - k=3 tolerates 2 failures
  - **Less than quorum** (quorum needs `2f+1`)
- **Placement:** Multi-host, fault domain distribution
  - Satellites for geo-redundancy (still within region)
- **Non-quorum: ALL replicas must succeed**
  - Any failure → full transaction system recovery triggered
  - Uses fewer resources but all-replica requirement

**Transaction Commit Logging Protocol:**
1. **Broadcast:** Proxy sends log message to ALL LogServers
2. **Targeted data:** Only designated replicas get mutation data
   - Proxy determines affected StorageServers
   - Sends to preferred LogServers + replication extras
   - Others get empty message body
3. **Message:** LSN (current) + previous LSN
   - Ensures sequential processing by LSN
4. **Durability:** LogServers write to SSD
5. **Commit:** ONLY after ALL k replicas ack

**Recovery Algorithm (Section 2.4.4):**
- **Trigger:** LogServer/TS component failure
- **Detection:** ClusterController detects & recruits new TS
- **Version tracking:**
  - **DV** = highest LSN persisted on each old LogServer
  - **KCV** = highest LSN Proxy reported as committed
  - **PEV** = Max(KCVs) — highest committed version client saw
  - **RV** = Min(DVs from recoverable set) — guaranteed recovered
- **Healing:** Copy PEV+1→RV from old to new LogServers
- **Rollback:** StorageServers discard uncommitted data (past RV)
- **Result:** Recovery typically < 5 seconds

**Scalability Limitations:**
- **Linear scaling with sharding:** Add LogServers = more throughput
- **Real bottleneck:** CPU saturation under max write workload
- **All k-replica must complete** before commit
- **Tail latency:** Controlled by slowest replica
- **Hard limit:** ~30 LogServers practical max

---

#### 💡 Connection to Quorum-Based Logging Ideas

**Current FDB vs Quorum Proposal:**

| Aspect | Current FDB | Quorum Proposal |
|--------|----|---------|-----|
| Replication | k = f + 1, all must succeed | f + (f+1), quorum needs |
| Commit condition | ALL designated replicas | ANY quorum (f+1 of 2f+1) |
| Failure handling | Trigger recovery | Quorum continues, heal later |
| Resources | Fewer replicas | More replicas (2f+1) |
| Tail latency | Controlled by slowest | Masked by quorum |
| Recovery complexity | Simple, fast (~5s) | More complex, less frequent |

**Why quorum doesn't work NOW:**

1. **Tied to recovery algorithm:** Quorum logging would complicate recovery
   - Need to track quorum state, not just single version number
   - Which versions can be safely committed during quorum?
2. **Version ordering complexity:** LSN still needs global ordering
   - Can't just take "quorum committed" — must respect LSN order
3. **Cross-LogServer coordination:** Quorum requires consensus across shards

**Key insight from FDB design:**
- `f+1` replication chosen to **minimize resources** at expense of flexibility
- Recovery used as "catch-all" because simpler than quorum state tracking
- Simplicity = strength AND weakness

### 🎨 Your Quorum-Based Design (new-arch.mmd)

**The Architecture:**

**New Components:**
1. **Log Proxy (LP1, LP2)** — Intermediate layer between LogServers and StorageServers
   - LP1 pulls from L1, L2; LP2 pulls from L3, L4
   - Serves as the single data provider for StorageServers
   
2. **CommitLogBox (CL)** — New coordination mechanism
   - Separate from transaction log
   - CommitProxy writes commit acknowledgments here
   - Log Proxies read from it to validate commits

**Updated Architecture Understanding:**

```
CommitProxy Phase:
  ① Generate batch-UID + CV → write to QUORUM of Txn Log
  ② Resolution ↔ Resolver (conflict check)
  ③ Write commit to CommitLogBox (CV, batch-UID)
  ④ Update Sequencer (CV)

Then (asynchronous):
  Log Proxies pull from Txn Log (across key-ranges)
  → StorageServers pull from Log Proxies
```

**CRITICAL CHANGES from standard FDB:**

1. **QUORUM writing (not all-replica):**
   - CP writes to f+1 of k LogServers (quorum)
   - NOT all k replicas must complete before commit
   - **Same as Aurora DSQL!**

2. **Key-range partitioning (sharding):**
   - Txn Log is partitioned by key-range
   - Each partition can scale independently
   - **NOT global ordering of ALL writes** - only within each partition's log!

3. **Multiple log subsets:**
   - Different keys → different Txn Log subsets
   - Each subset has its own quorum
   - Write to quorum of relevant subset only

4. **Separation of concerns:**
   - **Sequencer → CV assignment** (global ordering for transactions)
   - **Txn Log → durability** (partitioned, quorum-based)
   - **CommitLogBox → commit status** (what's been committed)
   - **Log Proxy → data retrieval** (what SS can safely read)

**How It Solves FDB Problems:**

| Problem | Current FDB | Your Solution |
|--------|----|----|----|
| **Tail latency** | All-replica must complete | Quorum (f+1 of k) - fastest complete |
| **Scalability** | ~30 LogServers max | More LogServers help through sharding |
| **Resource efficiency** | f+1 replicas | 2f+1 for quorum (more but scalable) |
| **Recovery dependency** | Complex recovery for single failure | Quorum continues, repair later |

---

### 📊 Comprehensive Comparison

| Aspect | Standard FDB | Your Design | Aurora DSQL |
|--------|----|----|----|----|
| **Write target** | ALL k replicas | QUORUM f+1 | QUORUM f+1 |
| **Partitioning** | Yes (sharded) | Yes, key-range | Yes, global |
| **Ordering** | Global LSN | Global CV + partitioned logs | Global at commit |
| **Scalability** | ~30 LogServers | Limited by quorum overhead | Virtually unlimited |
| **Tail latency** | Worst replica | Quorum (fastest f+1) | Quorum |
| **Failure handling** | System recovery | Quorum continues | Quorum continues |
| **Abstraction** | None | LogProxy/CommitBox | Control plane |

**Net Effect:** This design combines:
- **Quorum-based durability** (like Aurora DSQL, not FDB)
- **Key-range partitioning** (horizontal scalability)
- **Global sequencing** (via CV, preserves strict serializability)
- **Async replication** (tail replicas sync later)

This achieves **quorum-based tail latency masking** while maintaining **global ordering** through the CV mechanism. The Log Proxy provides an abstraction layer for StorageServers, and the CommitLogBox serves as the single source of truth for commit status.

---

## 🔍 Quorum Implementation Options Analysis

**Exploring all 3 options as requested:**

#### Option 1: Quorum Check at CP
```
CP writes to ALL k LogServers → waits for quorum f+1 → marks batch complete
```

**Pros:**
- CP has full transaction context
- Clear coordination point (CP + Sequencer)
- batch-UID + quorum tracking aligned

**Cons:**
- **Reintroduces tail latency problem** (CP waits for log completion)
- CP becomes bottleneck for every transaction
- Adds to existing CP complexity
- **Defeats purpose of async durability**

#### Option 2: Quorum Check at LogServers
```
CP writes → All LogServers → LogServers self-coordinate ISR → mark as ready
```

**Pros:**
- No CP overhead
- LogServers can independently track quorum
- Scales better across partitions

**Cons:**
- Requires explicit consensus between LogServers
- LogServers need state tracking coordination
- Need recovery coordination for LogServer failures
- More complex protocol

#### Option 3: Quorum Check at Log Proxy (RECOMMENDED) 🏆
```
CP writes → Log Proxies pull → quorum check at LP → mark batch ready
```

**Why this is best for your architecture:**
1. Leverages your Log Proxy as abstraction layer
2. CP unblocked instantly (async durability)
3. Tail latency masking happens at proxy level
4. StorageServers read from ready batches only
5. Cleaner separation of concerns

---

## 🦫 Kafka Insights for FDB Recovery Design

### Key Patterns from Kafka

**1. ISR (In-Sync Replica) Tracking**
- Leader maintains list of replicas that are caught up
- Only ISR members can become new leader during failover
- Dynamic - replicas join/leave ISR based on lag

**2. Leader-Follower Replication**
- All reads/writes → partition leader
- Followers pull/pull from leader
- Replicated log order preserved
- High watermark tracks committed writes

**3. Controller-Based Failover**
- One broker elected as "controller"
- Detects broker failures
- Elects new leader from ISR
- Updates cluster metadata

**4. Replication Lag Management**
- `replica.lag.time.max.ms` threshold
- Fell behind → removed from ISR
- Falls back in → must re-sync
- Tolerates lagging followers without failover

**5. Unclean Leader Election Trade-off**
- **Normal:** Wait for ISR replica (data safety)
- **Unclean:** Elect any replica (availability)
- Trade-off: Data loss vs unavailability

**6. High Watermark**
- Tracks last committed offset
- Consumers only read up to HW
- Protects against phantom reads

---

### 🎯 Kafka-Inspired Design for Your System

**Log System Abstraction with Quorum API:**
```
API: quorumWrite(batch-UID, data) → returns when f+1 achieved
API: quorumStatus(batch-UID) → returns "available", "pending", "quorum-achieved"
API: isReady(batch-UID) → returns true if quorum achieved for read
```

**Leader-Follower Pattern for Each Partition:**
- Within each key-range partition, one LogServer = "leader"
- Leader handles all writes for that partition
- Followers replicate from leader (pull-based)
- ISR tracking: which followers are caught up

**Recovery Workflow (like Kafka):**

**Scenario: Leader LogServer Fails**
```
1. Log Proxy detects (or controller)
2. Promote follower from ISR to leader (fast, follower already has data)
3. Update "who is leader for partition X" metadata
4. CP updates to use new leader
5. Followers catch up asynchronously

Impact:
- Writes continue (just new leader)
- No data loss if ISR was healthy
- Failover time: milliseconds
```

**Scenario: Follower LogServer Fails**
```
1. ISR shrinks (follower removed)
2. Leader continues serving writes
3. Quorum still satisfied (f+1 of remaining)
4. When follower rejoins: fetch all data from leader
5. Rejoins ISR once caught up

Impact:
- Reduced fault tolerance temporarily
- No write impact
- Automatic recovery
```

**Scenario: Slow Follower (Recovery Delay)**
```
1. Follower falls behind threshold
2. Removed from ISR
3. Quorum still works with remaining
4. When lagging follower reconnects:
   - Must fully re-sync (like Kafka)
   - Any lost data is overwritten
   - After sync, returns to ISR
```

---

### 🔧 Proposed Failure Handling Strategy

**For Your Architecture:**

1. **Identify LogServer → Leader election within partition**
   - Controller tracks "leader for partition P"
   - Followers pull from leader
   - Leader writes → commits after quorum

2. **Quorum tracking via batch-UID**
   - batch-UID tracks: "which partition servers have this batch"
   - Log Proxy aggregates answers: "got quorum for batch X"
   - CL records: "CP wrote X, quorum achieved at T"

3. **Recovery coordinator (Controller)**
   - Detects LogServer failure
   - Promotes follower from ISR
   - Updates cluster metadata
   - CP resumes with new leader

4. **Re-join protocol (like Kafka)**
   - Failed LogServer → re-fetches from new leader
   - Must fully sync before rejoining ISR
   - Any lost data is overwritten

5. **Log Proxy as read guard**
   - Only serves data where quorum achieved
   - Tracks batch-UID progress per partition
   - Blocks reads until quorum for that CV

**Key benefit:** You get **quorum durability** + **fast failover** + **tail latency masking**.

---

### 💡 Remaining Questions

**For the 3 quorum options:**

1. **Option 1 (CP):** Does your design actually need CP quorum sync?
   - Can CP just batch-UID broadcast, and quorum happens downstream?

2. **Option 2 (LogServer):** Can LogServers do ISR-style quorum tracking?
   - Would need explicit coordination protocol between LogServers

3. **Option 3 (Log Proxy):** Best fit! But:
   - How does Log Proxy know which batch-UIDs have quorum?
   - Need quorum feedback loop from LogServers to Log Proxy?

**Failure handling specifics:**

4. **What triggers recovery?**
   - Controller detects failure → leader election
   - Or Log Proxy detects quorum below threshold?

5. **Does CL coordinate recovery?**
   - CL has "what was written"
   - If CL ≠ LogServer state → reconcile?
   - How? Controller mediation?

6. **Partial quorum during failure:**
   - LogServer fails mid-batch
   - batch-UID can track: "incomplete"
   - Retry logic? Or mark batch as incomplete?

7. **StorageServer safety:**
   - Can SS pull data that's "written but not quorum-durable" yet?
   - Log Proxy must serve as gatekeeper
   - Returns "retry later" until quorum met

**Abstraction layer benefits:**

8. **Which quorum option best leverages Log Proxy abstraction?**
   - Clearly Option 3 (Log Proxy based)
   - But what does the "quorum write API" look like?
   - How does CP know it has quorum?

---

---

### 📝 Action Items
- [ ] Clarify where quorum logic is implemented (CP? Separate coordinator? Log Proxy?)
- [ ] Design CL durability mechanism (quorum? f+1 replicas?)
- [ ] Clarify batch-UID tracking granularity
- [ ] Map out failure scenarios: CP→CL success, quorum failure
- [ ] Design quorum write API for Log System abstraction
- [ ] Specify Leader-Follower replication protocol for partitions
- [ ] Define ISR tracking mechanism for each partition
- [ ] Implement Controller-based follower promotion logic
- [ ] Define batch-UID quorum tracking granularity
- [ ] Design recovery flow: failure detection → leader election → quorum restoration
- [ ] Map Kafka's ISR lifecycle to your CV + batch-UID model
- [ ] Decide: CP quorum sync vs async quorum at proxy level
- [ ] Design CL role in recovery coordination

---

## 📊 Kafka vs FDB vs Your Design: Quorum Comparison

| Mechanism | FDB (Current) | Aurora DSQL | Kafka (Pattern) | Your Design |
|-----------|------------------------------|-----------------------|---------|------------------------------|
| **Replication** | k=f+1, ALL must succeed | Quorum f+1 | Leader-follower ISR | Quorum f+1 per partition |
| **Write target** | ALL replicas | ALL but quorum check | Leader only | ALL LogServers (async) |
| **Ordering** | LSN/CV global | Global at commit | Per-partition leader | CV global |
| **Scalability** | ~30 LogServers | Virtually unlimited | Unbounded | More LogServers help |
| **Tail latency** | Worst replica (slow) | Quorum (fastest f+1) | Leader writes only | Quorum (fastest f+1) |
| **Leader election** | None (all-replica) | Auto-healing | ISR leader | ISR leader promotion |
| **Failure handling** | System recovery (TS refresh) | Quorum continues | Leader promotion | Leader promotion |
| **Recovery trigger** | Any failure | Quorum breach | Controller detects | Controller/LogProxy |
| **Recovery from** | Full TS recovery | Auto-healing | ISR leader | ISR leader |
| **Abstraction** | None | Control plane | Controller | Log Proxy abstraction |
| **Re-join protocol** | Full recovery | Full sync | Must re-sync | Full sync required |

**Your Hybrid Approach:**
- ✅ Quorum-based durability (like Aurora DSQL)
- ✅ Key-range partitioning (horizontal scaling)
- ✅ Global ordering via CV (preserves strict serializability)
- ✅ Leader-follower for fast failover (like Kafka)
- ✅ Log Proxy abstraction layer (your innovation!)
- ✅ Async replication (tail latency masking)

**Net Effect:** This achieves quorum durability WITHOUT sacrificing tail latency OR fast failover, while maintaining global ordering. The Log Proxy layer provides the critical abstraction that lets all pieces work together cleanly.

---

---

## Notes

[Any unstructured thoughts go here]

---

## Session Close
- **Last updated:** 2026-03-30 21:48 PDT
