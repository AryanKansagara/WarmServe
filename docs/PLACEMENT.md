# Centralized capacity-placement experiment

## Data structures and algorithm

The `placement` package represents an inventory of nodes, a map of used resource vectors and a map of job reservations. Each request declares positive CPU millicores and memory MiB plus a nonnegative GPU count. Bounded resource values keep accounting and fixed-point score arithmetic within int64 range.

A placement operation holds one mutex across feasibility checks, node selection and reservation. It filters unavailable nodes and capacity violations, then minimizes normalized remaining CPU+memory slack. CPU-only requests receive an extra penalty on GPU nodes to preserve accelerator capacity. Equal scores use node IDs as deterministic tie-breakers. This is a greedy heuristic, not an optimal bin-packing solver, preemption mechanism or fairness guarantee.

Placement scans N nodes in O(N) time. Job lookup/release uses maps with expected O(1) lookup cost. State uses O(N+J) space for nodes and jobs. Snapshot sorting costs O(N log N). Single-lock serialization simplifies invariants but limits throughput and does not provide distributed coordination.

Job IDs are idempotency keys: repeating the same ID and request returns the existing reservation without consuming resources again. Reusing an ID with different resource requirements fails. Release removes a reservation exactly once. Marking a node unavailable blocks new placements but does not move or terminate existing jobs; the caller must decide failure/recovery policy.

## Verification

Executed on 2026-10-27, macOS ARM64, Go 1.27.1. Commands:

```sh
go test -race -count=1 -cover ./...
go test -race -count=1 -run TestConcurrentCapacityStress -v ./placement
go vet ./...
```

All 12 placement test functions passed with 100% statement coverage; the existing gateway suite has 19 functions and 64.7% statement coverage. The stress test issues 200 concurrent reservation attempts against five simulated 2,000-millicore/4,096-MiB nodes. Each request asks for 100 millicores and 128 MiB. Exactly 100 requests fit the CPU budget, with no overcommit. Releasing all accepted reservations restores every used resource to zero. This is an invariant test, not a throughput or public-cloud capacity benchmark.

## Scope and tradeoffs

This is a standalone, in-memory centralized scheduler model. It does not provision hardware, migrate workloads, schedule the gateway's child processes, persist reservations, implement consensus, provide failover or manage real distributed nodes. There is no optimality comparison or demonstrated cloud-utilization improvement. Node state is local caller-supplied information, not heartbeats or authoritative resource telemetry. Adding leases, durable transactions and reconciliation would require additional design and tests.

Potential next work: replay representative workload traces against first-fit and best-fit baselines; quantify packing/fairness tradeoffs; add expiration/reconciliation; integrate an authenticated capacity service only after defining ownership and failure semantics. These are proposals, not implemented features. Code was developed with AI assistance; inspect it and describe your contribution accurately.
