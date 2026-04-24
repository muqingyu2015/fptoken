# Technical Debt Register

This file tracks technical debt discovered during the refactor cycle.
Each item includes estimated fix effort and impact scope so the team can plan iterations explicitly.

## How To Read

- `Estimate`: engineering effort estimate for implementation + verification.
- `Impact scope`: modules/files likely to be touched and primary risk surface.
- `Priority`: `P0` highest, `P3` lowest.

## Debt Items

### TD-001 - Module boundary still coupled by raw collection APIs

- **Debt**: protocol DTOs were introduced, but some call sites and tests still directly pass raw collections (`List<BitSet>`, `List<CandidateItemset>`), which weakens boundary guarantees.
- **Estimate**: 8-12 hours
- **Impact scope**: `exclusivefp/index`, `exclusivefp/miner`, `exclusivefp/picker`, `ExclusiveFrequentItemsetSelector`, integration tests around miner/picker entry points.
- **Priority**: P1
- **Suggested fix**: migrate internal call chain and test fixtures to protocol objects by default; keep raw overloads as compatibility wrappers only.

### TD-002 - ThreadLocal lifecycle policy lacks dedicated stress tests

- **Debt**: `ThreadLocal.remove()` cleanup was added, but no dedicated thread-pool regression test currently proves no cross-task residue under sustained reuse.
- **Estimate**: 4-6 hours
- **Impact scope**: `exclusivefp/miner/BeamFrequentItemsetMiner`, `runner/result/LineFileIndexBuilders`, concurrency test package.
- **Priority**: P1
- **Suggested fix**: add fixed-thread-pool loop tests that interleave valid/invalid workloads and assert deterministic outputs across rounds.

### TD-003 - Public API contract docs not yet centrally generated

- **Debt**: preconditions were improved in code comments, but there is no single generated/maintained API contract page for external users.
- **Estimate**: 6-10 hours
- **Impact scope**: `api/*`, `ExclusiveFrequentItemsetSelector`, `docs/README.md`, project `README.md`.
- **Priority**: P2
- **Suggested fix**: add `docs/api-contracts.md` with versioned contract rules and cross-links to source APIs.

### TD-004 - Performance baselines are documented but not fully gated in CI

- **Debt**: performance reports exist, but CI does not enforce baseline thresholds for key scenarios (e.g., pre-merge hint speedup, candidate budget stability).
- **Estimate**: 10-14 hours
- **Impact scope**: performance tests under `src/test/java/.../performance`, test scripts in `scripts/`, CI pipeline config.
- **Priority**: P1
- **Suggested fix**: add threshold checks and "warn vs fail" stages; persist baseline snapshots per branch.

### TD-005 - Static tuning config still shared across tests

- **Debt**: `@ResourceLock` mitigates order dependency, but shared static tuning still increases test orchestration complexity and parallel ceiling.
- **Estimate**: 12-16 hours
- **Impact scope**: `ExclusiveFrequentItemsetSelector` runtime tuning setters/getters, tests that mutate tuning knobs.
- **Priority**: P2
- **Suggested fix**: introduce request-scoped tuning object for core pipeline and deprecate mutable static tuning in tests and production flow.

### TD-006 - Data path memory profiling coverage is incomplete

- **Debt**: refactor reduced allocations (ByteRef path, staged index build), but there is no repeatable memory profiling checklist in repo automation.
- **Estimate**: 6-8 hours
- **Impact scope**: `exclusivefp/index`, `exclusivefp/miner`, `runner` data loaders, docs.
- **Priority**: P3
- **Suggested fix**: add memory smoke benchmark script and document target allocation ceilings for representative sample-data workloads.

### TD-007 - Demo package drift prevention is mostly manual

- **Debt**: demo entry points were updated to newer APIs, but no automated guard ensures future demo code avoids deprecated APIs.
- **Estimate**: 3-5 hours
- **Impact scope**: `src/.../demo`, `src/test/.../demo`, lint/test scripts.
- **Priority**: P2
- **Suggested fix**: add a demo contract test that fails on usage of `@Deprecated` public APIs.

## Maintenance Rule

- Update this register whenever a refactor task introduces deferred work.
- If an item is completed, keep it in this file and mark it with completion note/date instead of deleting it.
