# Unit Method Coverage Checklist

This checklist tracks boundary and exception-focused unit tests for the core classes.

## Covered now

- `GreedyExclusiveItemsetPicker.pick(List<CandidateItemset>, int)`
  - normal flow, null/empty candidates, invalid dictionary size, negative term id, oversized term id, `Integer.MAX_VALUE` dictionary size, disjoint/conflict/tie-break paths
- `TwoPhaseExclusiveItemsetPicker.pick(List<CandidateItemset>, int, int)`
  - parameter validation, `maxSwapTrials` at `0/1/Integer.MAX_VALUE`, negative term id, oversized term id, one-opt replacement behavior, greedy equivalence at zero swaps
- `BeamFrequentItemsetMiner.mineWithStats(...)` (both overloads)
  - null/empty tidsets, null config, candidate truncation, beam/branching/frequent-term boundary combinations, non-positive controls (`maxFrequentTermCount`, `maxBranchingFactor`, `beamWidth`), idle-stop and runtime overload behavior
- `TermTidsetIndex.build(List<DocTerms>)`
  - null rows, null row element, empty rows, sparse doc ids, duplicate term content, null/empty terms in row, negative doc id
- `TermTidsetIndex.getIdToTerm()`
  - getter behavior covered, including reference semantics
- `TermTidsetIndex.getTidsetsByTermId()`
  - getter behavior covered, including reference semantics
- `CandidateItemset` constructors and getters
  - null ctor args, explicit support path, support/saving boundaries, term/doc reference semantics, zero-length termIds
- `SelectorConfig` constructor and getters
  - all non-positive validations, min/max relation checks, boundary valid values
- `ByteArrayUtils.copy/hash/compareUnsigned/toHex/formatTermsHex/normalizeTerms`
  - empty/null inputs, deterministic semantics, dedup semantics, null element/list failure modes
- `ExclusiveFrequentItemsetSelector` four public entry points
  - overload consistency, null/empty short-circuit, invalid config propagation, candidate-limit behavior, null-row short-circuit ordering

## Remaining TODO (priority ordered)

- `P1` Add targeted overflow-stress tests for score math in `TwoPhaseExclusiveItemsetPicker` replacement objective (very large `support`/`estimatedSaving` combinations), with explicit expected behavior documentation.
- `P1` Add deterministic micro-cases that assert exact candidate sets for `BeamFrequentItemsetMiner` under fixed beam/branch settings across multiple depths.
- `P2` Add guardrail tests around extremely large doc ids (disabled-by-default because they can trigger OOM on some JVMs), to document expected failure mode clearly.
- `P2` Add mutation-safety tests for facade output containers (`SelectedGroup`, `ExclusiveSelectionResult`) at API boundary level, complementing current DTO getter semantics tests.
- `P3` Add concurrency smoke tests (read-only parallel invocations) for `ExclusiveFrequentItemsetSelector` to catch accidental shared-state regressions.
