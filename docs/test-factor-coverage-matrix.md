# Test Factor Coverage Matrix

This document maps the newly requested testing factors to executable tests in this repository.

## Covered In Current Automated Tests

- Data quality:
  - Dirty/sparse/truncated-like payload handling: `EngineeringFactorsCoverageTest.dataQuality_sparseAndDirtyTerms_shouldNotCrashAndStayBounded`
  - Binary encoding (0x00-0xFF): `EngineeringFactorsCoverageTest.dataQuality_fullByteRange_shouldHandleBinaryBytesCorrectly`
  - Duplicate data (same `docId` repeated): `EngineeringFactorsCoverageTest.dataQuality_duplicateRowsWithSameDocId_shouldNotInflateSupport`
  - Out-of-order input docs: `EngineeringFactorsCoverageTest.dataQuality_outOfOrderInput_shouldKeepAscendingDocIdsInOutput`
- Algorithm correctness:
  - Beam small-set exactness check: `EngineeringFactorsCoverageTest.algorithmCorrectness_smallDataset_exactPairsShouldMatchBeamWithWideSearch`
  - Min-support monotonic direction check: `EngineeringFactorsCoverageTest.algorithmCorrectness_lowerMinSupport_shouldNotReduceCandidateCountOnCanonicalData`
  - Greedy vs two-phase vs brute-force gap check: `EngineeringFactorsCoverageTest.algorithmCorrectness_twoPhaseGapShouldNotBeWorseThanGreedyAgainstBruteforce`
  - Determinism/idempotence: `SelectorReliabilityRobustnessTest.parallelFacadeInvocations_sameInput_shouldBeDeterministic`, `ExclusiveFrequentItemsetSelectorFunctionalTest.sameInput_deterministicFingerprint`
- Concurrency/thread safety:
  - Async orchestration (`CompletableFuture`) consistency: `EngineeringFactorsCoverageTest.concurrency_completableFutureBatches_shouldMatchSerialResults`
  - Thread-pool reuse isolation: `EngineeringFactorsCoverageTest.concurrency_threadPoolReuse_shouldNotCrossContaminateTaskResults`
  - Mixed valid/invalid concurrent isolation: `SelectorReliabilityRobustnessTest.parallelMixedValidAndInvalidRequests_shouldIsolateFailures`
- Security/compliance:
  - Sensitive payload not leaked in exception message: `EngineeringFactorsCoverageTest.security_sensitivePayloadShouldNotLeakToErrorMessage`
  - Input fuzz and long-term bytes: `InputFuzzSafetyTest`
- Performance execution quality:
  - Environment snapshot presence: `PerformanceExecutionReliabilityTest.executionQuality_environmentSnapshot_shouldContainCoreRuntimeFields`
  - Warm-up + repeated runs (P50/P95/stddev): `PerformanceExecutionReliabilityTest.executionQuality_warmupAndRepeatedRuns_shouldProduceStableStats`
  - Standardized CSV/Markdown archiving: `PerformanceExecutionReliabilityTest.executionQuality_resultArchive_shouldWriteStandardizedCsvAndMarkdown`
  - Failure isolation in batch execution: `PerformanceExecutionReliabilityTest.executionQuality_failureIsolation_shouldContinueCollectingAfterSingleCaseFailure`
  - Performance run with correctness invariants: `PerformanceExecutionReliabilityTest.executionQuality_perfRun_shouldAlsoValidateResultCorrectnessInvariants`

## Partially Covered By Proxy Tests

- JVM/GC/memory/soak/fault-injection proxies:
  - `PerformanceAdvancedInfrastructureTest`
  - `PerformanceCaseCatalogPart2Test`
  - `PerformanceApplicationScenarioTest`

These are not full production-environment validations, but they provide regression guards in CI.

## Out Of Scope For Pure Unit/Local Integration Tests

The following factors require deployment/runtime infrastructure and should be validated in staging/perf environments:

- Container/K8s behavior (eviction, rolling update, readiness/liveness endpoints)
- OS-level resource leak checks (`lsof`, `netstat`, native memory tracking)
- Signal handling (`SIGTERM`/`SIGINT`) and process lifecycle integration
- Distributed observability (Prometheus scrape correctness, OpenTelemetry trace propagation)
- Cross-version rollback and disaster-recovery drills
- Cost-per-GB / cross-AZ network cost modeling

Recommended practice: keep these as separate environment-gated suites in CI/CD pipeline jobs.
