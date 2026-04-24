# Repository guide (for agents and contributors)

## Layout

| Path | Role |
|------|------|
| `src/cn/lxdb/plugins/muqingyu/fptoken/` | Production code (API under `api/`, core under `exclusivefp/`, loaders under `runner/`) |
| `src/test/java/.../fptoken/tests/` | JUnit 5 tests (`unit`, `functional`, `performance`, …) |
| `sample-data/` | Fixtures (`line-records/`, `real-docs/`) |
| `docs/` | Human docs; start at [`docs/README.md`](docs/README.md) and [`docs/index.html`](docs/index.html) |
| `scripts/` | **Canonical** test runner: `run-fptoken-tests.ps1` |
| `archive/` | Historical only; not part of build (see `archive/README.md`) |

## Build and test (PowerShell)

From repo root:

```powershell
.\scripts\run-fptoken-tests.ps1
```

- `-Perf` / `-Scale` / `-Budget` / `-Stress` / `-Soak` — set matching `-Dfptoken.run*` JVM flags.
- `-HtmlReport -ExcludePerfTag` — JUnit XML under `build/test-results/junit-xml/`, summary HTML under `build/test-results/junit-html/index.html`.
- `-SkipCompile` — use existing `bin/` + `bin-test/` (e.g. after IDE compile).
- `-ExtraJvmArgs @('...')` — extra JVM arguments.

Alias: `scripts/run-tests-html-report.ps1` ≈ `-HtmlReport -ExcludePerfTag`.

## Primary API

- `cn.lxdb.plugins.muqingyu.fptoken.api.ExclusiveFpRowsProcessingApi` — row processing and `ProcessingOptions` (incl. pre-merge hints).
- `cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector` — lower-level selector.

## Conventions

- Performance tests: `@Tag("performance")` and often `@EnabledIfSystemProperty(fptoken.runPerfTests)`.
- Paths in docs: prefer forward slashes (`sample-data/...`).

Root narrative: [`README.md`](README.md).
