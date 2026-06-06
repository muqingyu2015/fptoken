# FPToken 全量测试报告

> Markdown 摘要；HTML 报告页见 [test.html](test.html)。  
> **最新结果请以 CI / 本地 `run-fptoken-tests.ps1 -HtmlReport` 为准。**

## 执行

```powershell
.\scripts\run-fptoken-tests.ps1 -HtmlReport -ExcludePerfTag
```

报告输出：`build/test-results/junit-html/index.html`

## 覆盖范围

| 类别 | 示例 |
|------|------|
| 单元 | `FpGroupHotNgramBitIndexTest`、`FpGroupHotNgramRebuildTest` |
| 功能 | `FpBitIndexWriteReadFunctionalTest`、`FpIndexBuildQueryFunctionalTest` |
| 查询 | `FpQueryLengthPatternTest`、`FpAcctbalQueryDiagnosticTest` |
| 性能 | `@Tag("performance")`，默认 `-ExcludePerfTag` 跳过 |

## 关键用例

- `flush_selectiveRead_matchesFullRead_whenLenRowExceedsSkipInterval` — skip 表多段回归
- `search_selectiveFpBits_matchesFullIndex` — selective 与全量一致
- `bucketIndex_shortNgrams_packBytes_longNgrams_useHash` — bucket 规则

## 相关

- [test-factor-coverage-matrix.md](test-factor-coverage-matrix.md)
- [method-coverage-checklist.md](method-coverage-checklist.md)
