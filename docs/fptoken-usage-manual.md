# FPToken 完整使用手册

> Markdown 摘要；Quickstart 与示例见 [HTML](fptoken-usage-manual.html)。  
> **注意**：手册中 Pre-merge hint / 互斥项集 API 属**历史模块**；当前 BlockTree FP 模块见 [fp-token-design_20260517.md](fp-token-design_20260517.md)。

## 目录

1. Quickstart：5 分钟跑通
2. 示例 1：基础参数控制
3. 示例 2：带 n-gram 与 skip-hash 的完整调用
4. 示例 3：预合并提示（Pre-merge hints）— 历史
5. 示例 4：采样 + 提示 + 效果统计 — 历史
6. 生产落地建议与排障清单

## 当前模块快速入口

### 构建与测试

```powershell
.\scripts\run-fptoken-tests.ps1 -HtmlReport -ExcludePerfTag
```

### 字段约定

- FP 字段后缀：`_bfp`（二进制）、`_sfp`（字符串）
- 配置：`Lucene80FPSearchConfig`

### 查询路径

- `FpTokenQuery(fieldName, queryBytes, …)` → `FpSearch.search`
- selective 位图：`terms.fpBits(indexId, groupId, bucketKeys, bucketKeys)`

## 生产排障

- 0 命中：列名 mismatch、slice 长度、旧段 bucket 规则未重建
- `AlreadyClosedException`：升级含 selective 预取修复的 jar
- 慢查询：避免全量 `fpBits(null,null)`；确认 selective 路径生效
