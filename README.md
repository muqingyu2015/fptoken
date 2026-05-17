# FPToken（FP Token 模块）

面向 **LXDB / 补丁版 Lucene** 的二进制指纹（Fingerprint）索引：在 BlockTree 写段阶段按 **组（index_id + group_id）** 聚合词项，对 common 载荷做 **byte n-gram 热词挖掘** 与 **8×256 双套位图索引**，并支持高级别 term 的 **透传写出**（复用已有 `fpBits`）。

> 本仓库为 **2026-05 重写** 后的独立模块源码；须与完整 LXDB 工程（含补丁 Lucene）联编。设计说明见 [`docs/fp-token-design_20260517.html`](docs/fp-token-design_20260517.html)。

---

## 阅读导航

| 文档 | 说明 |
|------|------|
| [`docs/fp-token-design_20260517.html`](docs/fp-token-design_20260517.html) | 技术设计（类职责、数据流、落盘格式） |
| [`docs/fp-token-review-and-test-report_20260517.html`](docs/fp-token-review-and-test-report_20260517.html) | 代码审查 + 单元测试结果 + 潜在缺陷清单 |
| [`docs/README.md`](docs/README.md) | 历史/协作文档索引 |
| [`AGENTS.md`](AGENTS.md) | 贡献者与 Agent 速览 |

---

## 依赖

| 依赖 | 路径 / 说明 |
|------|-------------|
| **LXDB Common** | `lib/lxdb_common-5_0_0_0-pg_so.jar`（须自行放入 `lib/`，未提交到 Git 时请从 LXDB 构建产物拷贝） |
| **补丁 Lucene** | 由上述 JAR（及完整 LXDB 工程）提供：`Terms#iterator_fp()`、`Terms#fpBits`、`BlockTreeTermsWriter#writefp`、`TermsWriter` 等 |
| **JUnit** | `scripts/run-fptoken-tests.ps1` 可自动下载；另自动拉取 `commons-logging` 供部分 Lucene 类静态初始化 |

仅使用「fptoken 源码 + 单个 common jar」**无法**完成全量 `javac`；测试脚本会在失败时回退到可单测子集（见下文）。

---

## 包结构（`cn.lxdb.plugins.muqingyu.fptoken`）

```
token/          FPToken、FpTokenAnalyzer、BinarySlidingWindowApi（64B 窗 / 32B 步）
config/         FpTokenBlockLevelPolicy、Lucene80FPSearchConfig（字段后缀 _bfp / _sfp）
api/            FPBlockTreeTermsWriter、FpTokenBlockOrchestrator、FpFilteredTermsEnum
dataset/common/ FpTokenTermLayout、FpTermKey、FPDocList、FpBlockInfo、组 KV 容器
dataset/block/  FpGroupDataOriginal / Rebuild、FpGroupHotNgramRebuild、FpGroupHotNgramBitIndex
```

---

## 核心流程（简图）

```
FpTokenAnalyzer → 带 12 字节 FP 头的 term
       ↓
iterator_fp 字典序遍历
       ↓
FpTokenBlockOrchestrator（按 group_level 与 targetLevel 分流）
   ├─ 高级别 / 体量达标 → FpGroupDataOriginal（fpBits 透传 writefp）
   └─ 可合并路径 → FpGroupDataRebuild
         → FpGroupHotNgramRebuild（common → hot，阈值默认 32）
         → FpGroupHotNgramBitIndex（hot/common 各 8×256 FixedBitSet）
         → writefp + termsbit 侧车 + FpBlockInfo → fpblock_list
```

字段识别：`Lucene80FPSearchConfig.isFpField(name)` — 后缀 `_bfp`（二进制）或 `_sfp`（字符串）。

---

## 构建与测试

**推荐脚本**（仓库根目录）：

```powershell
.\scripts\run-fptoken-tests.ps1 -HtmlReport -ExcludePerfTag
```

| 场景 | 命令 |
|------|------|
| 默认单元测试（排除 `lxdb-runtime`、`performance`） | 上式或 `.\scripts\run-fptoken-tests.ps1` |
| 含依赖完整 LXDB 运行时的用例 | `.\scripts\run-fptoken-tests.ps1 -IncludeLxdbRuntimeTag`（须在完整 classpath 下） |
| 已用 IDE 与 LXDB 全量编译 | `.\scripts\run-fptoken-tests.ps1 -SkipCompile` |

**报告目录**（可删除）：`build/test-results/junit-html/index.html`

**编译说明**：

1. 脚本将 `lib/*.jar` 加入 classpath，并尝试编译全部 `src/cn`。
2. 若失败，回退编译子集：`BinarySlidingWindowApi`、`ByteRef`、`WindowTerm`、`Lucene80FPSearchConfig`、`FPDocList`、`FpTermKey`、`FpTokenTermLayout`、`FpBlockInfo`。
3. 集成类（`api/*`、`FpTokenAnalyzer`、`FPToken` 等）需在 **LXDB 完整工程** 中编译。

**测试包**：`src/test/java/cn/lxdb/plugins/muqingyu/fptoken/tests/unit/`

---

## 与旧版 fptoken（互斥频繁项集 / Pre-merge hint）的关系

本仓库 **已不再包含** 旧版 `ExclusiveFpRowsProcessingApi`、采样挖掘、Pre-merge hint 等实现；相关文档若仍出现在 `docs/` 下，仅作历史参考。新模块解决的是 **Lucene 段内 FP 字段的写段编排与 n-gram 位图**，与「行级互斥项集三层输出」是不同层次的能力，可在 LXDB 产品内组合使用。

---

## 已知问题（摘要）

完整列表与测试证据见 [`docs/fp-token-review-and-test-report_20260517.html`](docs/fp-token-review-and-test-report_20260517.html)。摘要：

- **P0**：`FpGroupHotNgramRebuild.buildFinalHotTerms` 可能丢弃已有 hot 的 `FPDocList`；`read_group_id` 强转 `short` 与 4 字节 group_id 不一致；`FpFilteredTermsEnum` 缺少 `SeekStatus` 类型导致无法全量编译。
- **P1**：独立 classpath 下 Lucene/LXDB 静态初始化依赖未齐；`FpFilteredTermsEnum` 原地改写词项前缀的风险。

---

## 许可与归属

模块作者见各源文件 `@author`；与 LXDB/Lucene 补丁的版权与分发策略以宿主工程为准。
