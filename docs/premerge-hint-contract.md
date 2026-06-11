# Pre-merge Hint Contract

本文定义 pre-merge hint 的调用契约：何时传入、字段要求、精度责任、性能回退归属与回滚流程。

## 1. 何时应该传入提示

- **建议传入**：调用方正在做“段合并/批次合并”，且当前批次与历史段分布相近（同业务、同分区、时间窗口未明显漂移）。
- **不建议传入**：历史段与当前段分布差异大、数据热词快速漂移、或尚未建立日志观测能力。
- **默认优先级**：优先传 `premergeMutexGroupHints`；`premergeSingleTermHints` 仅作为补充且应限流。

## 2. 提示字段最小要求

每条 hint 对应 `ExclusiveFpRowsProcessingApi.PremergeHint`：

- `termRefs`：非空；建议已归一化（编码一致、无空 term、去重）。
- 互斥组合提示（mutex）：建议长度 >= 2。
- 单词提示（single）：长度通常为 1；不建议大规模无筛选地全量下发。

对应选项：

- `withPremergeMutexGroupHints(...)`
- `withPremergeSingleTermHints(...)`
- `withHintBoostWeight(...)`
- `withHintValidationMode(...)`

### 2.1 序列化格式与兼容性

建议使用 `PremergeHintWireCodec`：

- 编码：`PremergeHintWireCodec.encodeV1(hints)`
- 解码：`PremergeHintWireCodec.decodeLenient(payload)`

兼容性约束：

- 采用 `FPTOKEN_PREMERGE_HINTS	v=1` 头 + 行级 `key=value` 格式。
- `terms=` 为已编码词项列表（base64，逗号分隔）。
- 解码端对未知字段（如未来新增 `source=...`、`confidence=...`）**直接忽略**，不抛异常。
- 脏行/缺失字段会被跳过，保证“可读多少读多少”的安全降级。

## 3. 精度与性能责任边界

- **算法侧保证**：所有提示候选都会经过当前段校验与全量回算，不影响正确性（最多影响性能）。
- **调用方责任**：提示质量（新鲜度、稳定性、覆盖度）由调用方保证；低质量提示会带来性能回退风险。
- **平台侧责任**：提供可观测指标与降级能力，避免性能回退持续放大。

## 4. 观测与验收指标（建议上线门槛）

至少记录以下字段并按 merge job 聚合：

- `improvementPercent` / `improveMedianPercent`
- `candidateCount`
- `selectedGroupCount`
- `selectedGroupTotalEstimatedSaving`
- `selectedGroupAverageEstimatedSaving`
- `selectedGroupLowSavingPercent`

建议门槛（可按业务调优）：

- `negativeRatio`（负优化占比）长期 > 30%：默认关闭 hints
- `effectiveRatio`（有效占比）长期 >= 60% 且 `negativeRatio` 低：默认开启 hints

可使用：

- `cn.lxdb.plugins.muqingyu.fptoken.runner.analysis.HintEffectivenessLogAnalyzer`

## 5. 回退与回滚机制

### 5.1 运行时快速回退（调用侧）

- 关闭 hints：`withPremergeMutexGroupHints(empty)` + `withPremergeSingleTermHints(empty)` + `withHintBoostWeight(0)`
- 或只保留 mutex hints，关闭 single hints。

### 5.2 责任归属与处置

- **提示生产方**：负责提示源质量与时效（分段漂移、重复提示、噪声提示）。
- **平台维护方**：负责阈值策略、自动降级与回滚开关可用性。
- **值班流程**：当观察到连续负优化（例如 3 个窗口）时，先禁用 single hints，再评估是否整体禁用 hints。

## 6. 版本化与变更管理

- 新增/修改提示策略时必须同步更新本文档与性能报告。
- 若引入新字段（如提示来源、时间窗、置信度），需保证向后兼容：旧调用方可继续仅传 `termRefs`。

