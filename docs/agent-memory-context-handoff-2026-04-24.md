# FPToken Agent Memory Context Handoff (2026-04-24)

> 用途：把当前聊天中沉淀的“关键上下文记忆”固化为单文件，便于迁移到真实项目后快速唤醒。

## 1) 项目当前状态（高层）

- 当前项目测试主线可通过：默认全量测试集已跑通（无失败）。
- 近期工作重心：
  - pre-merge hint 质量治理、冲突消解、统计可观测增强
  - 二进制检索方向的评估文档与对外展示材料
  - 热点性能路径（`buildWithSupportBounds`）针对性优化
  - 内存泄漏防护测试与缓存自动卫生清理
- 用户偏好：不喜欢“类里套类”的写法，优先拆到对应包的独立类型。

## 2) 已完成的关键改动（可迁移关注点）

### 2.1 预合并提示（Pre-merge hint）可观测性

- 已在处理结果与控制台输出中增加提示效果统计字段，包含：
  - hint 输入数量
  - hint 映射候选数量
  - 合并后候选数量
  - hint 合并耗时与占总流水线比例
- 已增加运行时健康评估输出（assessment + 建议动作），用于识别负优化风险。

### 2.2 提示缓存的自动改进（卫生清理）

- 在 `ExclusiveFpRowsProcessingApi` 的 premerge hint 聚合缓存加入自动卫生清理：
  - 按操作窗口周期检测命中/未命中比例
  - miss 主导时触发缓存清理，释放陈旧引用
  - 暴露 cleanup 计数用于测试/观测

### 2.3 内存泄漏专项测试

- 新增独立泄漏守护测试类：
  - `MemoryLeakGuardUnitTest`
- 覆盖两个方向：
  - 同进程重复调用后历史结果可回收（WeakReference + GC 压测）
  - 高 miss 场景下缓存自动卫生清理可触发

### 2.4 热点性能优化（buildWithSupportBounds）

- 已落地两项低风险优化：
  1. 支持度排序由 O(n²) 插入排序改为 O(n log n) 原生排序
  2. second pass 位图填充内层访问优化（数组直达，减少间接调用）
- 结论：先消除明确次优实现；first pass 仍是主要热段，后续可继续深挖。

### 2.5 结构风格改动（按用户偏好）

- `WindowTerm` 已从内部静态类拆为包级独立类：
  - `src/cn/lxdb/plugins/muqingyu/fptoken/api/WindowTerm.java`

## 3) 新增/更新的关键文档

- `docs/fptoken-boss-customer-presentation.html`
  - 老板/客户导向项目介绍 PPT（HTML）
- `docs/binary-retrieval-solution-evaluation-ppt.html`
  - 二进制检索专家评审版 PPT（HTML）
- `docs/buildWithSupportBounds-performance-improvement-report.html`
  - 热点优化复盘（为什么慢、怎么改、改了什么、后续怎么做）

## 4) 测试与执行约定

- 主测试命令：
  - `./scripts/run-fptoken-tests.ps1`
- 常用回归命令（排除性能）：
  - `./scripts/run-fptoken-tests.ps1 -ExcludePerfTag`
- 说明：
  - 性能/规模/预算测试含门控与长时场景，建议分组跑，不建议一次性全开长跑。

## 5) 仍可继续推进的方向（建议优先级）

- P1：`buildDerivedData` 多遍扫描合并，降低重复遍历开销
- P1：`buildWithSupportBounds` first pass 深化优化（hash 路径与局部去重）
- P1：性能门限接入 CI（关键场景预算告警/失败门禁）
- P2：按数据族群沉淀参数模板（特别是 hint 与采样联动）

## 6) 迁移到真实项目后的“唤醒模板”

把下面这段贴给 Agent，可快速恢复上下文：

```text
请加载这个记忆文件并按其中上下文继续：
docs/agent-memory-context-handoff-2026-04-24.md

重点延续：
1) pre-merge hint 可观测与负优化治理
2) buildWithSupportBounds / buildDerivedData 的性能优化路线
3) 内存泄漏守护测试与缓存自动清理策略
4) 保持“尽量避免类里套类，优先包级类型”的代码风格
```

## 7) 备注

- 本文件是“会话级上下文快照”，不替代正式架构文档与变更记录。
- 若后续有新一轮大改，建议新增一个带日期的新 handoff 文件，避免覆盖历史信息。
