# LXDB 打标功能 — 设计与使用说明

> Markdown 摘要；完整 SQL/JSON 示例见 [HTML](lxdb-marking-design-and-usage.html)。

## 一、开发背景

业务侧对查询结果/文档打标、规则过滤；与 FP 检索正交，属 LXDB 平台能力。

## 二、业界做法与 LXDB 思路

规则表 + `mark_filter` JSON + 引擎侧过滤。

## 三、旧环境：系统规则表 DDL

历史 DDL 参考 HTML 正文。

## 四、使用说明（SQL）

插入规则、绑定表/列、查询时携带 mark 上下文。

## 五、mark_filter JSON 语义

引擎支持的运算符与字段路径。

## 六、Java 实现架构（format.marking 包）

与 fptoken 包独立；集成时在查询链路后置过滤。

## 七、小结

打标 ≠ FP 索引；可组合使用。
