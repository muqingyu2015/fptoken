# 二进制检索 · FPToken 演示

> Markdown 摘要；PPT 式 HTML 见 [fptoken-binary-search-ppt.html](fptoken-binary-search-ppt.html)。

## 什么是「二进制检索」？

在原始字节流上子串/ngram 匹配，不依赖自然语言分词。

## 本项目的技术特点

- 列名前缀隔离稀疏列
- 组内热词 + v3 bucket 倒排（skip 跳跃读）
- selective 查询控制 IO

## 对比主流方案

| 方案 | 特点 |
|------|------|
| NGRAM 暴力分词 | 词项爆炸、posting 大 |
| Skip Index | 粗粒度，难精确 ngram AND |
| 暴力匹配 | CPU/IO 高 |
| **FPToken** | 组级闭块 + hot/common + selective bucket 读 |

## 开发背景

LXDB 宽表/JSON 稀疏列、二进制指纹字段 `_bfp` / `_sfp`。

## 相关

- [binary-search-lucene-design.md](binary-search-lucene-design.md)
- [fp-token-design_20260517.md](fp-token-design_20260517.md)
