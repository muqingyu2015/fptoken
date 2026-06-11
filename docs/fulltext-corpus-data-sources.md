# Fulltext 语料（10×1亿行 CSV）

## 标准第三方数据（无与你完全同构的 CSV）

| 数据集 | 规模 | 格式 | 链接 |
|--------|------|------|------|
| **CodeSearchNet** | ~600万函数 | JSONL | https://github.com/github/CodeSearchNet |
| **The Stack** | TB级源码 | 按文件 | https://huggingface.co/datasets/bigcode/the-stack（需登录） |
| **Project CodeNet** | 竞赛代码 | 专用 | https://github.com/IBM/Project_CodeNet |

本仓库脚本会把上述数据**拆成逐行**写入你的列：`hashstr, txt1, txt2, txt3, txt4`。

## 当前产物位置

```
sample-data/corpus/
  fulltext_corpus_0001.csv   # 分片 1（目标 1 亿行）
  fulltext_corpus_0002.csv   # … 共 10 个
  fulltext_corpus_manifest.json
  fulltext_corpus_copy_all.sql
  generate.log
```

**已有进度**（若中断可续跑）：见 `manifest.json` 的 `total_rows`。

## 生成命令（断点续跑）

```powershell
pip install datasets huggingface_hub

# 推荐：登录后可用 The Stack（否则仅 clone 很难凑满 10×1亿）
huggingface-cli login

py -3 scripts/generate-fulltext-corpus.py `
  --clone-repos --codesearchnet --s3-codesearchnet --stack `
  --local "D:\cursor\fptoken" "D:\开源项目源码" `
  --rows-per-file 100000000 --min-parts 10 `
  --out-dir "D:\cursor\fptoken\sample-data\corpus"
```

或：`.\scripts\run-fulltext-corpus-10x100m.ps1`

## 规模说明

| 目标 | 约磁盘 |
|------|--------|
| 10 文件 × 1 亿行 | **100–150 GB** |
| 第 1 片曾到 ~5750 万行 | ~5.9 GB |

单靠本地 + CodeSearchNet + GitHub 浅克隆**通常不够 10 亿行**；必须 **`--stack` 且 HF 已同意协议**，或长期跑完 `github-repos-seed.txt`（含 `torvalds/linux`）。

## COPY 导入

```sql
\copy fulltext(hashstr, txt1, txt2, txt3, txt4)
FROM 'D:/cursor/fptoken/sample-data/corpus/fulltext_corpus_0001.csv'
WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');
```
