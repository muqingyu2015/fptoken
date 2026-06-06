# Web JSONL → Fulltext CSV 生成规则

适用于 `sample-data/web/**/*.jsonl` → `sample-data/web-csv-by-file/**/*.csv`（脚本：`scripts/generate-web-per-file-csv.py`）。

## 1. 输入

| 项 | 规则 |
|----|------|
| 根目录 | 任意 `web/` 目录 |
| 文件类型 | 递归扫描 `**/*.jsonl` |
| 编码 | UTF-8，`errors=replace` |
| 行格式 | 每行一个 JSON 对象（JSONL） |

## 2. 输出

| 项 | 规则 |
|----|------|
| 映射 | **1 个 `.jsonl` → 1 个 `.csv`** |
| 目录 | 保持相对路径，仅扩展名改为 `.csv` |
| 示例 | `web/ZWJCYLK3.0-1/1.jsonl` → `web-csv-by-file/ZWJCYLK3.0-1/1.csv` |
| CSV 编码 | UTF-8 |
| 行结束符 | `\n`（Unix LF） |
| 表头 | 首行固定：`hashstr,txt1,txt2,txt3,txt4` |
| 引号 | 标准 CSV（`csv.writer`，字段含逗号/换行时自动加引号） |

## 3. 列定义

| 列 | 类型 | 生成规则 |
|----|------|----------|
| `hashstr` | 字符串（十进制整数） | `String.hashCode(txt4)`，见下文算法 |
| `txt1` | 字符串 | 源 **文件名**，不含路径，如 `1.jsonl` |
| `txt2` | 字符串 | 同 `txt1` |
| `txt3` | 字符串 | 同 `txt1` |
| `txt4` | 字符串 | 正文片段，见第 4、5 节 |

## 4. 从 JSONL 取正文（txt4 源文本）

对每一行 JSONL：

1. 去掉行尾 `\n` / `\r`。
2. 空行（`strip()` 后为空）→ **跳过**，不写 CSV。
3. `json.loads(line)` 解析：
   - 失败 → `txt4 = strip(整行原文)`。
   - 成功且为对象 → 按顺序取第一个**非空字符串**字段：
     - `Content` → `content` → `text` → `body` → `Body` → `TITLE` → `title`
   - 若都没有 → `txt4 = strip(整行原文)`。
4. `txt4` 经 `strip()` 后仍为空 → **跳过**。
5. **去掉 NUL**：`txt4.replace('\x00', '')`（PostgreSQL COPY 不接受 `\0`）。
6. **保留标点**，不做清洗、不去 HTML。

## 5. 1024 长度拆分（核心）

| 项 | 规则 |
|----|------|
| 上限 | `MAX_TXT4_LEN = 1024`（**字符数**，非字节） |
| `len(txt4) ≤ 1024` | 输出 **1** 行 CSV |
| `len(txt4) > 1024` | 按字符切分：`txt4[0:1024]`, `txt4[1024:2048]`, … 每段一行 |
| 每段 | 单独计算 `hashstr = hashCode(该段)` |
| `txt1~txt3` | 各段相同，均为源 `.jsonl` 文件名 |

示例：`txt4` 长度 2500 → 3 行（1024 + 1024 + 452）。

## 6. hashstr 算法（Java String.hashCode）

与 Java `String.hashCode()` 一致，32 位有符号整数，输出十进制字符串：

```text
h = 0
for each char c in txt4 (UTF-16 code unit；Python 3 对 BMP 字符与 Java 一致):
    h = (31 * h + ord(c)) & 0xFFFFFFFF
if h >= 0x80000000:
    h = h - 0x10000000   # 转为有符号
hashstr = str(h)
```

Python 参考：

```python
def java_string_hashcode(text: str) -> int:
    h = 0
    for ch in text:
        h = (31 * h + ord(ch)) & 0xFFFFFFFF
    if h >= 0x80000000:
        h -= 0x100000000
    return h
```

## 7. 单行示例

**输入**（`1.jsonl` 一行）：

```json
{"ID": "WZ0002764773", "Content": "abc...很长正文..."}
```

**输出**（`Content` 长度 ≤ 1024 时 1 行）：

```csv
hashstr,txt1,txt2,txt3,txt4
1234567890,1.jsonl,1.jsonl,1.jsonl,abc...正文...
```

## 8. PostgreSQL 导入

```sql
\copy fp_token_example(hashstr, txt1, txt2, txt3, txt4)
FROM '/data1/web-csv-by-file/ZWJCYLK3.0-1/1.csv'
WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');
```

## 9. 与本仓库脚本对应关系

| 能力 | 脚本 |
|------|------|
| 每 jsonl 一个 csv | `scripts/generate-web-per-file-csv.py` |
| 全部合并一个 csv | `scripts/rebuild-fulltext-split-1024.py web` |
| 批量 COPY | `scripts/psql-copy-csv-folder-batch.sh` |

## 10. 默认参数汇总

```text
MAX_TXT4_LEN     = 1024
CONTENT_KEYS     = Content, content, text, body, Body, TITLE, title
SANITIZE         = 删除 U+0000
SPLIT            = 按字符切分，不截断到词边界
TXT1/TXT2/TXT3   = jsonl 文件名（basename）
HASH             = Java String.hashCode(txt4_chunk)
SKIP             = 空行、空 Content
```
