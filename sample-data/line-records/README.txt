Sample input files for FptokenLineFileEntryMain.

Rules:
- One line = one record.
- UTF-8 bytes per line should be <= 64.
- Most lines are 64 bytes; some short lines and empty lines are included.
- Loader caps each file at 32000 lines.

Run:
java -cp bin cn.lxdb.plugins.muqingyu.fptoken.FptokenLineFileEntryMain

At runtime, if standard files are missing, the program auto-generates:
- records_001_small.txt
- records_002_medium.txt
- records_003_large.txt
- records_004_limit32000.txt
- records_005_sparse_short.txt
- records_006_mix.txt
