# zer0share 增量同步修复 (index_dailybasic optional) — 验收报告

- 生成: 2026-10-19 15:45 CST
- 改动文件: `/git/zer0share/scripts/download_all_v2.py` (备份 `download_all_v2.py.bak-20261019`)
- 新增验证脚本: `scripts/verify_sync_fix.py`
- 环境: `/miniconda3/envs/quant/bin/python`
- **结论: 修复本身 PASS** — `index_dailybasic` 在文件缺失时跳过、0 网络请求、dry-run 与真实运行均确认;
  但 **3600s 定时任务仍会超时**, 原因已换人: 另有 12 张表的目标文件同样不存在, 仍走 1644 个交易日全历史回填
  (实测 `adj_factor` 900s 只跑到 200/1644)。详见第 6 节。

## 1. 根因复现 (改之前, 15:15 实跑)

```console
$ /miniconda3/envs/quant/bin/python download_all_v2.py --dry-run --mode incremental --tables index_dailybasic
[26] index_dailybasic (指数每日指标)...
  📅 交易日历: /quantdata/zer0share/data/stock/trade_cal.parquet (20200102 ~ 20260930, 1636 天)
  📅 合并分区日历 /quantdata/zer0share/data/stock/trade_cal → 覆盖至 20261231 (8797 天)
  已覆盖: 无 (文件不存在或为空) → 计划全量区间
  计划下载: 20200102 ~ 20261019, 1644 个交易日
```

目标文件 `/quantdata/zer0share/data/index/index_dailybasic.parquet` 全盘不存在 (见第 5 节末), 于是每次调度都
计划 1644 个交易日逐日下载; 该表在 TABLES 里排第 27 位, 卡住它后面的 moneyflow/龙虎榜/财报/宏观全部停更。

## 2. 改动 (最小集, 未动其它表/full 模式)

| 位置 | 改动 |
|---|---|
| `_date()` | 增加 `optional=False` 参数, 写进 spec |
| `TABLES` | `index_dailybasic` 传 `optional=True`, 其它表不变 |
| `run_incremental()` | 开头: `spec['optional']` 且 `resolve_path()` 后文件不存在 → 打印 `⏭ <name>: 文件缺失且为 optional, 跳过 (不做全历史回填)` 并 return (在任何网络调用之前) |
| `inc_date_table()` | `plan_start = max(plan_start, START)`: 非 optional 表缺文件时保留全历史回填, 但起点不早于 `START` |

`resolve_path()` 会先看 `alt_paths`, 所以 optional 表如果换了落盘位置仍能正常增量, 不会被误跳过。

## 3. 验收 1: dry-run 单表

```console
$ /miniconda3/envs/quant/bin/python download_all_v2.py --dry-run --mode incremental --tables index_dailybasic
======================================================================
Zer0share 增量刷新 v2 (tushare: https://tl.kaixin8.top/tushare/sdk)
  模式: incremental   时间: 20200101 ~ 20261019   表: 1/39 张   [DRY-RUN: 不联网/不写盘]
======================================================================

[26] index_dailybasic (指数每日指标)...
⏭ index_dailybasic: 文件缺失且为 optional, 跳过 (不做全历史回填)

======================================================================
Dry-run 完成 (0.0s): 仅打印计划, 未发起网络请求, 未写入任何文件
```

- 显示跳过: **是**；1644 天计划消失；网络请求 **0** (dry-run 本身不联网, 且跳过分支在 `batch_by_date` 之前返回)。
- 更硬的证据见第 5 节 A: `dry_run=False` 时把 `pro` 换成"任何属性访问即抛错"的代理, 仍正常跳过, 证明没有碰网络。

## 4. 验收 2: dry-run 全部表

```console
$ /usr/bin/time -f "WALL=%es" /miniconda3/envs/quant/bin/python download_all_v2.py --dry-run --mode incremental
...
[26] index_dailybasic (指数每日指标)...
⏭ index_dailybasic: 文件缺失且为 optional, 跳过 (不做全历史回填)
...
Dry-run 完成 (1.4s): 仅打印计划, 未发起网络请求, 未写入任何文件

exit=0
WALL=1.82s
```

- 39/39 张表全部列出计划, `index_dailybasic` 显示跳过。
- 总耗时 **1.82s** (< 30s 要求)。

## 5. 验收 3: 真实运行

### 5a. 设计指定的命令 (timeout 900s) — **未完成, 卡在 adj_factor**

```console
$ timeout 900 /miniconda3/envs/quant/bin/python download_all_v2.py --mode incremental \
    --tables daily,daily_basic,adj_factor,stk_limit,suspend_d,moneyflow,moneyflow_ind
...
[1] daily (日线行情)...
  已覆盖: 20261016 (共 7,592,862 行, 最近一日 5,567 只股票)
  计划下载: 20261019 ~ 20261019, 1 个交易日, 每个交易日约 5,567 只股票
  ⚠️ daily: 计划区间无数据返回

[4] adj_factor (复权因子)...
  已覆盖: 无 (文件不存在或为空) → 计划全量区间
  计划下载: 20200102 ~ 20261019, 1644 个交易日
  ...20201102 (553,691 rows, 200/1644)

real    15m0.024s
EXIT=124
```

- `daily`: 20261019 接口返回空 (15:20 实测, 当日数据尚未发布), 属上游未出数, 不是代码问题。
- `adj_factor`: 文件不存在 → 非 optional → 全历史 1644 天回填。900s 只完成 **200/1644** 个交易日
  (≈4.5s/天, 全量需 ~2 小时), 超时被杀, 该表未写入 (脚本在整表下载完才 merge+write_atomic, 无半成品)。
- `daily_basic / moneyflow / moneyflow_ind / stk_limit / suspend_d` **未被执行到** (排在被杀的 adj_factor 之后)。

> 说明: 第一次跑同一命令时, 看到 6 分钟内 adv_factor 进度为 0 且全量必超时, 手动 kill (EXIT=143);
> 随后按验收要求重跑完整 900s (上面这次, EXIT=124), 结果一致。

### 5b. 真实跑快表子集 (补齐每张表覆盖区间与行数)

```console
$ /usr/bin/time -f "WALL=%es" timeout 420 /miniconda3/envs/quant/bin/python download_all_v2.py \
    --mode incremental --tables daily,daily_basic,stk_limit,suspend_d
[1] daily (日线行情)...
  已覆盖: 20261016 (共 7,592,862 行, 最近一日 5,567 只股票)
  计划下载: 20261019 ~ 20261019, 1 个交易日, 每个交易日约 5,567 只股票
  ⚠️ daily: 计划区间无数据返回
[5] daily_basic (每日指标)...
  已覆盖: 20261016 (共 8,076,895 行, 最近一日 5,567 只股票)
  计划下载: 20261019 ~ 20261019, 1 个交易日, 每个交易日约 5,567 只股票
  ⚠️ daily_basic: 计划区间无数据返回
[22] stk_limit (涨跌停价)...
  已覆盖: 20261019 (共 8,186,084 行, 最近一日 5,653 只股票)
  ✅ 无需下载: 已覆盖至 20261019, 晚于 --end(20261019)
[23] suspend_d (停复牌)...
  已覆盖: 20261019 (共 6,165 行, 最近一日 12 只股票)
  ✅ 无需下载: 已覆盖至 20261019, 晚于 --end(20261019)
All done. Total time: 0.3 min
❌ moneyflow 不存在: /quantdata/zer0share/data/stock/moneyflow.parquet
⚠️ build_sector_fundflow 退出码 1:
real    0m17.036s
```

覆盖区间/行数 (跑前跑后一致, 因为 20261019 上游无数据):

| 表 | 覆盖区间 | 行数 | 本次结果 |
|---|---|---|---|
| daily | 20200102 ~ 20261016 | 7,592,862 | 计划 1 天 (20261019), 接口空返回 |
| daily_basic | 20200102 ~ 20261016 | 8,076,895 | 计划 1 天, 接口空返回 (未重下历史) |
| stk_limit | … ~ 20261019 | 8,186,084 | 无需下载 |
| suspend_d | … ~ 20261019 | 6,165 | 无需下载 |
| adj_factor | 无文件 | 0 | 全历史回填, 900s 到 200/1644, 未完成 |
| moneyflow | 无文件 | 0 | 未执行到 (被 adj_factor 耗尽窗口); 实测 32.65s/天, 全量约 895 min |
| moneyflow_ind | 无文件 | 0 | 未执行到; 接口 `unknown_api` (见 5c 末), 即使跑到也会全空 |

`build_sector_fundflow` 在 `moneyflow` 文件缺失时子步骤失败, 是既有问题 (本修复未改该脚本), 不影响下载 exit code。

### 5c. 离线语义验证 (`verify_sync_fix.py`, 不联网/不写盘)

```console
$ /miniconda3/envs/quant/bin/python verify_sync_fix.py
TABLES[index_dailybasic] = {'name': 'index_dailybasic', 'kind': 'date', 'path': '/quantdata/zer0share/data/index/index_dailybasic.parquet', 'optional': True}

[A] optional + 文件缺失, dry_run=False:
⏭ index_dailybasic: 文件缺失且为 optional, 跳过 (不做全历史回填)
[A] 判定: PASS (耗时 0.0s, 文件仍不存在=True)

[B] optional + 文件存在 (path=daily.parquet), dry_run=True:
  📅 交易日历: /quantdata/zer0share/data/stock/trade_cal.parquet (20200102 ~ 20260930, 1636 天)
  📅 合并分区日历 /quantdata/zer0share/data/stock/trade_cal → 覆盖至 20261231 (8797 天)
  已覆盖: 20261016 (共 7,592,862 行, 最近一日 5,567 只股票)
  计划下载: 20261019 ~ 20261019, 1 个交易日, 每个交易日约 5,567 只股票
[B] 判定: PASS (耗时 0.503s)

[C] 非 optional (weekly) + 文件缺失, dry_run=True:
  已覆盖: 无 (文件不存在或为空) → 计划全量区间
  计划下载: 20200102 ~ 20261019, 1644 个交易日
[C] 判定: PASS (耗时 0.0s)

RESULT: PASS
```

- A 把 `pro` 换成访问即抛 `AssertionError` 的代理, 仍正常跳过 → **0 次网络调用**。
- B 证明 `optional` 只在文件缺失时生效, 文件存在时照常增量 (不会把这张表永久废掉)。
- C 证明非 optional 表仍是首次建表语义, 起点 `START`。

各表单交易日接口实测延迟 (用于第 6 节预算, 每天 3 次取均值):

| API | 行/天 | 秒/天 | 1644 天估算 |
|---|---|---|---|
| adj_factor | 5,574 | 5.51 | ~151 min (实测 900s 完成 200 天) |
| moneyflow | 5,574 | 32.65 | ~895 min |
| weekly | 0 (该日无数据) | 9.70 | ~266 min |
| monthly | — | 2.66 (`upstream_pool_exhausted`) | ~73 min |
| margin_detail | 2,348 | 8.52 | ~233 min |
| hk_hold | 1,018 | 3.70 | ~101 min |
| top_inst | 660 | 5.76 | ~158 min |
| top_list | 62 | 0.64 | ~18 min |
| shibor | 2,000 | 6.85 | ~188 min |
| repurchase | — | 1.21 (`upstream_pool_exhausted`) | ~33 min |
| moneyflow_hsgt | 1 | 0.14 | ~4 min |
| moneyflow_ind | — | 0.57 | 接口返回 `[unknown_api] 接口不存在或未开放`, 无法建表 |

### 5d. 数据文件状态 (证明没重复重下/没污染)

```console
$ python -c '<读 parquet 表头>'   # 15:41 实跑
daily            rows=7,592,862 last=20261016 mtime=17:04:33 size=223.0MB
daily_basic      rows=8,076,895 last=20261016 mtime=15:02:15 size=493.7MB
adj_factor       MISSING
stk_limit        rows=8,186,084 last=20261019 mtime=15:03:13 size=44.6MB
suspend_d        rows=    6,165 last=20261019 mtime=15:03:30 size=0.0MB
moneyflow        MISSING
moneyflow_ind    MISSING
stock_st         rows=  273,918 last=20261019 mtime=15:12:48 size=0.1MB
index_dailybasic MISSING
```

- `daily_basic` 仍是手工补到 20261016 的版本 (mtime 15:02, 行数未变), `stock_st` 仍是手工补到 20261019 的版本 (mtime 15:12) — 本次运行**没有**重下这两张表。
- `index_dailybasic.parquet` 仍然不存在: 修复只让它跳过, 没有伪造空文件。

## 6. 改后 3600s 窗口能覆盖哪些表, 以及建议拆分

**工作日例行同步 (所有目标文件都存在的前提下)**: `daily` 1 天 + `daily_basic` 1 天 (已知 ~65s/天, 本次 20261019 无数据未能复测) +
`stk_limit`/`suspend_d` 已覆盖即 0 + `index_daily` 9 个代码 + `index_weight` 1 天 + `top_list`/`moneyflow_hsgt`
小表 → 合计几分钟, 3000s 余量充足。`index_dailybasic` 现在固定 0s 跳过。

**但当前数据状态下降不下来**: dry-run 显示 **12 张表目标文件不存在**, 每张都计划 1644 个交易日
(weekly, monthly, adj_factor, moneyflow, moneyflow_ind, moneyflow_hsgt, margin_detail, hk_hold,
top_list, top_inst, shibor, repurchase) + 12 张财报/股东类表按 5577 只股票全历史。
按实测延迟, 仅 1644 天类的表合计 ≈ **35.3 小时** (2119 min), 所以 3600s 仍会超时——只是卡点从 `index_dailybasic`
换成了排在它前面的 `adj_factor`/`moneyflow`。修复解决的是"永远卡在不可能完成的表", 不是"首次建表太慢"。

建议拆分:

1. **周六慢任务 (≥ 4h, 分块串行)**: `adj_factor` (~2.5h), `moneyflow` (~15h, 分 4-6 段), `weekly` (~4.4h),
   `monthly`, `margin_detail`, `top_inst`, `shibor`, `repurchase`, 以及 5577 只股票的财报类表。
   首次建完后它们都变成"补 1 天", 周中任务自然瘦下来。
2. **工作日 3600s 任务**: `daily, daily_basic, stk_limit, suspend_d, index_daily, index_weight, top_list, moneyflow_hsgt`。
   实测 17s (今日上游无数据时) / 有天数据时主要是 daily_basic 65s×天数。
3. **`moneyflow_ind`**: 接口 `unknown_api` (代理未开放), 建议标 `optional` 或从 TABLES 移除, 否则每次都白跑 1644 次失败请求
   (每次 0.1-1.3s, 约 16-35 min)。
4. **`adj_factor` 有现成分区目录** `/quantdata/zer0share/data/stock/adj_factor/` (8738 个分区), 但 spec 没配
   `alt_paths`, 所以脚本认为"文件不存在"。加 `alt_paths` 或先做一次分区→单文件 seed, 可省掉 2.5 小时回填;
   这属于改其它表行为, 未在本次改动范围内, 需你确认后再动。

## 7. 未完成/失败项 (如实记录)

| 项 | 状态 | 原因 |
|---|---|---|
| `adj_factor` 真实增量/建表 | 未完成 | 非 optional 且无文件 → 1644 天全历史; 900s 实测 200/1644, 接口 5.51s/天 |
| `moneyflow` 真实增量 | 未完成 | 同上 (未被执行到); 实测 32.65s/天, 全量约 15 小时 |
| `moneyflow_ind` 真实增量 | 未能验证 | 接口 `[unknown_api]` 未开放; 建议标 optional |
| `daily`/`daily_basic` 20261019 落库 | 未完成 | 15:20 实测接口空返回 (当日数据未发布); 重跑即可, 代码路径已验证为"只补 1 天" |
| 3600s 定时任务整体跑完 | 未达成 | 不是 `index_dailybasic` 造成, 是 12 张缺失表的首次全历史回填; 见第 6 节拆分 |
