# 数据同步指南

本文档是同步相关的完整参考：Tushare 积分要求、全部同步表清单、首次同步顺序和定时调度配置。

## Tushare 积分要求

同步数据需要 [Tushare Pro](https://tushare.pro) Token。各类数据的积分门槛：

| 数据 | 积分要求 |
|---|---|
| 基础行情（日历、基础信息、日线、复权因子、每日指标） | ≥ 2000 |
| `stock_st`（每日 ST 列表） | ≥ 3000 |
| 中信行业成分、`opt_basic`、部分期货扩展数据 | ≥ 5000 |
| ETF 日线行情 `fund_daily` | ≥ 5000（8000 积分频次更高） |
| 基金复权因子 `fund_adj` | ≥ 600（5000 积分以上频次更高） |
| 指数公告 `idx_anns` | ≥ 6000 |
| ETF 基础信息、ETF 份额规模、上交所 ETF 持仓 | ≥ 8000 |

## 首次同步

一键同步全部，或按专题分组同步：

```bash
uv run python main.py sync --all      # 全部
uv run python main.py sync --stock    # 股票核心 + 指数 + 行业
uv run python main.py sync --etf      # ETF 专题
uv run python main.py sync --futures  # 期货
uv run python main.py sync --options  # 期权
```

也可以逐表执行（顺序不可颠倒）。首次同步日线等历史数据量较大，受 Tushare 每分钟调用频次限制影响，全量回填可能需要数小时。

### 批量增量刷新 `sync-bulk`（下载脚本已融合进包）

`scripts/download_all_v2.py` 的增量语义已包内化为 `zer0share/bulk.py`，并注册为 CLI 子命令 `sync-bulk`：

```bash
zer0share sync-bulk                                   # 增量刷新全部 39 张表（默认预算 780s）
zer0share sync-bulk --dry-run                         # 只打印计划：已覆盖到哪天 + 将下载哪些区间，不联网不写盘
zer0share sync-bulk --tables daily,daily_basic        # 只刷指定表
zer0share sync-bulk --end 20261031                    # 指定截止日
zer0share sync-bulk --mode full                       # 原 download_all_v2 全量行为
zer0share sync-bulk --include-missing                 # 也回补本地从未建成的表（全历史，极慢）
zer0share sync-bulk --budget-seconds 3600             # 调整时间预算
```

安装后即有命令（`[project.scripts] zer0share = "zer0share.cli:cli"`，已 `pip install -e .`）：

```bash
uv run python -m zer0share.bulk --dry-run   # 等价入口（不走 click）
```

与 `sync` 并存是有意的：`sync` 走每日分区规范 + MetaStore 记账；`sync-bulk` 走单文件 +
`alt_paths` + `date=*` 分区规范，历史数据都按后者落盘，合并到前者会拆成两个文件。

工程约束（踩坑换来的，勿简化）：

- 覆盖情况只读 parquet footer 统计 + 尾部 row group，绝不整表 pandas 读入
  （`daily_basic` 8.07M×18 整表读入实测 2.98GB RSS，顶爆 cron cgroup 预算）
- 单文件合并走 pyarrow 按 row group 流式读写 + 原子替换，内存只跟单个 row group 成正比
- 沿用旧文件压缩算法（`daily_basic` 原是 polars zstd，改 snappy 文件涨 53%；zstd 显式 level 3）
- 从未建成的表默认不发请求（否则每晚从零回补 1645 个交易日 → 3600s 窗口必超时）；
  `--include-missing` 可显式开启
- 预算耗尽后剩余表标 `DEFERRED(budget)`，exit code 仍按核心表（daily/daily_basic/adj_factor/
  stk_limit/suspend_d/index_daily/index_weight）覆盖判定：齐全 0，有缺口 3

### 股票核心（必选）

```bash
uv run python main.py sync --table trade_cal    # 交易日历（必须最先，8 个交易所）
uv run python main.py sync --table basic        # 股票基础信息
uv run python main.py sync --table daily_kline  # 日线行情（依赖交易日历）
uv run python main.py sync --table adj_factor   # 复权因子（依赖交易日历）
uv run python main.py sync --table daily_basic  # 每日指标：总市值、流通市值等
uv run python main.py sync --table stock_st     # 每日 ST 股票列表
uv run python main.py sync --table suspend_d    # 每日停牌列表
uv run python main.py sync --table stk_limit    # 每日涨跌停价格
uv run python main.py sync --table index_weight # 沪深300/中证500/中证1000成分
uv run python main.py sync --table index_daily  # 宽基指数日线行情
uv run python main.py sync --table idx_anns     # 指数公司公告（自然日同步）
uv run python main.py sync --table industry     # 申万行业分类 + 成分映射
uv run python main.py sync --table ci_member    # 中信行业成分映射
uv run python main.py sync --table sw_daily     # 申万行业指数日线行情
```

### ETF 专题（可选）

```bash
uv run python main.py sync --table etf_basic       # ETF 基础信息
uv run python main.py sync --table etf_index       # ETF 基准指数列表
uv run python main.py sync --table fund_daily      # ETF 日线行情
uv run python main.py sync --table fund_adj        # 基金复权因子
uv run python main.py sync --table etf_share_size  # ETF 份额规模（通常次日 08:30 后更新）
uv run python main.py sync --table etf_sh_cons     # 上交所 ETF 每日持仓组合
```

### 期货扩展（可选，需积分 ≥ 5000）

```bash
uv run python main.py sync --table fut_basic          # 期货合约基础信息
uv run python main.py sync --table fut_daily          # 期货日线行情
uv run python main.py sync --table fut_holding        # 期货持仓排名
uv run python main.py sync --table fut_wsr            # 期货仓单日报
uv run python main.py sync --table fut_settle         # 期货结算参数
uv run python main.py sync --table fut_mapping        # 期货主力与连续合约映射
uv run python main.py sync --table ft_limit           # 期货涨跌停价格
uv run python main.py sync --table fut_weekly         # 期货周线行情
uv run python main.py sync --table fut_monthly        # 期货月线行情
uv run python main.py sync --table fut_index_daily    # 期货指数日线行情
uv run python main.py sync --table fut_weekly_detail  # 期货交易所周度明细
```

### 期权扩展（可选，需积分 ≥ 5000）

```bash
uv run python main.py sync --table opt_basic          # 期权合约基础信息
uv run python main.py sync --table opt_daily          # 期权日线行情
```

### RiceQuant 分钟线（可选，需 RiceQuant 账号）

```bash
uv run python main.py sync --ricequant                          # 按顺序同步全部 RiceQuant 表
uv run python main.py sync --table ricequant_basic              # 合约基础信息
uv run python main.py sync --table ricequant_stock_minute       # 股票分钟线
uv run python main.py sync --table ricequant_etf_basic          # ETF 基础信息
uv run python main.py sync --table ricequant_etf_minute         # ETF 分钟线
```

需要在 `config/settings.toml` 的 `[ricequant]` 里配置 license key 或用户名/密码，详见 [RiceQuant 分钟线文档](ricequant.md)。

## 首次验证建议

先同步一个小区间，确认 Tushare 权限和字段可用后再全量回填：

```bash
uv run python main.py sync --table daily_basic --start-date 20240101 --end-date 20240131
uv run python main.py sync --table stock_st --start-date 20240101 --end-date 20240131
uv run python main.py sync --table suspend_d --start-date 20240101 --end-date 20240131
uv run python main.py sync --table stk_limit --start-date 20240101 --end-date 20240131
uv run python main.py sync --table index_weight --start-date 20240101 --end-date 20240131
uv run python main.py sync --table fut_daily --start-date 20240101 --end-date 20240131
uv run python main.py sync --table ft_limit --start-date 20240101 --end-date 20240131
uv run python main.py sync --table opt_daily --start-date 20240101 --end-date 20240131
```

## 查看同步状态

```bash
uv run python main.py status
```

## 定时调度

每个表在 `config/settings.toml` 的 `[scheduler]` 里单独配置触发时间（`HH:MM`），默认值基于 Tushare 各接口实际入库时间设计：

```toml
[scheduler]
# 凌晨 — 静态参考数据（增量检查，数据已最新时零API消耗）
trade_cal         = "02:00"
basic             = "02:05"
# 盘前 — Tushare 盘前推送
stk_limit         = "09:15"   # 8:40 ready; delayed to avoid early empty responses
adj_factor        = "09:25"   # 9:15~9:20 ready
stock_st          = "09:28"   # 9:20 ready
# 收盘后第一波 — 日线行情（15:00~16:00 ready）
daily_kline       = "16:10"
# 收盘后第二波 — 每日指标及其余数据（3min 间隔，17:05~17:56）
daily_basic       = "17:05"
# ... 完整示例见 config/settings.example.toml
```

同步失败可通过企业微信告警，在 `[notifier]` 里配置 webhook。

### systemd 服务（推荐，服务器常驻）

```bash
# 安装并启用服务
sudo cp scripts/zer0share-scheduler.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable zer0share-scheduler
sudo systemctl start zer0share-scheduler

# 查看状态
sudo systemctl status zer0share-scheduler

# 查看实时日志
journalctl -u zer0share-scheduler -f

# 修改 settings.toml 后重启生效
sudo systemctl restart zer0share-scheduler
```

### 手动启动（调试用）

```bash
uv run python main.py scheduler start
```
