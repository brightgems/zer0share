"""zer0share/bulk.py — scripts/download_all_v2.py 的包内化版本（2026-10-21 融合）。

原脚本是独立文件：自带 tushare 客户端、硬编码 TOKEN/HTTP_URL/输出路径、print 输出。
融合进包后能力与增量语义一字不改，只把「硬编码常量」换成「可配置」：

  * TOKEN / HTTP_URL  → config/settings.toml 的 [tushare] 段（不再硬编码凭证）
  * 输出路径          → [paths].data_dir 派生（stock/macro/us/hk/index）
  * print             → loguru（走 zer0share.logging 初始化）
  * CLI               → `zer0share sync-bulk` 子命令（argparse 原样保留，
                        与逐表 `sync` 命令并存）

两套命令并存是有意的：
  `zer0share sync`      走 zer0share.sync 的每日分区规范 + MetaStore 记账
  `zer0share sync-bulk` 走 download_all_v2 的单文件 + alt_paths + date=* 分区规范，
                        历史数据都按后者落盘，合并到前者会拆成两个文件。

保留原脚本的关键工程约束（都是踩坑换来的，勿简化）：
  * 覆盖情况一律读 parquet footer 统计 + 尾部 row group，绝不整表 pandas 读入
    （daily_basic 8.07M×18 整表读入实测 2.98GB RSS，顶爆 cron cgroup 预算）
  * 单文件合并走 pyarrow 按 row group 流式读写，内存只跟单 row group 成正比
  * 沿用旧文件压缩算法（daily_basic 原是 polars zstd，改 snappy 文件涨 53%）
  * 从未建成的表默认不发请求，否则每晚从零回补 1645 个交易日 → 3600s 窗口必超时
  * 预算耗尽后剩余表标 DEFERRED(budget)，exit code 仍按核心表覆盖判定
"""

from __future__ import annotations

import argparse
import os
import sys
import time
import traceback
from datetime import datetime, timedelta

import pandas as pd
import tushare as ts
from loguru import logger

from zer0share.config import Config, load_config

# ---------------------------------------------------------------- 默认常量
START = "20200101"
END = "20260930"  # full 模式默认截止日（保持原行为）

DEFAULT_BUDGET_SECONDS = 780.0  # 增量模式默认时间预算；留出 3600s 定时窗口的余量
QUARTER_LAG_DAYS = 45  # 财报披露滞后：只把距今超过该天数的最近季末视为"预期已出齐"

# 执行顺序：核心表优先，其余按注册顺序跟后
PRIORITY = [
    "daily", "daily_basic", "adj_factor", "stk_limit", "suspend_d",
    "moneyflow", "moneyflow_ind", "stock_st", "index_daily", "index_weight",
    "trade_cal",
]
# 核心表：必须每次跑到 --end（或最近交易日），有缺口 → exit 3。
# moneyflow/moneyflow_ind 在 PRIORITY 里排位靠前（设计指定顺序），但本地从未建成 →
# 默认被 missing 规则跳过，且不列入核心判定（否则没有本地数据也永远算缺口，每晚固定 exit 3）。
CORE_TABLES = (
    "daily", "daily_basic", "adj_factor", "stk_limit", "suspend_d",
    "index_daily", "index_weight",
)

_DEADLINE: float | None = None  # time.time() 形式的预算截止点；None = 不限制
READ_FAILED = object()  # read_old_safe 的哨兵：现有文件存在但读不出来

# ---------------------------------------------------------------- 主题分组
# 对齐 sync 命令的 --stock/--futures/--options/--etf 主题开关, 让 sync-bulk 也能按主题下载。
# 注意两套命令的**表名与落盘规范不同** (sync 走每日分区+MetaStore, sync-bulk 走单文件),
# 这里列的是 sync-bulk 侧的表名, 与 sync 的 STOCK_TABLES 等不是同一套。
BULK_TOPICS: dict[str, list[str]] = {
    "quote": ["daily", "weekly", "monthly", "adj_factor", "daily_basic", "stk_limit", "suspend_d"],
    "fundflow": ["moneyflow", "moneyflow_ind", "moneyflow_hsgt", "margin_detail", "hk_hold"],
    "toplist": ["top_list", "top_inst"],
    "finance": ["income", "balancesheet", "cashflow", "fina_indicator", "dividend",
                "forecast", "stk_holdernumber", "top10_holders", "top10_floatholders",
                "stk_managers", "stk_rewards"],
    "index": ["index_daily", "index_weight", "index_dailybasic", "sw_daily"],
    "bond": ["cb_basic", "cb_daily"],
    "fund": ["fund_nav", "fund_portfolio", "fund_share"],
    "us": ["us_daily"],
    "macro": ["shibor", "repurchase", "repo_daily"],
}
# 全部主题合并 (去重, 保持注册顺序), 用于 --topic all 与未知主题提示
ALL_TOPICS = list(BULK_TOPICS)

INDEX_CODES = [
    "000001.SH", "399001.SZ", "399006.SZ", "000300.SH", "000905.SH",
    "000688.SH", "399005.SZ", "000016.SH", "000852.SH",
]
SW_CODES = [f"801{i:03d}.SI" for i in range(1, 32)]
US_STOCKS = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "META", "NFLX"]

DEFAULT_FEATURES_DIR = "/quantdata/quantdb/6_ml_datasets/features_daily"

TRADE_CAL_NAME = "trade_cal.parquet"  # 列: exchange/cal_date/is_open/pretrade_date
TRADE_CAL_DIR = "trade_cal"  # 分区日历 (覆盖更长区间, 作为本地二级来源)

# 模块级共享客户端；make_pro() 每次调用复用同一实例（原脚本在模块导入时建）
_PRO = None


class BulkSyncError(RuntimeError):
    """bulk 同步失败（配置缺失、目录不可写等）。"""


# ---------------------------------------------------------------- 目录 / 客户端
def dirs_for(cfg: Config) -> dict[str, str]:
    """各市场输出目录（由 [paths].data_dir 派生）。"""
    base = str(cfg.data_dir)
    return {
        "OUTPUT": os.path.join(base, "stock"),
        "MACRO_OUTPUT": os.path.join(base, "macro"),
        "US_OUTPUT": os.path.join(base, "us"),
        "HK_OUTPUT": os.path.join(base, "hk"),
        "INDEX_OUTPUT": os.path.join(base, "index"),
    }


def ensure_dirs(dirs: dict[str, str]) -> None:
    for d in dirs.values():
        os.makedirs(d, exist_ok=True)


def make_pro(cfg: Config):
    """按 config/settings.toml 的 [tushare] 段建 pro 客户端（代理地址可选）。"""
    global _PRO
    if _PRO is not None:
        return _PRO
    if not cfg.tushare_token:
        raise BulkSyncError("config/settings.toml 缺少 [tushare].token")
    pro = ts.pro_api(cfg.tushare_token)
    if cfg.tushare_http_url:
        pro._DataApi__http_url = cfg.tushare_http_url
    _PRO = pro
    return pro


# ---------------------------------------------------------------- 基础工具
def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    """对象列清洗: 空串→None, 能整体转数值的列转成数值, 否则统一为字符串"""
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].replace("", None)
            # pandas 2.2+/3.0 弃用 pd.to_numeric(errors='ignore'), 用 try/except 复刻其语义
            try:
                converted = pd.to_numeric(df[col])
            except (ValueError, TypeError):
                converted = df[col]
            if converted.dtype != "object":
                df[col] = converted
            else:
                df[col] = df[col].astype(str)
    return df


def save(df, path, name) -> bool:
    if df is None or len(df) == 0:
        logger.info(f"  ⚠️ {name}: 0 rows, skipped")
        return False
    clean_df(df)
    df.to_parquet(path, index=False)
    sz = os.path.getsize(path) / 1e6
    logger.info(f"  ✅ {name}: {len(df):,} rows, {sz:.1f} MB → {path}")
    return True


def write_atomic(df, path, name, expect_stat=None) -> bool:
    """增量合并结果原子落盘: 先写 path.tmp 再 os.replace。

    expect_stat = 读取旧文件前的 (st_mtime_ns, st_size); 若写回前文件被其他进程改过
    则放弃写入，避免覆盖并发下载进程正在写的数据。
    """
    if expect_stat is not None:
        cur = file_stat(path)
        if cur != expect_stat:
            logger.warning(f"  ⚠️ {name}: 文件在处理期间被其他进程修改, 放弃写入以免覆盖")
            return False
    tmp = f"{path}.tmp"
    try:
        df.to_parquet(tmp, index=False)
        os.replace(tmp, path)
    except Exception:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass
        raise
    sz = os.path.getsize(path) / 1e6
    logger.info(f"  ✅ {name}: {len(df):,} rows, {sz:.1f} MB → {path} (原子写入)")
    return True


def skip_if_exists(path, name) -> bool:
    """Skip download if file already exists and is non-trivial size"""
    if os.path.exists(path) and os.path.getsize(path) > 1000:
        sz = os.path.getsize(path) / 1e6
        logger.info(f"  ⏭️ {name}: already exists ({sz:.1f} MB), skipping")
        return True
    return False


def read_parquet(path, columns=None):
    try:
        return pd.read_parquet(path, columns=columns)
    except Exception as e:
        logger.info(f"  ⚠️ 读取失败 {path}: {str(e)[:100]}")
        return None


def parquet_columns(path):
    """只读 footer schema 拿列名, 不物化任何数据"""
    try:
        import pyarrow.parquet as pq
        return list(pq.ParquetFile(path).schema_arrow.names)
    except Exception:
        try:
            return list(pd.read_parquet(path).columns)
        except Exception:
            return None


# ---------------------------------------------------------------- 交易日历
_CAL_CACHE: dict[bool, list[str] | None] = {}


def _cal_from_df(df) -> list[str]:
    """cal_date/is_open → 升序交易日 list"""
    if df is None or len(df) == 0 or "cal_date" not in df.columns:
        raise ValueError("缺少 cal_date 列或为空")
    s = df["cal_date"].astype(str)
    if "is_open" in df.columns:
        mask = df["is_open"].astype(str).str.lower().isin(["1", "true"])
        s = s[mask]
    dates = sorted(set(s))
    if not dates:
        raise ValueError("无交易日")
    return dates


def _load_partitioned_cal(dirs):
    """读分区日历 (trade_cal/exchange=*/data.parquet)。整目录读会因各交易所 schema
    不一致失败，所以逐文件读; A 股交易日以 exchange=SSE 为准。"""
    import glob

    root = os.path.join(dirs["OUTPUT"], TRADE_CAL_DIR)
    files = sorted(glob.glob(os.path.join(root, "exchange=SSE", "*.parquet"))) or \
        sorted(glob.glob(os.path.join(root, "*", "*.parquet")))
    if not files:
        raise ValueError("无分区日历文件")
    parts = [pd.read_parquet(f, columns=["cal_date", "is_open"]) for f in files]
    return _cal_from_df(pd.concat(parts, ignore_index=True))


def load_trade_calendar(dirs, full=False):
    """优先读 trade_cal.parquet; full=True 时再尝试同目录的分区日历
    (trade_cal/exchange=*, 覆盖区间更长), 都不可用返回 None。"""
    key = bool(full)
    if key in _CAL_CACHE:
        return _CAL_CACHE[key]
    path = os.path.join(dirs["OUTPUT"], TRADE_CAL_NAME)
    cal = None
    try:
        cal = _cal_from_df(pd.read_parquet(path))
        if not key:
            logger.info(f"  📅 交易日历: {path} ({cal[0]} ~ {cal[-1]}, {len(cal)} 天)")
    except Exception as e:
        logger.info(f"  ⚠️ 本地交易日历不可用 ({str(e)[:80]}), 退回 pd.bdate_range")
    if key:
        try:
            ext = _load_partitioned_cal(dirs)
            cal = sorted(set(cal or []) | set(ext))
            logger.info(f"  📅 合并分区日历 {os.path.join(dirs['OUTPUT'], TRADE_CAL_DIR)} → 覆盖至 {cal[-1]} ({len(cal)} 天)")
        except Exception:
            pass
    _CAL_CACHE[key] = cal
    return cal


def _end_of(cal):
    return cal[-1] if cal else "00000000"


def get_trade_dates(start, end) -> list[str]:
    """(原行为, full 模式沿用) 工作日近似日期列表"""
    return [d.strftime("%Y%m%d") for d in pd.bdate_range(start, end)]


def fetch_cal_online(pro, start, end):
    try:
        df = pro.trade_cal(exchange="SSE", start_date=start, end_date=end)
        return _cal_from_df(df)
    except Exception as e:
        logger.info(f"  ⚠️ 在线交易日历获取失败 ({str(e)[:60]}), 退回 pd.bdate_range")
        return None


def plan_trade_dates(dirs, start, end, use_api=False, pro=None):
    """增量计划用交易日 (闭区间 yyyymmdd, 升序)。
    优先本地交易日历; 只有日历本身不覆盖时才依次尝试: 在线 pro.trade_cal (非 dry-run)
    → pd.bdate_range。返回 (dates, note); note 非 None 说明尾段是近似值。
    """
    if start and end and start > end:
        return [], None
    cal = load_trade_calendar(dirs)
    if not cal:
        return get_trade_dates(start, end), "无本地日历, 全部按工作日近似"
    head = [d for d in cal if start <= d <= end]
    cal_max = _end_of(cal)
    if end <= cal_max:
        return head, None
    tail_start = start if start > cal_max else \
        (datetime.strptime(cal_max, "%Y%m%d") + timedelta(days=1)).strftime("%Y%m%d")
    if tail_start > end:
        return head, None
    ext = load_trade_calendar(dirs, full=True)
    if ext is not None and _end_of(ext) >= tail_start:
        tail = [d for d in ext if tail_start <= d <= end]
        return sorted(set(head) | set(tail)), None
    if use_api and pro is not None:
        tail = fetch_cal_online(pro, tail_start, end)
        if tail is not None:
            return sorted(set(head) | set(tail)), None
    tail = get_trade_dates(tail_start, end)
    return sorted(set(head) | set(tail)), \
        f"本地日历覆盖至{cal_max}, 其后{len(tail)}天按工作日近似(节假日以接口空返回兜底)"


def last_trading_day(dirs, end, use_api=False, pro=None):
    lo = (datetime.strptime(end, "%Y%m%d") - timedelta(days=20)).strftime("%Y%m%d")
    dates, _ = plan_trade_dates(dirs, lo, end, use_api=use_api, pro=pro)
    return dates[-1] if dates else end


def latest_expected_quarter(end, lag=QUARTER_LAG_DAYS):
    """距今超过 lag 天的最近季末 — 该季报原则上已披露完毕"""
    ref = datetime.strptime(end, "%Y%m%d").date() - timedelta(days=lag)
    p = pd.Period(ref, freq="Q")
    qe = p.end_time.date()
    if qe > ref:
        qe = (p - 1).end_time.date()
    return qe.strftime("%Y%m%d")


def quarter_ends(start, end) -> list[str]:
    """季度末日期列表 (原 fund_portfolio 的 period 取值)"""
    return [d.strftime("%Y%m%d") for d in pd.period_range(start, end, freq="Q").to_timestamp(how="end")]


def next_day(d):
    return (datetime.strptime(d, "%Y%m%d") + timedelta(days=1)).strftime("%Y%m%d")


def days_ago(d, n):
    return (datetime.strptime(d, "%Y%m%d") - timedelta(days=n)).strftime("%Y%m%d")


# ---------------------------------------------------------------- 预算
def budget_left():
    """剩余预算秒数; None = 未设预算"""
    return None if _DEADLINE is None else _DEADLINE - time.time()


def budget_exhausted():
    left = budget_left()
    return left is not None and left <= 0


# ---------------------------------------------------------------- 下载批量器
def call_api(pro, fn, params, retries=2, backoff=3.0):
    """带重试的接口调用 (代理偶发 ReadTimeout 属正常); 返回 (df, err)。
    成功 → (df, None); 最终失败 → (None, 最后一次异常)。
    err 让上层区分「当天数据还没出」和「凭证/网络故障」—— 两者处理方式完全不同:
    前者是正常等待, 后者要告警。旧版一律返回 None, 定时任务会在数据源故障时静默失败。
    预算耗尽时不再重试, 避免拖过定时窗口。"""
    last = None
    for k in range(retries + 1):
        try:
            return fn(**params), None
        except Exception as e:
            last = e
            if k >= retries or budget_exhausted():
                return None, last
            time.sleep(backoff * (k + 1))
    return None, last


def _err_key(err: BaseException) -> str:
    """把异常压成一个可读的错误分类键, 供批量汇总。

    tushare 的错误码在消息里 (如 upstream_pool_exhausted / unauthorized),
    比异常类名有信息量得多: 前者说明当天数据还没发布, 后者说明凭证失效要换 key。
    """
    msg = str(err)
    low = msg.lower()
    # tushare 错误码形如 "[upstream_pool_exhausted] 数据当前不可用..."
    if "[" in msg and "]" in msg:
        code = msg[msg.index("[") + 1:msg.index("]")].strip()
        if code and " " not in code:
            return code
    for kw, label in [("unauthorized", "unauthorized(凭证/Token无效)"),
                      ("token已过期", "token已过期"),
                      ("token", "token(凭证/Token)"),
                      ("timeout", "timeout(网络)"),
                      ("connection", "connection(网络)")]:
        if kw in low:
            return label
    return f"{type(err).__name__}: {msg[:60]}"


def batch_by_date(pro, api_name, date_col="trade_date", name="", sleep=0.15, dates=None,
                  paginate=False, page_size=5000, max_pages=200):
    """按日期批量下载 (dates=None 时沿用原 START~END 工作日序列)
    返回 (df, err_summary): df=None 时 err_summary 非空, 形如
    "N/M 天失败: upstream_pool_exhausted x1, ..." — 调用方据此区分
    「数据未就绪」(upstream_pool_exhausted / 空返回) 与「凭证/网络故障」。

    paginate=True: 单日 offset 分页拉全量。moneyflow 一类接口单次上限 5000 行,
    而全市场日资金流约 5575 只 (实测), 不分页每天静默丢 575 行 ≈ 10%。
    """
    if dates is None:
        dates = get_trade_dates(START, END)
    all_dfs = []
    empty = 0
    errs: dict[str, int] = {}

    def _fetch_day(d):
        """单日数据: 不分页直接返回; 分页则 offset 循环直至取空。"""
        if not paginate:
            return call_api(pro, getattr(pro, api_name), {date_col: d})
        parts, offset, err = [], 0, None
        for _ in range(max_pages):
            if budget_exhausted():
                break
            df, err = call_api(pro, getattr(pro, api_name),
                               {date_col: d, "offset": offset, "limit": page_size})
            if err is not None:
                return None, err
            if df is None or len(df) == 0:
                break
            parts.append(df)
            if len(df) < page_size:  # 取满才可能有下一页
                break
            offset += len(df)
            time.sleep(sleep)
        if not parts:
            return None, err
        return pd.concat(parts, ignore_index=True), None

    for i, d in enumerate(dates):
        if budget_exhausted():
            logger.info(f"  ⏹ 预算耗尽: 中断 {name or api_name} 的下载 (已完成 {i}/{len(dates)} 天)")
            break
        df, err = _fetch_day(d)
        if err is not None:
            key = _err_key(err)
            errs[key] = errs.get(key, 0) + 1
        elif df is not None and len(df) > 0:
            all_dfs.append(df)
        else:
            empty += 1
        time.sleep(sleep)
        if (i + 1) % 200 == 0:
            total = sum(len(x) for x in all_dfs)
            logger.info(f"  ...{d} ({total:,} rows, {i + 1}/{len(dates)})")
    summary = ""
    if errs:
        detail = ", ".join(f"{k} x{v}" for k, v in sorted(errs.items(), key=lambda kv: -kv[1]))
        summary = f"{sum(errs.values())}/{len(dates)} 天接口报错: {detail}"
    if not all_dfs:
        if summary:
            return None, summary
        return None, (f"接口全部返回空 ({empty}/{len(dates)} 天, 数据可能尚未发布)"
                      if empty else "无日期需要下载")
    if summary:
        summary += f"; 其余 {len(all_dfs)} 天成功"
    return pd.concat(all_dfs, ignore_index=True), summary


def batch_by_stock(pro, api_name, stock_list, name="", sleep=0.15, **extra):
    """按股票批量下载"""
    all_dfs = []
    for i, code in enumerate(stock_list):
        try:
            params = {"ts_code": code, **extra}
            df = getattr(pro, api_name)(**params)
            if df is not None and len(df) > 0:
                all_dfs.append(df)
        except Exception:
            pass
        time.sleep(sleep)
        if (i + 1) % 500 == 0:
            total = sum(len(x) for x in all_dfs)
            logger.info(f"  ...{code} ({total:,} rows, {i + 1}/{len(stock_list)})")
    if all_dfs:
        return pd.concat(all_dfs, ignore_index=True)
    return None


def batch_by_stock_window(pro, api_name, windows, name="", sleep=0.15, **extra):
    """按 (ts_code, start_date, end_date) 窗口批量下载; 接口不接受 start/end 时
    自动退回该 code 整段历史。
    返回 (df, err_summary): 失败时 err_summary 形如 "3/9 代码失败: ..."，便于上层区分
    「数据未就绪」与「凭证/网络故障」。"""
    all_dfs = []
    empty = 0
    errs: dict[str, int] = {}
    for i, (code, s, e) in enumerate(windows):
        if budget_exhausted():
            logger.info(f"  ⏹ 预算耗尽: 中断 {name or api_name} 的下载 (已完成 {i}/{len(windows)} 个代码)")
            break
        df, err = None, None
        try:
            df = getattr(pro, api_name)(**{"ts_code": code, "start_date": s, "end_date": e, **extra})
        except Exception as e1:
            try:
                df = getattr(pro, api_name)(**{"ts_code": code, **extra})
            except Exception as e2:
                df, err = None, e2
        if err is not None:
            k = _err_key(err)
            errs[k] = errs.get(k, 0) + 1
        elif df is not None and len(df) > 0:
            all_dfs.append(df)
        else:
            empty += 1
        time.sleep(sleep)
        if (i + 1) % 200 == 0:
            total = sum(len(x) for x in all_dfs)
            logger.info(f"  ...{code} ({total:,} rows, {i + 1}/{len(windows)})")
    summary = ""
    if errs:
        detail = ", ".join(f"{k} x{v}" for k, v in sorted(errs.items(), key=lambda kv: -kv[1]))
        summary = f"{sum(errs.values())}/{len(windows)} 个代码接口报错: {detail}"
    if not all_dfs:
        if summary:
            return None, summary
        return None, (f"接口全部返回空 ({empty}/{len(windows)} 个代码, 数据可能尚未发布)"
                      if empty else "无代码需要下载")
    if summary:
        summary += f"; 其余 {len(all_dfs)} 个代码成功"
    return pd.concat(all_dfs, ignore_index=True), summary


def fetch_fund_portfolio(pro, quarters, sleep=0.15):
    """按季度下载, 每季度内 offset 分页 (单次上限约2000行)"""
    all_fp = []
    for q in quarters:
        offset = 0
        while True:
            try:
                df = pro.fund_portfolio(period=q, offset=offset, limit=2000)
            except Exception:
                df = None
            if df is None or len(df) == 0:
                break
            all_fp.append(df)
            offset += len(df)
            time.sleep(sleep)
            if offset >= 60000:  # 安全上限
                break
        total = sum(len(x) for x in all_fp)
        logger.info(f"  ...{q} ({total:,} rows)")
    if all_fp:
        return pd.concat(all_fp, ignore_index=True)
    return None


# ---------------------------------------------------------------- 本地文件 / 分区
def resolve_path(spec):
    """读写同一份文件: 优先脚本目标路径; 目标不存在而已有历史落盘位置时, 直接以该位置
    为准 (避免同一张表被增量下载拆成两个文件), 都不存在时用目标路径新建。
    返回 (路径, 是否回退)。"""
    p = spec["path"]
    if os.path.exists(p):
        return p, None
    for alt in spec.get("alt_paths", []):
        if os.path.exists(alt):
            return alt, alt
    return p, None


def file_stat(path):
    try:
        st = os.stat(path)
        return (st.st_mtime_ns, st.st_size)
    except OSError:
        return None


def dedup_keys(new, spec):
    """业务主键: 优先 spec['key'] 中实际存在的列; 否则退回 (ts_code, trade_date)/trade_date 规则"""
    keys = [c for c in spec.get("key", []) if c in new.columns]
    if keys:
        return keys
    date_col = spec.get("date_col", "trade_date")
    return (["ts_code"] if "ts_code" in new.columns else []) + \
           ([date_col] if date_col in new.columns else [])


def _is_text(dt):
    return dt.kind in ("O", "U", "S") or pd.api.types.is_string_dtype(dt)


def _is_num(dt):
    return dt.kind in ("i", "u", "f")


def match_dtypes(new, old):
    """对齐新旧同名列的 dtype。
    现有文件多把 trade_date/日期列存成字符串, 而新下载数据经 clean_df 后会变成 int64 —
    直接 concat 会让同一列混着 int 与 str, 写 parquet 时报 ArrowInvalid。"""
    for col in new.columns:
        if col not in old.columns:
            continue
        ao, an = old[col].dtype, new[col].dtype
        if ao == an:
            continue
        ao_text, an_text = _is_text(ao), _is_text(an)
        ao_num, an_num = _is_num(ao), _is_num(an)
        if an_num and ao_num:
            # 新列浮点而旧列整型, 且新列确有小数 → 旧列升为浮点, 否则新列降为整型
            if an.kind == "f" and ao.kind == "i" and not new[col].dropna().mod(1).eq(0).all():
                old[col] = old[col].astype(float)
            else:
                new[col] = new[col].astype(ao)
        elif an_num and ao_text:
            new[col] = new[col].map(str)  # 旧行是字符串 → 新行对齐成字符串
        elif an_text and ao_num:
            conv = pd.to_numeric(new[col], errors="coerce")
            if isinstance(conv, pd.Series) and len(conv) and conv.notna().all():
                new[col] = conv.astype(ao)  # 新列可整体转数值 → 对齐旧列
            else:
                old[col] = old[col].map(str)  # 否则两列统一成字符串
    return new


def merge_rows(old, new, spec, name):
    """新旧数据 concat → 按业务主键去重(保留新下载的一份, 容忍重述) → 按日期排序"""
    keys = dedup_keys(new, spec)
    if old is not None and len(old):
        new = match_dtypes(new, old)
        merged = pd.concat([old, new], ignore_index=True)
        merged = merged.drop_duplicates(subset=keys, keep="last")
    else:
        merged = new
    date_col = spec.get("date_col", "trade_date")
    sort_cols = [c for c in [date_col] + keys if c in merged.columns]
    if not sort_cols:
        sort_cols = [c for c in merged.columns[:2]]
    return merged.sort_values(sort_cols).reset_index(drop=True)


def partition_dates(root):
    """<root>/date=YYYYMMDD/ 分区目录里的日期 (yyyymmdd 升序); 目录不存在返回 []"""
    try:
        names = os.listdir(root)
    except OSError:
        return []
    out = []
    for n in names:
        if not n.startswith("date="):
            continue
        raw = n[5:].replace("-", "")
        if len(raw) == 8 and raw.isdigit():
            out.append(raw)
    return sorted(out)


def partition_dir_of(spec, dirs):
    """日期表的分区目录 (仅当目录里确有 date=* 分区时返回) — adj_factor 的历史只以分区形式存在,
    只查 <name>.parquet 会误判为『文件不存在』→ 触发 1645 天全历史回补。"""
    root = spec.get("partition_dir") or os.path.join(dirs["OUTPUT"], spec["name"])
    return root if partition_dates(root) else None


def parquet_max_date(path, col="trade_date"):
    """只读 row-group 统计拿最大日期 (不整表 collect)"""
    import pyarrow.parquet as pq

    try:
        pf = pq.ParquetFile(path)
    except Exception:
        return None
    if col not in pf.schema_arrow.names:
        return None
    idx = pf.schema_arrow.names.index(col)
    mx = None
    for rg in range(pf.metadata.num_row_groups):
        st = pf.metadata.row_group(rg).column(idx).statistics
        if st is None or not st.has_min_max:
            continue
        v = str(st.max).replace("-", "")[:8]
        if len(v) == 8 and v.isdigit() and (mx is None or v > mx):
            mx = v
    return mx


def table_max_date(spec, dirs):
    """该表本地覆盖到的最新日期 (单文件 → alt_paths → date=* 分区), 用于核心覆盖判定"""
    if spec["kind"] not in ("date", "code", "stock"):
        return None
    p, _alt = resolve_path(spec)
    mx = parquet_max_date(p, spec.get("date_col", "trade_date")) if os.path.exists(p) else None
    if mx:
        return mx
    pdates = partition_dates(partition_dir_of(spec, dirs))
    return pdates[-1] if pdates else None


def has_local_data(spec, dirs):
    """本地是否有该表的任何数据 (单文件 >1KB 或 有 date=* 分区); 没有任何数据 = 从未建成"""
    p, _alt = resolve_path(spec)
    if os.path.exists(p) and os.path.getsize(p) > 1000:
        return True
    return bool(partition_dates(partition_dir_of(spec, dirs)))


def write_partitions(df, root, spec, name):
    """把按日下载的数据写成 <root>/date=YYYYMMDD/data.parquet 分区 (adj_factor 的活布局)。
    已存在的分区按业务主键去重合并, 保证可重跑。"""
    date_col = spec.get("date_col", "trade_date")
    if df is None or len(df) == 0 or date_col not in df.columns:
        logger.info(f"  ⚠️ {name}: 无可写分区数据 ({date_col} 列缺失或空)")
        return 0
    keys = dedup_keys(df, spec)
    dcol = df[date_col].astype(str).str.replace("-", "").str.slice(0, 8)
    wrote = rows = 0
    for d, grp in df.groupby(dcol, sort=True):
        dst_dir = os.path.join(root, f"date={d}")
        os.makedirs(dst_dir, exist_ok=True)
        fp = os.path.join(dst_dir, "data.parquet")
        try:
            if os.path.exists(fp):
                old = read_parquet(fp)
                if old is None:
                    logger.info(f"  ⚠️ {name}: 分区 {d} 旧文件读不出, 跳过该日")
                    continue
                grp = match_dtypes(grp, old)
                merged = pd.concat([old, grp], ignore_index=True) \
                    .drop_duplicates(subset=keys, keep="last")
            else:
                merged = grp
            tmp = fp + ".tmp"
            merged.to_parquet(tmp, index=False)
            os.replace(tmp, fp)
            wrote += 1
            rows += len(merged)
        except Exception as e:
            logger.info(f"  ⚠️ {name}: 分区 {d} 写入失败 {str(e)[:80]}")
    logger.info(f"  ✅ {name}: {rows:,} 行 → {wrote} 个 date=* 分区 ({root})")
    return wrote


def write_streamed(path, new, spec, name):
    """低内存合并回写单文件日期表: 按 footer/row-group 流式遍历旧文件 (pyarrow
    ParquetFile.iter_batches), 丢掉被重下的日期, 追加新行, 用 ParquetWriter 边读边写。
    不走 pandas 整表 load: daily_basic(8.07M×18) 实测整表读入 2.98GB RSS, 超 cron cgroup 预算;
    即使是 polars concat+sink 也会把整表物化 (实测 2.4GB)。这里内存只跟 batch 大小成正比。
    原子替换 (先写 path.tmp 再 os.replace); 旧文件读不出时放弃写入, 绝不覆盖。"""
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    date_col = spec.get("date_col", "trade_date")
    keys = dedup_keys(new, spec)
    nd = new.copy()
    nd[date_col] = nd[date_col].astype(str).str.replace("-", "").str.slice(0, 8)
    nd = nd.drop_duplicates(subset=keys, keep="last")
    new_dates = sorted(nd[date_col].unique().tolist())
    try:
        pf = pq.ParquetFile(path)
        schema = pf.schema_arrow
        old_rows = pf.metadata.num_rows
        if date_col not in schema.names:
            logger.info(f"  ❌ {name}: 现有文件缺 {date_col} 列, 放弃写入")
            return False
    except Exception as e:
        logger.info(f"  ❌ {name}: 现有文件不可读 ({str(e)[:80]}), 放弃写入以免覆盖")
        return False
    # 新行对齐旧 schema (列序/类型; 旧文件缺的列补 null, 多出的列丢弃)
    tbl = pa.Table.from_pandas(nd, preserve_index=False)
    have = {f.name: tbl.column(f.name) for f in schema if f.name in tbl.column_names}
    cols = []
    for f in schema:
        c = have.get(f.name)
        if c is None:
            cols.append(pa.nulls(len(tbl), type=f.type))
            continue
        if c.type != f.type:
            try:
                c = c.cast(f.type, safe=False)
            except Exception:
                c = pc.cast(c, pa.string()) if pa.types.is_string(f.type) else c
        cols.append(c)
    new_tbl = pa.table(cols, schema=schema)
    tmp = f"{path}.tmp"
    dropped = 0
    # 按旧文件 row group 逐块读写: 内存只跟单个 row group 成正比
    # (daily_basic 的 row group = 100k 行 ≈ 60MB; 用 iter_batches 小批次反而因读缓冲不释放冲到 904MB)。
    rg_rows = 1_000_000
    comp = "snappy"
    try:
        rg_rows = max(pf.metadata.row_group(i).num_rows for i in range(pf.metadata.num_row_groups))
        comp = (pf.metadata.row_group(0).column(0).compression or "snappy").lower()
    except Exception:
        pass
    # 沿用旧文件的压缩算法: daily_basic 原是 polars ZSTD, 若改成 snappy 文件会涨 53%;
    # zstd 显式 level 3 (默认 level 1 会再涨 20%)
    if comp == "zstd":
        writer = pq.ParquetWriter(tmp, schema, compression="zstd", compression_level=3)
    else:
        writer = pq.ParquetWriter(tmp, schema, compression=comp)
    try:
        in_new = pa.array(new_dates, type=pa.string())
        for i in range(pf.metadata.num_row_groups):
            t = pf.read_row_group(i)
            if date_col in t.column_names:
                hit = pc.is_in(t.column(date_col).cast(pa.string()), value_set=in_new)
                ndrop = pc.sum(hit).as_py() or 0
                if ndrop:
                    dropped += ndrop
                    t = t.filter(pc.invert(hit))
            if t.num_rows:
                writer.write_table(t, row_group_size=rg_rows)
            del t
        writer.write_table(new_tbl, row_group_size=rg_rows)
    finally:
        writer.close()
    os.replace(tmp, path)
    total = old_rows - dropped + new_tbl.num_rows
    sz = os.path.getsize(path) / 1e6
    logger.info(f"  ✅ {name}: 追加 {new_tbl.num_rows:,} 行 ({new_dates[0]}~{new_dates[-1]}, "
                f"替换旧行 {dropped:,}) → 共 {total:,} 行, {sz:.1f} MB → {path} (流式合并+原子替换)")
    return True


# ---------------------------------------------------------------- 覆盖情况检查
def coverage_of_date_table(spec, path, cols):
    """返回 (max_trade_date, 当日股票数, 总行数)。
    只读 footer 统计 + 尾部 row group, 不整表读入 — daily_basic 整表读入实测 2.98GB RSS,
    会顶爆 cron worker 的 cgroup 预算。"""
    date_col = spec.get("date_col", "trade_date")
    if not cols or date_col not in cols:
        return None, None, 0
    import pyarrow.parquet as pq

    try:
        pf = pq.ParquetFile(path)
    except Exception as e:
        logger.info(f"  ⚠️ 读取失败 {path}: {str(e)[:100]}")
        return None, None, 0
    total = pf.metadata.num_rows
    mx = parquet_max_date(path, date_col)
    if not mx:
        return None, None, total
    n_last = None
    if "ts_code" in pf.schema_arrow.names:
        codes = set()
        for rg in range(max(0, pf.metadata.num_row_groups - 2), pf.metadata.num_row_groups):
            t = pf.read_row_group(rg, columns=[date_col, "ts_code"])
            ds = [str(v).replace("-", "")[:8] for v in t.column(date_col).to_pylist()]
            cs = t.column("ts_code").to_pylist()
            codes |= {c for d, c in zip(ds, cs) if d == mx}
        n_last = len(codes) if codes else None
    return mx, n_last, total


def coverage_of_stock_table(spec, path, cols):
    """per ts_code 的最新覆盖日期 (公告日期优先, 缺失时回退后续候选列)"""
    if not cols or "ts_code" not in cols:
        return {}
    cov_cols = [c for c in spec["coverage"] if c in cols]
    if not cov_cols:
        return {}
    cur = read_parquet(path, ["ts_code"] + cov_cols)
    if cur is None or len(cur) == 0:
        return {}
    key = pd.to_numeric(cur[cov_cols[0]], errors="coerce")
    for c in cov_cols[1:]:  # 首选列为空的行回退到后续候选列
        key = key.fillna(pd.to_numeric(cur[c], errors="coerce"))
    out = cur.assign(_k=key).groupby("ts_code")["_k"].max().dropna()
    return {str(k): f"{int(v):08d}" for k, v in out.items()}


def coverage_of_code_table(spec, path, cols):
    """per ts_code 的最新 trade_date (指数日线/申万行业日线)"""
    if not cols or "trade_date" not in cols:
        return {}
    cur = read_parquet(path, ["ts_code", "trade_date"])
    if cur is None or len(cur) == 0:
        return {}
    out = cur.groupby("ts_code")["trade_date"].max()
    return {str(k): str(v) for k, v in out.items()}


# ---------------------------------------------------------------- 股票代码来源
def codes_for(pro, spec, args, ctx, dirs):
    """股票类表的代码清单: 本地优先 (dry-run 绝不联网), 缺失时非 dry-run 才在线取"""
    src = spec.get("codes_from", "stock_basic")
    key = "cb" if src == "cb_basic" else "all"
    if key in ctx:
        return ctx[key]
    codes = []
    if src == "stock_basic":
        p = os.path.join(dirs["OUTPUT"], "stock_basic.parquet")
        if os.path.exists(p):
            df = read_parquet(p, ["ts_code"])
            if df is not None:
                codes = [str(c) for c in df["ts_code"].dropna().unique().tolist()]
        if not codes and not args.dry_run:
            df = pro.stock_basic(exchange="", list_status="L", fields="ts_code")
            if df is not None and len(df):
                codes = [str(c) for c in df["ts_code"].tolist()]
                if not os.path.exists(p):
                    save(df, p, "stock_basic")
    else:  # cb_basic
        p = os.path.join(dirs["OUTPUT"], "cb_basic.parquet")
        if os.path.exists(p):
            df = read_parquet(p, ["ts_code"])
            if df is not None:
                codes = [str(c) for c in df["ts_code"].dropna().unique().tolist()]
        if not codes:
            p2 = os.path.join(dirs["OUTPUT"], "cb_daily.parquet")  # 退而求其次: 现有 cb_daily 里出现过的 code
            if os.path.exists(p2):
                df = read_parquet(p2, ["ts_code"])
                if df is not None:
                    codes = [str(c) for c in df["ts_code"].dropna().unique().tolist()]
        if not codes and not args.dry_run:
            df = pro.cb_basic()
            if df is not None and len(df):
                codes = [str(c) for c in df["ts_code"].tolist()]
                if not os.path.exists(p):
                    save(df, p, "cb_basic")
    ctx[key] = codes
    return codes


def read_old_safe(path, spec, name):
    """读现有完整文件; 文件存在却读不出来时返回哨兵 READ_FAILED, 避免用增量数据覆盖已有文件"""
    if not os.path.exists(path):
        return None
    old = read_parquet(path)
    if old is None:
        logger.info(f"  ❌ {name}: 现有文件读取失败, 为避免覆盖已存在数据, 放弃写入")
        return READ_FAILED
    return old


# ---------------------------------------------------------------- 增量: 各类表
def inc_date_table(pro, spec, args, end, dirs):
    name = spec["name"]
    path, alt = resolve_path(spec)
    if alt:
        logger.info(f"  📂 目标文件不存在, 覆盖情况取自 {alt}")
    cols = parquet_columns(path) if os.path.exists(path) else None
    last, n_last, total = coverage_of_date_table(spec, path, cols)
    if os.path.exists(path) and cols and "trade_date" not in cols:
        logger.info(f"  ❌ {name}: 现有文件缺少 trade_date 列, 无法增量, 跳过该表")
        return
    # 单文件不存在时, 覆盖情况必须取自 <name>/date=* 分区目录 (adj_factor 的活布局);
    # 不修这里就会一直误判『文件不存在』→ 每次全历史回补 1645 天 → 定时任务超时。
    part_root = None
    cov_printed = False
    if last is None:
        part_root = partition_dir_of(spec, dirs)
        if part_root:
            pds = partition_dates(part_root)
            last = pds[-1]
            logger.info(f"  已覆盖: {last} (取自 date=* 分区 {part_root}, 共 {len(pds)} 个分区) "
                        f"→ 只补缺口, 不重下历史")
            cov_printed = True
    plan_start = next_day(last) if last else START
    # 非 optional 表缺文件时保留首次建表语义 (全历史回填), 但起点不早于 START
    if plan_start < START:
        plan_start = START
    dates, note = plan_trade_dates(dirs, plan_start, end, use_api=not args.dry_run, pro=pro)
    if last and not cov_printed:
        bits = []
        if total is not None:
            bits.append(f"共 {total:,} 行")
        if n_last:
            bits.append(f"最近一日 {n_last:,} 只股票")
        logger.info(f"  已覆盖: {last}" + (f" ({', '.join(bits)})" if bits else ""))
    elif not last:
        logger.info("  已覆盖: 无 (文件不存在或为空) → 计划全量区间")
    if not dates:
        logger.info(f"  ✅ 无需下载: 已覆盖至 {last}, 晚于 --end({end})")
        return
    tail = f"; {note}" if note else ""
    per_day = f", 每个交易日约 {n_last:,} 只股票" if n_last else ""
    logger.info(f"  计划下载: {dates[0]} ~ {dates[-1]}, {len(dates)} 个交易日{per_day}{tail}")
    if args.dry_run:
        return
    new, err = batch_by_date(pro, spec.get("api", name), date_col=spec.get("date_col", "trade_date"),
                             name=name, dates=dates, paginate=spec.get("paginate", False))
    if new is None or len(new) == 0:
        logger.info(f"  ⚠️ {name}: 计划区间无数据返回" + (f" — {err}" if err else ""))
        return
    if err:
        logger.info(f"  ⚠️ {name}: 部分日期失败 — {err}")
    new = clean_df(new)
    if part_root:
        write_partitions(new, part_root, spec, name)
        return
    if os.path.exists(path):
        write_streamed(path, new, spec, name)
        return
    merged = merge_rows(None, new, spec, name)
    write_atomic(merged, path, name)


def inc_stock_table(pro, spec, args, end, ctx, dirs):
    name = spec["name"]
    path, alt = resolve_path(spec)
    if alt:
        logger.info(f"  📂 目标文件不存在, 覆盖情况取自 {alt}")
    cols = parquet_columns(path) if os.path.exists(path) else None
    covered = coverage_of_stock_table(spec, path, cols)
    if spec["expected"] == "quarter":
        thr = latest_expected_quarter(end)
        thr_desc = f"最新预期季末 {thr} (滞后 {QUARTER_LAG_DAYS} 天)"
    else:
        thr = last_trading_day(dirs, end, use_api=not args.dry_run, pro=pro)
        thr_desc = f"最近交易日 {thr}"
    codes = codes_for(pro, spec, args, ctx, dirs) or sorted(covered)
    if not codes:
        logger.info(f"  ⚠️ {name}: 本地无代码清单 (dry-run 不联网), 无法给出计划")
        return
    need = [c for c in codes if covered.get(c, "") < thr]
    fresh = sum(1 for c in need if not covered.get(c))
    logger.info(f"  已覆盖: {len(codes) - len(need)}/{len(codes)} 只"
                + (f", 文件内最新日期 {max(covered.values())}" if covered else " (文件不存在或无可判定日期列)")
                + f"; 阈值={thr_desc}")
    if not need:
        logger.info(f"  ✅ 无需下载: 全部已覆盖至 {thr}")
        return
    # 重叠窗口起点夹到 START 附近, 避免老股票(最新日期贴近 START)的窗口早于项目起点
    windows = [(c, max(days_ago(covered[c], 365), START) if covered.get(c) else START, end) for c in need]
    first_start = min(w[1] for w in windows)
    logger.info(f"  计划下载: {len(need)} 只 (其中 {fresh} 只无历史, 从 {START} 起; "
                f"其余从自身最新日期前 365 天重叠, 窗口末端 {end}), 并集 {first_start} ~ {end}"
                + (f"; 例: {need[:3]}" if len(need) <= 20 else ""))
    if args.dry_run:
        return
    new, err = batch_by_stock_window(pro, spec["api"], windows, name=name)
    if new is None or len(new) == 0:
        logger.info(f"  ⚠️ {name}: 无数据返回" + (f" — {err}" if err else ""))
        return
    if err:
        logger.info(f"  ⚠️ {name}: 部分代码失败 — {err}")
    new = clean_df(new)
    stat = file_stat(path)
    old = read_old_safe(path, spec, name)
    if old is READ_FAILED:
        return
    merged = merge_rows(old, new, spec, name)
    write_atomic(merged, path, name, expect_stat=stat)


def inc_code_table(pro, spec, args, end, dirs):
    """指数日线/申万行业日线: 按代码增量"""
    name = spec["name"]
    path, alt = resolve_path(spec)
    if alt:
        logger.info(f"  📂 目标文件不存在, 覆盖情况取自 {alt}")
    cols = parquet_columns(path) if os.path.exists(path) else None
    covered = coverage_of_code_table(spec, path, cols)
    thr = last_trading_day(dirs, end, use_api=not args.dry_run, pro=pro)
    codes = spec["codes"]
    need = [c for c in codes if covered.get(c, "") < thr]
    fresh = sum(1 for c in need if not covered.get(c))
    logger.info(f"  已覆盖: {len(codes) - len(need)}/{len(codes)} 个代码"
                + (f", 文件内最新日期 {max(covered.values())}" if covered else " (文件不存在)")
                + f"; 阈值=最近交易日 {thr}")
    if not need:
        logger.info(f"  ✅ 无需下载: 全部已覆盖至 {thr}")
        return
    windows = [(c, next_day(covered[c]) if covered.get(c) else START, end) for c in need]
    first_start = min(w[1] for w in windows)
    logger.info(f"  计划下载: {len(need)} 个代码 (其中 {fresh} 个无历史, 从 {START} 起; "
                f"其余从自身最新日期次日起), 窗口并集 {first_start} ~ {end}; 例: {need[:3]}")
    if args.dry_run:
        return
    new, err = batch_by_stock_window(pro, spec["api"], windows, name=name)
    if new is None or len(new) == 0:
        logger.info(f"  ⚠️ {name}: 无数据返回" + (f" — {err}" if err else ""))
        return
    if err:
        logger.info(f"  ⚠️ {name}: 部分代码失败 — {err}")
    new = clean_df(new)
    stat = file_stat(path)
    old = read_old_safe(path, spec, name)
    if old is READ_FAILED:
        return
    merged = merge_rows(old, new, spec, name)
    write_atomic(merged, path, name, expect_stat=stat)


def inc_quarter_table(pro, spec, args, end):
    """基金持仓: 按季度增量, 已完整覆盖的季度跳过"""
    name = spec["name"]
    path, _ = resolve_path(spec)
    cols = parquet_columns(path) if os.path.exists(path) else None
    covered = set()
    if not os.path.exists(path):
        logger.info("  已覆盖: 无 (文件不存在) → 全部季度需下载")
    elif cols and "end_date" in cols:
        cur = read_parquet(path, ["end_date"])
        if cur is not None:
            covered = {str(x) for x in cur["end_date"].dropna().unique().tolist()}
            logger.info(f"  已覆盖: {len(covered)} 个季度"
                        + (f" ({min(covered)}~{max(covered)})" if covered else ""))
    else:
        logger.info(f"  ⚠️ {name}: 现有文件缺少 end_date 列 → 全部季度按未覆盖处理")
    quarters = [q for q in quarter_ends(START, end) if q not in covered]
    if not quarters:
        logger.info(f"  ✅ 无需下载: 所有季度已覆盖")
        return
    logger.info(f"  计划下载: {len(quarters)} 个季度, {quarters[0]} ~ {quarters[-1]} "
                f"(含 --end({end}) 所在季度, 未出数的季度由接口空返回跳过)")
    if args.dry_run:
        return
    new = fetch_fund_portfolio(pro, quarters)
    if new is None or len(new) == 0:
        logger.info(f"  ⚠️ {name}: 无数据返回")
        return
    new = clean_df(new)
    stat = file_stat(path)
    old = read_old_safe(path, spec, name)
    if old is READ_FAILED:
        return
    merged = merge_rows(old, new, spec, name)
    write_atomic(merged, path, name, expect_stat=stat)


def run_incremental(pro, spec, args, end, ctx, dirs):
    """返回状态: 'ok' | 'deferred_missing'"""
    name = spec["name"]
    # optional 表: 目标文件缺失时跳过, 绝不做全历史回填
    # (根因: index_dailybasic 目标文件从不存在, 旧逻辑每次回退到 1644 个交易日逐日下载, ~65s/天)
    if spec.get("optional"):
        p, _alt = resolve_path(spec)
        if not os.path.exists(p):
            logger.info(f"⏭ {name}: 文件缺失且为 optional, 跳过 (不做全历史回填)")
            return "deferred_missing"
    # 从未建成的表 (无单文件也无 date=* 分区): 默认不发任何请求。
    # 旧行为每晚会从零回补 1645 个交易日/整表 → 3600s 定时任务必然超时, 且核心表被挤在后面。
    if not args.include_missing and not has_local_data(spec, dirs):
        logger.info(f"⏭ {name}: SKIP (missing, use --include-missing) — 本地无任何数据, 不回补全历史")
        return "deferred_missing"
    kind = spec["kind"]
    if kind == "date":
        inc_date_table(pro, spec, args, end, dirs)
    elif kind == "stock":
        inc_stock_table(pro, spec, args, end, ctx, dirs)
    elif kind == "code":
        inc_code_table(pro, spec, args, end, dirs)
    elif kind == "quarter":
        inc_quarter_table(pro, spec, args, end)
    else:  # legacy 表不在增量范围内, 沿用 skip_if_exists 的一次性下载
        p, name = spec["path"], spec["name"]
        if skip_if_exists(p, name):
            return "ok"
        if args.dry_run:
            logger.info(f"  [dry-run] 文件不存在 → 将整表下载一次 (沿用 skip_if_exists 行为)")
            return "ok"
        try:
            save(spec["fetch"](end), p, name)
        except Exception as e:
            logger.info(f"  ❌ {name}: {str(e)[:80]}")
    return "ok"


# ---------------------------------------------------------------- full 模式 (原行为)
def run_full(pro, spec, args, end, ctx, dirs):
    p, name = spec["path"], spec["name"]
    if skip_if_exists(p, name):
        return
    if args.dry_run:
        logger.info(f"  [dry-run] 文件不存在 → full 模式将整表下载 ({START} ~ {end}), 不执行")
        return
    kind = spec["kind"]
    if kind == "legacy":
        if spec.get("tolerant"):  # 原脚本对这几张表单独 try/except, 其余异常直接中断
            try:
                save(spec["fetch"](end), p, name)
            except Exception as e:
                logger.info(f"  ❌ {name}: {str(e)[:80]}")
        else:
            save(spec["fetch"](end), p, name)
    elif kind == "date":
        df, _err = batch_by_date(pro, spec.get("api", name), date_col=spec.get("date_col", "trade_date"), name=name)
        save(df, p, name)
    elif kind == "stock":
        codes = codes_for(pro, spec, args, ctx, dirs)
        if spec.get("range"):
            df = batch_by_stock(pro, spec["api"], codes, name=name, start_date=START, end_date=end)
        else:
            df = batch_by_stock(pro, spec["api"], codes, name=name)
        save(df, p, name)
    elif kind == "code":
        all_dfs = []
        for c in spec["codes"]:
            try:
                df = getattr(pro, spec["api"])(ts_code=c, start_date=START, end_date=end)
                if df is not None and len(df) > 0:
                    all_dfs.append(df)
            except Exception:
                pass
        if all_dfs:
            save(pd.concat(all_dfs, ignore_index=True), p, name)
    elif kind == "quarter":
        try:
            df = fetch_fund_portfolio(pro, quarter_ends(START, end))
            if df is not None:  # 原行为: 无数据时不调用 save
                save(df, p, name)
        except Exception as e:
            if spec.get("tolerant"):
                logger.info(f"  ❌ {name}: {str(e)[:80]}")
            else:
                raise


# ---------------------------------------------------------------- 表注册表
# kind: date=按交易日, stock=按 ts_code, code=固定代码列表, quarter=按季度, legacy=一次性接口
# key:  业务主键候选列 (按实际存在的列取), 缺省退回 (ts_code, trade_date)/trade_date
def _stock(name, title, coverage, expected, key, date_col, range_=True, codes_from="stock_basic", dirs=None):
    return dict(name=name, title=title, kind="stock", api=name,
                path=f"{dirs['OUTPUT']}/{name}.parquet", coverage=coverage, expected=expected,
                key=key, date_col=date_col, range=range_, codes_from=codes_from)


def _date(name, title, path=None, api=None, optional=False, dirs=None, paginate=False):
    return dict(name=name, title=title, kind="date", api=api or name,
                path=path or f"{dirs['OUTPUT']}/{name}.parquet", date_col="trade_date",
                optional=optional, paginate=paginate)


# legacy 表的 fetch 走模块级 _PRO（build_tables 时 pro 可能还没建，故延迟绑定）
def _fetch_stock_basic():
    return _PRO.stock_basic(exchange="", list_status="L",
                            fields="ts_code,symbol,name,area,industry,market,list_date,exchange")


def _fetch_cb_basic():
    return _PRO.cb_basic()


def _fetch_fund_nav():
    return _PRO.fund_nav(ts_code="510300.SH")


def _fetch_fund_share():
    df, _err = batch_by_date(_PRO, "fund_share", name="fund_share")
    return df


def _fetch_us_daily(end):
    return batch_by_stock(_PRO, "us_daily", US_STOCKS, name="us_daily",
                          start_date=START, end_date=end)


def _fetch_repo_daily(end):
    return _PRO.repo_daily(ts_code="204001.SH", start_date=START, end_date=end)


def build_tables(dirs) -> list[dict]:
    """表注册表（与 scripts/download_all_v2.py 的 TABLES 逐字段等价）。

    路径由 data_dir 派生；原脚本硬编码的 TOKEN/HTTP_URL 不在此处，改由 make_pro() 从
    config/settings.toml 读取。
    """
    OUTPUT, MACRO_OUTPUT, US_OUTPUT, INDEX_OUTPUT = (
        dirs["OUTPUT"], dirs["MACRO_OUTPUT"], dirs["US_OUTPUT"], dirs["INDEX_OUTPUT"])
    return [
        dict(name="stock_basic", title="[0] 获取股票列表", kind="legacy",
             path=f"{OUTPUT}/stock_basic.parquet", api="stock_basic",
             fetch=_fetch_stock_basic),
        _date("daily", "[1] daily (日线行情)", dirs=dirs),
        _date("weekly", "[2] weekly (周线)", dirs=dirs),
        _date("monthly", "[3] monthly (月线)", dirs=dirs),
        _date("adj_factor", "[4] adj_factor (复权因子)", dirs=dirs),
        _date("daily_basic", "[5] daily_basic (每日指标)", dirs=dirs),
        # moneyflow 单日全市场约 5575 行 > 接口单次 5000 上限, 必须 offset 分页,
        # 否则每天静默丢 575 行 (≈10%)。实测两端点分页均可用 (offset=5000 → 575 行)。
        _date("moneyflow", "[6] moneyflow (资金流向)", dirs=dirs, paginate=True),
        # moneyflow_ind 两端点均未开放 (unknown_api / api_not_enabled), 保留注册项,
        # 缺文件时按 optional 处理, 不影响其他表
        _date("moneyflow_ind", "[6A] moneyflow_ind (行业资金流)", dirs=dirs, optional=True),
        _date("moneyflow_hsgt", "[6B] moneyflow_hsgt (沪深港通资金流向)", dirs=dirs),
        _date("margin_detail", "[7] margin_detail (融资融券)", dirs=dirs),
        _date("hk_hold", "[8] hk_hold (沪深港通持股)", dirs=dirs),
        _date("top_list", "[9] top_list (龙虎榜)", dirs=dirs),
        _date("top_inst", "[10] top_inst (龙虎榜机构)", dirs=dirs),
        _stock("income", "[11] income (利润表)",
               ["announce_date", "end_date"], "quarter", ["ts_code", "end_date", "report_type"], "end_date", dirs=dirs),
        _stock("balancesheet", "[12] balancesheet (资产负债表)",
               ["announce_date", "end_date"], "quarter", ["ts_code", "end_date", "report_type"], "end_date", dirs=dirs),
        _stock("cashflow", "[13] cashflow (现金流量表)",
               ["announce_date", "end_date"], "quarter", ["ts_code", "end_date", "report_type"], "end_date", dirs=dirs),
        _stock("fina_indicator", "[14] fina_indicator (财务指标)",
               ["ann_date", "end_date"], "quarter", ["ts_code", "end_date", "report_type"], "end_date", dirs=dirs),
        _stock("dividend", "[15] dividend (分红)",
               ["ann_date", "end_date"], "quarter", ["ts_code", "end_date", "ann_date", "div_proc"], "end_date", dirs=dirs),
        _stock("forecast", "[16] forecast (业绩预告)",
               ["ann_date", "end_date"], "quarter", ["ts_code", "ann_date"], "end_date", dirs=dirs),
        _stock("stk_holdernumber", "[17] stk_holdernumber (股东户数)",
               ["ann_date", "end_date"], "quarter", ["ts_code", "end_date"], "end_date", dirs=dirs),
        _stock("top10_holders", "[18] top10_holders (十大股东)",
               ["ann_date", "end_date"], "quarter", ["ts_code", "end_date", "holder_name"], "end_date", dirs=dirs),
        _stock("top10_floatholders", "[19] top10_floatholders (十大流通股东)",
               ["ann_date", "end_date"], "quarter", ["ts_code", "end_date", "holder_name"], "end_date", dirs=dirs),
        _stock("stk_managers", "[20] stk_managers (高管)",
               ["ann_date"], "quarter", ["ts_code", "ann_date", "name", "title"], "ann_date", range_=False, dirs=dirs),
        _stock("stk_rewards", "[21] stk_rewards (高管薪酬)",
               ["ann_date", "end_date"], "quarter", ["ts_code", "end_date", "name"], "end_date", range_=False, dirs=dirs),
        _date("stk_limit", "[22] stk_limit (涨跌停价)", dirs=dirs),
        _date("suspend_d", "[23] suspend_d (停复牌)", dirs=dirs),
        dict(name="index_daily", title="[24] index_daily (指数日线)", kind="code", api="index_daily",
             path=f"{INDEX_OUTPUT}/index_daily.parquet", alt_paths=[f"{OUTPUT}/index_daily.parquet"],
             codes=INDEX_CODES, date_col="trade_date"),
        # index_weight 无 ts_code 列, 主键必须带上 index_code+con_code, 否则会被 (trade_date) 去重压成每天一行
        dict(name="index_weight", title="[25] index_weight (指数权重)", kind="date", api="index_weight",
             path=f"{INDEX_OUTPUT}/index_weight.parquet", alt_paths=[f"{OUTPUT}/index_weight.parquet"],
             date_col="trade_date", key=["index_code", "con_code", "trade_date"]),
        _date("index_dailybasic", "[26] index_dailybasic (指数每日指标)",
              path=f"{INDEX_OUTPUT}/index_dailybasic.parquet", optional=True, dirs=dirs),
        dict(name="sw_daily", title="[27] sw_daily (申万行业日线)", kind="code", api="sw_daily",
             path=f"{INDEX_OUTPUT}/sw_daily.parquet", alt_paths=[f"{OUTPUT}/sw_daily.parquet"],
             codes=SW_CODES, date_col="trade_date"),
        dict(name="cb_basic", title="[28] cb_basic (可转债列表)", kind="legacy",
             path=f"{OUTPUT}/cb_basic.parquet", api="cb_basic", tolerant=True,
             fetch=_fetch_cb_basic),
        _stock("cb_daily", "[29] cb_daily (可转债日线)",
               ["trade_date"], "trade", ["ts_code", "trade_date"], "trade_date",
               codes_from="cb_basic", dirs=dirs),
        dict(name="fund_nav", title="[30] fund_nav (基金净值)", kind="legacy",
             path=f"{OUTPUT}/fund_nav_510300.parquet", api="fund_nav", tolerant=True,
             fetch=_fetch_fund_nav),
        dict(name="fund_portfolio", title="[30A] fund_portfolio (公募基金持仓)", kind="quarter",
             path=f"{OUTPUT}/fund_portfolio.parquet", api="fund_portfolio", date_col="end_date", tolerant=True,
             key=["ts_code", "end_date", "symbol", "ann_date"]),
        dict(name="fund_share", title="[30B] fund_share (基金份额)", kind="legacy",
             path=f"{OUTPUT}/fund_share.parquet", api="fund_share", tolerant=True,
             fetch=_fetch_fund_share),
        dict(name="us_daily", title="[31] us_daily (美股日线)", kind="legacy",
             path=f"{US_OUTPUT}/us_daily.parquet", api="us_daily", api_name="us_daily",
             fetch=_fetch_us_daily),
        _date("shibor", "[32] shibor (上海银行间同业拆放利率)",
              path=f"{MACRO_OUTPUT}/shibor.parquet", dirs=dirs),
        _date("repurchase", "[33] repurchase (国债回购利率)",
              path=f"{MACRO_OUTPUT}/repurchase.parquet", dirs=dirs),
        dict(name="repo_daily", title="[34] repo_daily (国债回购日线)", kind="legacy",
             path=f"{MACRO_OUTPUT}/repo_daily.parquet", api="repo_daily", tolerant=True,
             fetch=_fetch_repo_daily),
    ]


# ---------------------------------------------------------------- CLI 参数
def parse_args(argv=None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Zer0share tushare 下载脚本: 默认增量刷新, --mode full 保持原全量行为")
    ap.add_argument("--mode", choices=["incremental", "full"], default="incremental",
                    help="incremental=按缺口增量刷新(默认); full=原全量行为")
    ap.add_argument("--tables", default="",
                    help="逗号分隔表名过滤, 如 moneyflow,moneyflow_ind; 留空=全部")
    ap.add_argument("--end", default=None,
                    help=f"截止日期 yyyymmdd, 默认: incremental=今天, full=原 END({END})")
    ap.add_argument("--dry-run", action="store_true",
                    help="只打印计划: 每张表已覆盖到的最后日期 + 将下载的范围; 不发网络请求、不写文件")
    ap.add_argument("--include-missing", action="store_true",
                    help="增量模式下也回补本地从未建成的表 (全历史, 极慢, 默认关闭)")
    ap.add_argument("--topic", default="",
                    help=f"按主题下载, 逗号分隔; all=全部主题。可选: {','.join(ALL_TOPICS)}。"
                         "与 --tables 同时给出时取并集")
    ap.add_argument("--budget-seconds", type=float, default=None,
                    help=f"增量模式时间预算秒数 (默认 {DEFAULT_BUDGET_SECONDS:.0f}); 耗尽后剩余表标 DEFERRED(budget)")
    return ap.parse_args(argv)


def select_tables(table_arg, tables, topic_arg=""):
    all_names = [t["name"] for t in tables]
    wanted: set[str] = set()
    if topic_arg:
        for t in topic_arg.split(","):
            t = t.strip()
            if not t:
                continue
            if t == "all":
                wanted.update(all_names)
                continue
            if t not in BULK_TOPICS:
                logger.info(f"⚠️ 未知主题: {t}")
                logger.info(f"   可用主题: {','.join(ALL_TOPICS)}")
                continue
            wanted.update(BULK_TOPICS[t])
    if table_arg:
        wanted.update(t.strip() for t in table_arg.split(",") if t.strip())
    if not wanted:
        return list(tables)
    unknown = [w for w in sorted(wanted) if w not in all_names]
    if unknown:
        logger.info(f"⚠️ 未知表名: {unknown}")
        logger.info(f"   可用表名: {','.join(all_names)}")
    return [t for t in tables if t["name"] in wanted]


def order_tables(selected):
    """核心表优先 (PRIORITY 顺序), 其余保持注册顺序; 同名表稳定排序"""
    def key(item):
        i, spec = item
        try:
            return (PRIORITY.index(spec["name"]), i)
        except ValueError:
            return (len(PRIORITY), i)
    return [s for _i, s in sorted(enumerate(selected), key=key)]


def core_coverage(end, tables, dirs):
    """核心表覆盖判定 → (目标交易日, {表名: (本地最新日期, 是否达标)})"""
    target = last_trading_day(dirs, end)
    out = {}
    for spec in tables:
        if spec["name"] not in CORE_TABLES:
            continue
        mx = table_max_date(spec, dirs)
        out[spec["name"]] = (mx, bool(mx and mx >= target))
    return target, out


# ---------------------------------------------------------------- main
def main(argv=None) -> int:
    global _DEADLINE
    args = parse_args(argv)
    from zer0share.logging import init_logger

    cfg = load_config()
    init_logger(cfg.log_path)
    dirs = dirs_for(cfg)
    pro = make_pro(cfg)

    end = args.end or (END if args.mode == "full" else datetime.now().strftime("%Y%m%d"))
    tables = build_tables(dirs)
    selected = order_tables(select_tables(args.tables, tables, getattr(args, "topic", "")))
    t0 = time.time()
    budget = None
    if args.mode == "incremental":
        budget = DEFAULT_BUDGET_SECONDS if args.budget_seconds is None else float(args.budget_seconds)
        _DEADLINE = t0 + budget
    logger.info("=" * 70)
    logger.info(f"Zer0share {'全量下载 v2' if args.mode == 'full' else '增量刷新 v2'} (tushare: {cfg.tushare_http_url})")
    logger.info(f"  模式: {args.mode}   时间: {START} ~ {end}   表: {len(selected)}/{len(tables)} 张"
                + ("   [DRY-RUN: 不联网/不写盘]" if args.dry_run else ""))
    if args.mode == "incremental":
        logger.info(f"  时间预算: {budget:.0f}s (--budget-seconds)   include_missing={args.include_missing}")
    logger.info(f"  执行顺序: {', '.join(s['name'] for s in selected)}")
    logger.info("=" * 70)
    if not args.dry_run:
        ensure_dirs(dirs)
    ctx: dict = {}
    failures = []
    deferred_missing = []
    deferred_budget = []
    for idx, spec in enumerate(selected, 1):
        name = spec["name"]
        if budget is not None and budget_exhausted():
            logger.info(f"\n[{idx}/{len(selected)}] {name}: DEFERRED(budget)")
            deferred_budget.append(name)
            continue
        logger.info(f"\n{spec['title']} ({idx}/{len(selected)})...")
        if budget is not None and not args.dry_run:
            logger.info(f"  预算剩余: {budget_left():.0f}s")
        try:
            if args.mode == "full":
                run_full(pro, spec, args, end, ctx, dirs)  # 沿用原行为: 异常直接中断
            else:
                try:
                    status = run_incremental(pro, spec, args, end, ctx, dirs)
                except Exception as e:
                    logger.info(f"  ❌ {name}: {str(e)[:120]}")
                    traceback.print_exc()
                    failures.append(name)
                else:
                    if status == "deferred_missing":
                        deferred_missing.append(name)
        except Exception as e:
            logger.info(f"  ❌ {name}: {str(e)[:120]}")
            raise

    elapsed = time.time() - t0
    logger.info(f"\n{'=' * 70}")
    if args.dry_run:
        logger.info(f"Dry-run 完成 ({elapsed:.1f}s): 仅打印计划, 未发起网络请求, 未写入任何文件")
        logger.info(f"  核心表: {', '.join(CORE_TABLES)}   预算: "
                    + (f"{budget:.0f}s" if budget else "不限"))
        logger.info(f"  SKIP(missing): {len(deferred_missing)} 张 → {','.join(deferred_missing) or '无'}")
        return 0

    target, cov = core_coverage(end, tables, dirs)
    core_ok = all(ok for _mx, ok in cov.values())
    logger.info(f"PARTIAL_OK core_ok={core_ok}  core_target={target}  "
                f"budget={('%ds' % budget) if budget else 'none'}  elapsed={elapsed:.1f}s")
    for name, (mx, ok) in cov.items():
        logger.info(f"  core {name:<12} max_date={mx or '缺'}  {'OK' if ok else 'GAP'}")
    logger.info(f"  deferred_missing({len(deferred_missing)}): {', '.join(deferred_missing) or '无'}")
    logger.info(f"  deferred_budget({len(deferred_budget)}): {', '.join(deferred_budget) or '无'}")
    logger.info(f"  failures({len(failures)}): {', '.join(failures) or '无'}")
    if failures:
        logger.info(f"⚠️ 增量刷新部分表失败: {failures}")
    logger.info(f"All done. Total time: {elapsed / 60:.1f} min")

    logger.info(f"\nFiles:")
    for root, dirs_, files in os.walk(str(cfg.data_dir)):
        for f in sorted(files):
            if not f.endswith(".parquet"):
                continue
            rel = os.path.relpath(os.path.join(root, f), str(cfg.data_dir))
            if "date=" in rel or rel.count(os.sep) > 1:  # 分区文件跳过, 否则几千行噪声
                continue
            path = os.path.join(root, f)
            sz = os.path.getsize(path) / 1e6
            if sz > 0.01:
                logger.info(f"  {rel}: {sz:.1f} MB")

    # 行情下载完成后增量重建板块资金流缓存 (独立 try/except: 构建失败不影响下载 exit code)
    # 预算已耗尽或剩余不足时直接跳过, 保证整体在窗口内结束 (该子步骤自带 7200s 超时)
    left = budget_left()
    if left is not None and left < 60:
        logger.info(f"⏭ build_sector_fundflow: 预算剩余 {left:.0f}s < 60s, 跳过")
    elif not os.path.exists(os.path.join(dirs["OUTPUT"], "moneyflow.parquet")):
        # moneyflow 本地从未建成 (接口建表成本高, 默认被 missing 规则跳过)。
        # 不无条件下发子进程: 否则每晚固定报一次 "moneyflow 不存在" 的退出码 1,
        # 噪声告警会掩盖真正的构建失败。待 moneyflow 表建立后本步自动生效。
        logger.info("⏭ build_sector_fundflow: moneyflow 未建成, 跳过 (不影响行情下载)")
    else:
        try:
            import subprocess
            script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sector_fundflow.py")
            to = int(min(7200, left)) if left is not None else 7200
            r = subprocess.run([sys.executable, "-m", "zer0share.sector_fundflow", "--incremental"],
                               capture_output=True, text=True, timeout=to)
            logger.info(r.stdout)
            if r.returncode != 0:
                logger.info(f"⚠️ build_sector_fundflow 退出码 {r.returncode}: {r.stderr[:300]}")
        except Exception as e:
            logger.info(f"⚠️ build_sector_fundflow 失败(不影响行情下载): {e}")

    if failures:
        logger.info("EXIT 1 (脚本异常/表失败)")
        return 1
    logger.info(f"EXIT {0 if core_ok else 3} (核心表{'齐全' if core_ok else '有缺口'})")
    return 0 if core_ok else 3


if __name__ == "__main__":
    sys.exit(main())
