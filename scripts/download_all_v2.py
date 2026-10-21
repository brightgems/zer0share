#!/usr/bin/env python3
"""
Zer0share 全量数据下载 v2 — 基于 tushare 新接口 (tl.kaixin8.top)
覆盖: 股票行情/财务/融资融券/沪深港通/龙虎榜/基金持仓/可转债/指数/申万行业/美股/宏观数据

用法:
    python download_all_v2.py                                     # 增量刷新(默认), 只补 --end 之前的缺口
    python download_all_v2.py --mode full                         # 原全量行为 (skip_if_exists, START~END)
    python download_all_v2.py --tables moneyflow,moneyflow_ind    # 逗号分隔表名过滤
    python download_all_v2.py --end 20261016                      # 指定截止日期 (yyyymmdd)
    python download_all_v2.py --dry-run --tables daily,moneyflow  # 只打印计划 (不联网/不写盘)
    python download_all_v2.py --include-missing                   # 也回补从未建成的表 (全历史, 很慢)
    python download_all_v2.py --budget-seconds 600                # 改时间预算 (默认 780s)

增量策略:
    日期类表   — 读现有 parquet 的最大 trade_date, 只下载其后的交易日
                 (单文件不存在时读 <table>/date=* 分区目录, 见 adj_factor)
    股票类表   — per ts_code 判断覆盖, 未覆盖的从该 code 最新日期前 365 天重叠下载 (容忍财报重述)
    指数日线   — per 指数代码增量; 基金持仓 — 按季度增量 (已完整覆盖的季度跳过)
    合并写回   — concat 后按业务主键去重 (默认 (ts_code, trade_date)), 按日期排序,
                 原子写入 (先写 path.tmp 再 os.replace)

调度约束 (2026-10-20):
    * 从未建成的表默认不发请求: 只打印 `SKIP (missing, use --include-missing)` 并计入 deferred,
      否则每晚都会从零回补 1645 个交易日 → 3600s 定时任务必然超时。
    * 表按核心优先顺序执行, 每表开始前检查 --budget-seconds 剩余预算, 耗尽即标 DEFERRED(budget)。
    * 退出码: 核心表齐全=0, 核心表有缺口=3, 脚本异常=1。
"""
import argparse
import tushare as ts
import pandas as pd
import os, sys, time, traceback
from datetime import datetime, timedelta

TOKEN = 'tsr_QMct87xmApopWfm_LHghntnYae-E4D_9sFYHDKa9Tlc'
HTTP_URL = 'https://tl.kaixin8.top/tushare/sdk'
OUTPUT = '/quantdata/zer0share/data/stock'
MACRO_OUTPUT = '/quantdata/zer0share/data/macro'
US_OUTPUT = '/quantdata/zer0share/data/us'
HK_OUTPUT = '/quantdata/zer0share/data/hk'
INDEX_OUTPUT = '/quantdata/zer0share/data/index'

TRADE_CAL_PATH = f'{OUTPUT}/trade_cal.parquet'          # 列: exchange/cal_date/is_open/pretrade_date
TRADE_CAL_DIR = f'{OUTPUT}/trade_cal'                   # 分区日历 (覆盖更长区间, 作为本地二级来源)
QUARTER_LAG_DAYS = 45      # 财报披露滞后: 只把距今超过该天数的最近季末视为"预期已出齐"

START = '20200101'
END = '20260930'           # full 模式默认截止日 (保持原行为); incremental 默认为今天

# --- 调度/预算 (2026-10-20) -------------------------------------------------
DEFAULT_BUDGET_SECONDS = 780.0     # 增量模式默认时间预算; 留出 3600s 定时窗口的余量
# 执行顺序: 核心表优先, 其余按注册顺序跟后
PRIORITY = ['daily', 'daily_basic', 'adj_factor', 'stk_limit', 'suspend_d',
            'moneyflow', 'moneyflow_ind', 'stock_st', 'index_daily', 'index_weight',
            'trade_cal']
# 核心表: 必须每次跑到 --end(或最近交易日), 有缺口 → exit 3。
# moneyflow/moneyflow_ind 在 PRIORITY 里排位靠前 (设计指定顺序), 但本地从未建成 → 默认被
# missing 规则跳过, 且不列入核心判定 (否则没有本地数据也永远算缺口, 每晚固定 exit 3)。
CORE_TABLES = ('daily', 'daily_basic', 'adj_factor', 'stk_limit', 'suspend_d',
               'index_daily', 'index_weight')

_DEADLINE = None                   # time.time() 形式的预算截止点; None = 不限制

INDEX_CODES = ['000001.SH', '399001.SZ', '399006.SZ', '000300.SH', '000905.SH',
               '000688.SH', '399005.SZ', '000016.SH', '000852.SH']
SW_CODES = [f'801{i:03d}.SI' for i in range(1, 32)]
US_STOCKS = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA', 'META', 'NFLX']

pro = ts.pro_api(TOKEN)
pro._DataApi__http_url = HTTP_URL


# ---------------------------------------------------------------- 基础工具
def ensure_dirs():
    for d in [OUTPUT, MACRO_OUTPUT, US_OUTPUT, HK_OUTPUT, INDEX_OUTPUT]:
        os.makedirs(d, exist_ok=True)


def clean_df(df):
    """对象列清洗: 空串→None, 能整体转数值的列转成数值, 否则统一为字符串"""
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].replace('', None)
            # Try converting to numeric; if it works, use numeric; otherwise keep as string.
            # pandas 2.2+/3.0 弃用 pd.to_numeric(errors='ignore'), 用 try/except 复刻其语义:
            # 转换失败时 converted 仍是原列 (dtype == object), 走原有的字符串兜底分支。
            try:
                converted = pd.to_numeric(df[col])
            except (ValueError, TypeError):
                converted = df[col]
            if converted.dtype != 'object':
                df[col] = converted
            else:
                df[col] = df[col].astype(str)
    return df


def save(df, path, name):
    if df is None or len(df) == 0:
        print(f"  ⚠️ {name}: 0 rows, skipped")
        return False
    clean_df(df)
    df.to_parquet(path, index=False)
    sz = os.path.getsize(path) / 1e6
    print(f"  ✅ {name}: {len(df):,} rows, {sz:.1f} MB → {path}")
    return True


def write_atomic(df, path, name, expect_stat=None):
    """增量合并结果原子落盘: 先写 path.tmp 再 os.replace。
    expect_stat = 读取旧文件前的 (st_mtime_ns, st_size); 若写回前文件被其他进程改过则放弃写入,
    避免覆盖并发下载进程正在写的数据。"""
    if expect_stat is not None:
        try:
            cur = os.stat(path)
            cur = (cur.st_mtime_ns, cur.st_size)
        except OSError:
            cur = None
        if cur != expect_stat:
            print(f"  ⚠️ {name}: 文件在处理期间被其他进程修改, 放弃写入以免覆盖")
            return False
    tmp = f'{path}.tmp'
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
    print(f"  ✅ {name}: {len(df):,} rows, {sz:.1f} MB → {path} (原子写入)")
    return True


def skip_if_exists(path, name):
    """Skip download if file already exists and is non-trivial size"""
    if os.path.exists(path) and os.path.getsize(path) > 1000:
        sz = os.path.getsize(path) / 1e6
        print(f"  ⏭️ {name}: already exists ({sz:.1f} MB), skipping")
        return True
    return False


# ---------------------------------------------------------------- 交易日历
_CAL_CACHE = {}


def _cal_from_df(df):
    """cal_date/is_open → 升序交易日 list"""
    if df is None or len(df) == 0 or 'cal_date' not in df.columns:
        raise ValueError('缺少 cal_date 列或为空')
    s = df['cal_date'].astype(str)
    if 'is_open' in df.columns:
        mask = df['is_open'].astype(str).str.lower().isin(['1', 'true'])
        s = s[mask]
    dates = sorted(set(s))
    if not dates:
        raise ValueError('无交易日')
    return dates


def _load_partitioned_cal():
    """读分区日历 (trade_cal/exchange=*/data.parquet)。整目录读会因各交易所 schema 不一致失败,
    所以逐文件读; A 股交易日以 exchange=SSE 为准。"""
    import glob
    files = sorted(glob.glob(os.path.join(TRADE_CAL_DIR, 'exchange=SSE', '*.parquet'))) or \
        sorted(glob.glob(os.path.join(TRADE_CAL_DIR, '*', '*.parquet')))
    if not files:
        raise ValueError('无分区日历文件')
    parts = [pd.read_parquet(f, columns=['cal_date', 'is_open']) for f in files]
    return _cal_from_df(pd.concat(parts, ignore_index=True))


def load_trade_calendar(full=False):
    """优先读 trade_cal.parquet (需求指定的主日历); full=True 时再尝试同目录的分区日历
    (trade_cal/exchange=*, 覆盖区间更长), 都不可用返回 None。"""
    key = bool(full)
    if key in _CAL_CACHE:
        return _CAL_CACHE[key]
    cal = None
    try:
        cal = _cal_from_df(pd.read_parquet(TRADE_CAL_PATH))
        if not key:
            print(f"  📅 交易日历: {TRADE_CAL_PATH} ({cal[0]} ~ {cal[-1]}, {len(cal)} 天)")
    except Exception as e:
        print(f"  ⚠️ 本地交易日历不可用 ({str(e)[:80]}), 退回 pd.bdate_range")
    if key:     # 需要更长覆盖时, 再合并同目录的分区日历 (trade_cal/exchange=*)
        try:
            ext = _load_partitioned_cal()
            cal = sorted(set(cal or []) | set(ext))
            print(f"  📅 合并分区日历 {TRADE_CAL_DIR} → 覆盖至 {cal[-1]} ({len(cal)} 天)")
        except Exception:
            pass
    _CAL_CACHE[key] = cal
    return cal


def _end_of(cal):
    return cal[-1] if cal else '00000000'


def get_trade_dates(start, end):
    """(原行为, full 模式沿用) 工作日近似日期列表"""
    return [d.strftime('%Y%m%d') for d in pd.bdate_range(start, end)]


def fetch_cal_online(start, end):
    try:
        df = pro.trade_cal(exchange='SSE', start_date=start, end_date=end)
        return _cal_from_df(df)
    except Exception as e:
        print(f"  ⚠️ 在线交易日历获取失败 ({str(e)[:60]}), 退回 pd.bdate_range")
        return None


def plan_trade_dates(start, end, use_api=False):
    """增量计划用交易日 (闭区间 yyyymmdd, 升序)。
    优先本地交易日历; 只有日历本身不覆盖时才依次尝试: 在线 pro.trade_cal (非 dry-run) → pd.bdate_range。
    注意: 日历有效却查不到交易日 = 该区间确实没有交易日, 不再去联网兜底。
    返回 (dates, note); note 非 None 说明尾段是近似值。"""
    if start and end and start > end:
        return [], None
    cal = load_trade_calendar()
    if not cal:
        return get_trade_dates(start, end), '无本地日历, 全部按工作日近似'
    head = [d for d in cal if start <= d <= end]
    cal_max = _end_of(cal)
    if end <= cal_max:
        return head, None
    tail_start = start if start > cal_max else \
        (datetime.strptime(cal_max, '%Y%m%d') + timedelta(days=1)).strftime('%Y%m%d')
    if tail_start > end:
        return head, None
    ext = load_trade_calendar(full=True)        # 分区日历, 覆盖更长
    if ext is not None and _end_of(ext) >= tail_start:   # 覆盖到 → 日历权威
        tail = [d for d in ext if tail_start <= d <= end]
        return sorted(set(head) | set(tail)), None
    if use_api:
        tail = fetch_cal_online(tail_start, end)
        if tail is not None:
            return sorted(set(head) | set(tail)), None
    tail = get_trade_dates(tail_start, end)
    return sorted(set(head) | set(tail)), \
        f'本地日历覆盖至{cal_max}, 其后{len(tail)}天按工作日近似(节假日以接口空返回兜底)'


def last_trading_day(end, use_api=False):
    lo = (datetime.strptime(end, '%Y%m%d') - timedelta(days=20)).strftime('%Y%m%d')
    dates, _ = plan_trade_dates(lo, end, use_api=use_api)
    return dates[-1] if dates else end


def latest_expected_quarter(end, lag=QUARTER_LAG_DAYS):
    """距今超过 lag 天的最近季末 — 该季报原则上已披露完毕"""
    ref = datetime.strptime(end, '%Y%m%d').date() - timedelta(days=lag)
    p = pd.Period(ref, freq='Q')
    qe = p.end_time.date()
    if qe > ref:
        qe = (p - 1).end_time.date()
    return qe.strftime('%Y%m%d')


def quarter_ends(start, end):
    """季度末日期列表 (原 fund_portfolio 的 period 取值)"""
    return [d.strftime('%Y%m%d') for d in pd.period_range(start, end, freq='Q').to_timestamp(how='end')]


def next_day(d):
    return (datetime.strptime(d, '%Y%m%d') + timedelta(days=1)).strftime('%Y%m%d')


def days_ago(d, n):
    return (datetime.strptime(d, '%Y%m%d') - timedelta(days=n)).strftime('%Y%m%d')


# ---------------------------------------------------------------- 下载批量器
def call_api(fn, params, retries=2, backoff=3.0):
    """带重试的接口调用 (代理偶发 ReadTimeout 属正常); 返回 None 表示最终失败。
    预算耗尽时不再重试, 避免拖过定时窗口。"""
    for k in range(retries + 1):
        try:
            return fn(**params)
        except Exception:
            if k >= retries or budget_exhausted():
                return None
            time.sleep(backoff * (k + 1))
    return None


def batch_by_date(api_name, date_col='trade_date', name='', sleep=0.15, dates=None):
    """按日期批量下载 (dates=None 时沿用原 START~END 工作日序列)"""
    if dates is None:
        dates = get_trade_dates(START, END)
    all_dfs = []
    for i, d in enumerate(dates):
        if budget_exhausted():
            print(f"  ⏹ 预算耗尽: 中断 {name or api_name} 的下载 (已完成 {i}/{len(dates)} 天)")
            break
        df = call_api(getattr(pro, api_name), {date_col: d})
        if df is not None and len(df) > 0:
            all_dfs.append(df)
        time.sleep(sleep)
        if (i+1) % 200 == 0:
            total = sum(len(x) for x in all_dfs)
            print(f"  ...{d} ({total:,} rows, {i+1}/{len(dates)})")
    if all_dfs:
        return pd.concat(all_dfs, ignore_index=True)
    return None


def batch_by_stock(api_name, stock_list, name='', sleep=0.15, **extra):
    """按股票批量下载"""
    all_dfs = []
    for i, code in enumerate(stock_list):
        try:
            params = {'ts_code': code, **extra}
            df = getattr(pro, api_name)(**params)
            if df is not None and len(df) > 0:
                all_dfs.append(df)
        except Exception:
            pass
        time.sleep(sleep)
        if (i+1) % 500 == 0:
            total = sum(len(x) for x in all_dfs)
            print(f"  ...{code} ({total:,} rows, {i+1}/{len(stock_list)})")
    if all_dfs:
        return pd.concat(all_dfs, ignore_index=True)
    return None


def batch_by_stock_window(api_name, windows, name='', sleep=0.15, **extra):
    """按 (ts_code, start_date, end_date) 窗口批量下载; 接口不接受 start/end 时自动退回该 code 整段历史"""
    all_dfs = []
    for i, (code, s, e) in enumerate(windows):
        if budget_exhausted():
            print(f"  ⏹ 预算耗尽: 中断 {name or api_name} 的下载 (已完成 {i}/{len(windows)} 个代码)")
            break
        df = None
        try:
            df = getattr(pro, api_name)(**{'ts_code': code, 'start_date': s, 'end_date': e, **extra})
        except Exception:
            try:
                df = getattr(pro, api_name)(**{'ts_code': code, **extra})
            except Exception:
                df = None
        if df is not None and len(df) > 0:
            all_dfs.append(df)
        time.sleep(sleep)
        if (i+1) % 200 == 0:
            total = sum(len(x) for x in all_dfs)
            print(f"  ...{code} ({total:,} rows, {i+1}/{len(windows)})")
    if all_dfs:
        return pd.concat(all_dfs, ignore_index=True)
    return None


def fetch_fund_portfolio(quarters, sleep=0.15):
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
        print(f"  ...{q} ({total:,} rows)")
    if all_fp:
        return pd.concat(all_fp, ignore_index=True)
    return None


# ---------------------------------------------------------------- 本地文件读取
def parquet_columns(path):
    try:
        import pyarrow.parquet as pq
        return list(pq.ParquetFile(path).schema_arrow.names)
    except Exception:
        try:
            return list(pd.read_parquet(path).columns)
        except Exception:
            return None


READ_FAILED = object()        # read_old_safe 的哨兵: 现有文件存在但读不出来


def read_parquet(path, columns=None):
    try:
        return pd.read_parquet(path, columns=columns)
    except Exception as e:
        print(f"  ⚠️ 读取失败 {path}: {str(e)[:100]}")
        return None


def resolve_path(spec):
    """读写同一份文件: 优先脚本目标路径; 目标不存在而已有历史落盘位置时, 直接以该位置为准
    (避免同一张表被增量下载拆成两个文件), 都不存在时用目标路径新建。返回 (路径, 是否回退)。"""
    p = spec['path']
    if os.path.exists(p):
        return p, None
    for alt in spec.get('alt_paths', []):
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
    keys = [c for c in spec.get('key', []) if c in new.columns]
    if keys:
        return keys
    date_col = spec.get('date_col', 'trade_date')
    return (['ts_code'] if 'ts_code' in new.columns else []) + \
           ([date_col] if date_col in new.columns else [])


def _is_text(dt):
    return dt.kind in ('O', 'U', 'S') or pd.api.types.is_string_dtype(dt)


def _is_num(dt):
    return dt.kind in ('i', 'u', 'f')


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
            if an.kind == 'f' and ao.kind == 'i' and not new[col].dropna().mod(1).eq(0).all():
                old[col] = old[col].astype(float)
            else:
                new[col] = new[col].astype(ao)
        elif an_num and ao_text:
            new[col] = new[col].map(str)            # 旧行是字符串 → 新行对齐成字符串
        elif an_text and ao_num:
            conv = pd.to_numeric(new[col], errors='coerce')
            if isinstance(conv, pd.Series) and len(conv) and conv.notna().all():
                new[col] = conv.astype(ao)          # 新列可整体转数值 → 对齐旧列
            else:
                old[col] = old[col].map(str)        # 否则两列统一成字符串
    return new


def merge_rows(old, new, spec, name):
    """新旧数据 concat → 按业务主键去重(保留新下载的一份, 容忍重述) → 按日期排序"""
    keys = dedup_keys(new, spec)
    if old is not None and len(old):
        new = match_dtypes(new, old)
        merged = pd.concat([old, new], ignore_index=True)
        merged = merged.drop_duplicates(subset=keys, keep='last')
    else:
        merged = new
    date_col = spec.get('date_col', 'trade_date')
    sort_cols = [c for c in [date_col] + keys if c in merged.columns]
    if not sort_cols:
        sort_cols = [c for c in merged.columns[:2]]
    return merged.sort_values(sort_cols).reset_index(drop=True)


# ---------------------------------------------------------------- 预算 / 分区 / 新鲜度
def budget_left():
    """剩余预算秒数; None = 未设预算"""
    return None if _DEADLINE is None else _DEADLINE - time.time()


def budget_exhausted():
    left = budget_left()
    return left is not None and left <= 0


def partition_dates(root):
    """<root>/date=YYYYMMDD/ 分区目录里的日期 (yyyymmdd 升序); 目录不存在返回 []"""
    try:
        names = os.listdir(root)
    except OSError:
        return []
    out = []
    for n in names:
        if not n.startswith('date='):
            continue
        raw = n[5:].replace('-', '')
        if len(raw) == 8 and raw.isdigit():
            out.append(raw)
    return sorted(out)


def partition_dir_of(spec):
    """日期表的分区目录 (仅当目录里确有 date=* 分区时返回) — adj_factor 的历史只以分区形式存在,
    只查 <name>.parquet 会误判为『文件不存在』→ 触发 1645 天全历史回补。"""
    root = spec.get('partition_dir') or os.path.join(OUTPUT, spec['name'])
    return root if partition_dates(root) else None


def parquet_max_date(path, col='trade_date'):
    """只读 row-group 统计拿最大日期 (不整表 collect)"""
    import pyarrow.parquet as pq
    pf = pq.ParquetFile(path)
    if col not in pf.schema_arrow.names:
        return None
    idx = pf.schema_arrow.names.index(col)
    mx = None
    for rg in range(pf.metadata.num_row_groups):
        st = pf.metadata.row_group(rg).column(idx).statistics
        if st is None or not st.has_min_max:
            continue
        v = str(st.max).replace('-', '')[:8]
        if len(v) == 8 and v.isdigit() and (mx is None or v > mx):
            mx = v
    return mx


def table_max_date(spec):
    """该表本地覆盖到的最新日期 (单文件 → alt_paths → date=* 分区), 用于核心覆盖判定"""
    if spec['kind'] not in ('date', 'code', 'stock'):
        return None
    p, _alt = resolve_path(spec)
    mx = parquet_max_date(p, spec.get('date_col', 'trade_date')) if os.path.exists(p) else None
    if mx:
        return mx
    pdates = partition_dates(partition_dir_of(spec))
    return pdates[-1] if pdates else None


def has_local_data(spec):
    """本地是否有该表的任何数据 (单文件 >1KB 或 有 date=* 分区); 没有任何数据 = 从未建成"""
    p, _alt = resolve_path(spec)
    if os.path.exists(p) and os.path.getsize(p) > 1000:
        return True
    return bool(partition_dates(partition_dir_of(spec)))


def write_partitions(df, root, spec, name):
    """把按日下载的数据写成 <root>/date=YYYYMMDD/data.parquet 分区 (adj_factor 的活布局)。
    已存在的分区按业务主键去重合并, 保证可重跑。"""
    date_col = spec.get('date_col', 'trade_date')
    if df is None or len(df) == 0 or date_col not in df.columns:
        print(f"  ⚠️ {name}: 无可写分区数据 ({date_col} 列缺失或空)")
        return 0
    keys = dedup_keys(df, spec)
    dcol = df[date_col].astype(str).str.replace('-', '').str.slice(0, 8)
    wrote = rows = 0
    for d, grp in df.groupby(dcol, sort=True):
        dst_dir = os.path.join(root, f'date={d}')
        os.makedirs(dst_dir, exist_ok=True)
        fp = os.path.join(dst_dir, 'data.parquet')
        try:
            if os.path.exists(fp):
                old = read_parquet(fp)
                if old is None:
                    print(f"  ⚠️ {name}: 分区 {d} 旧文件读不出, 跳过该日")
                    continue
                grp = match_dtypes(grp, old)
                merged = pd.concat([old, grp], ignore_index=True)\
                           .drop_duplicates(subset=keys, keep='last')
            else:
                merged = grp
            tmp = fp + '.tmp'
            merged.to_parquet(tmp, index=False)
            os.replace(tmp, fp)
            wrote += 1
            rows += len(merged)
        except Exception as e:
            print(f"  ⚠️ {name}: 分区 {d} 写入失败 {str(e)[:80]}")
    print(f"  ✅ {name}: {rows:,} 行 → {wrote} 个 date=* 分区 ({root})")
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
    date_col = spec.get('date_col', 'trade_date')
    keys = dedup_keys(new, spec)
    nd = new.copy()
    nd[date_col] = nd[date_col].astype(str).str.replace('-', '').str.slice(0, 8)
    nd = nd.drop_duplicates(subset=keys, keep='last')
    new_dates = sorted(nd[date_col].unique().tolist())
    try:
        pf = pq.ParquetFile(path)
        schema = pf.schema_arrow
        old_rows = pf.metadata.num_rows
        if date_col not in schema.names:
            print(f"  ❌ {name}: 现有文件缺 {date_col} 列, 放弃写入")
            return False
    except Exception as e:
        print(f"  ❌ {name}: 现有文件不可读 ({str(e)[:80]}), 放弃写入以免覆盖")
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
    tmp = f'{path}.tmp'
    dropped = 0
    # 按旧文件 row group 逐块读写: 内存只跟单个 row group 成正比
    # (daily_basic 的 row group = 100k 行 ≈ 60MB; 用 iter_batches 小批次反而因读缓冲不释放冲到 904MB)。
    rg_rows = 1_000_000
    comp = 'snappy'
    try:
        rg_rows = max(pf.metadata.row_group(i).num_rows for i in range(pf.metadata.num_row_groups))
        comp = (pf.metadata.row_group(0).column(0).compression or 'snappy').lower()
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
    print(f"  ✅ {name}: 追加 {new_tbl.num_rows:,} 行 ({new_dates[0]}~{new_dates[-1]}, "
          f"替换旧行 {dropped:,}) → 共 {total:,} 行, {sz:.1f} MB → {path} (流式合并+原子替换)")
    return True


# ---------------------------------------------------------------- 覆盖情况检查
def coverage_of_date_table(spec, path, cols):
    """返回 (max_trade_date, 当日股票数, 总行数)。
    只读 footer 统计 + 尾部 row group, 不整表读入 — daily_basic 整表读入实测 2.98GB RSS,
    会顶爆 cron worker 的 cgroup 预算。"""
    date_col = spec.get('date_col', 'trade_date')
    if not cols or date_col not in cols:
        return None, None, 0
    import pyarrow.parquet as pq
    try:
        pf = pq.ParquetFile(path)
    except Exception as e:
        print(f"  ⚠️ 读取失败 {path}: {str(e)[:100]}")
        return None, None, 0
    total = pf.metadata.num_rows
    mx = parquet_max_date(path, date_col)
    if not mx:
        return None, None, total
    n_last = None
    if 'ts_code' in pf.schema_arrow.names:
        codes = set()
        for rg in range(max(0, pf.metadata.num_row_groups - 2), pf.metadata.num_row_groups):
            t = pf.read_row_group(rg, columns=[date_col, 'ts_code'])
            ds = [str(v).replace('-', '')[:8] for v in t.column(date_col).to_pylist()]
            cs = t.column('ts_code').to_pylist()
            codes |= {c for d, c in zip(ds, cs) if d == mx}
        n_last = len(codes) if codes else None
    return mx, n_last, total


def coverage_of_stock_table(spec, path, cols):
    """per ts_code 的最新覆盖日期 (公告日期优先, 缺失时回退后续候选列)"""
    if not cols or 'ts_code' not in cols:
        return {}
    cov_cols = [c for c in spec['coverage'] if c in cols]
    if not cov_cols:
        return {}
    cur = read_parquet(path, ['ts_code'] + cov_cols)
    if cur is None or len(cur) == 0:
        return {}
    key = pd.to_numeric(cur[cov_cols[0]], errors='coerce')
    for c in cov_cols[1:]:                       # 首选列为空的行回退到后续候选列
        key = key.fillna(pd.to_numeric(cur[c], errors='coerce'))
    out = cur.assign(_k=key).groupby('ts_code')['_k'].max().dropna()
    return {str(k): f'{int(v):08d}' for k, v in out.items()}


def coverage_of_code_table(spec, path, cols):
    """per ts_code 的最新 trade_date (指数日线/申万行业日线)"""
    if not cols or 'trade_date' not in cols:
        return {}
    cur = read_parquet(path, ['ts_code', 'trade_date'])
    if cur is None or len(cur) == 0:
        return {}
    out = cur.groupby('ts_code')['trade_date'].max()
    return {str(k): str(v) for k, v in out.items()}


# ---------------------------------------------------------------- CLI
def parse_args(argv=None):
    ap = argparse.ArgumentParser(
        description='Zer0share tushare 下载脚本: 默认增量刷新, --mode full 保持原全量行为')
    ap.add_argument('--mode', choices=['incremental', 'full'], default='incremental',
                    help='incremental=按缺口增量刷新(默认); full=原全量行为')
    ap.add_argument('--tables', default='',
                    help='逗号分隔表名过滤, 如 moneyflow,moneyflow_ind; 留空=全部')
    ap.add_argument('--end', default=None,
                    help='截止日期 yyyymmdd, 默认: incremental=今天, full=原 END(%s)' % END)
    ap.add_argument('--dry-run', action='store_true',
                    help='只打印计划: 每张表已覆盖到的最后日期 + 将下载的范围; 不发网络请求、不写文件')
    ap.add_argument('--include-missing', action='store_true',
                    help='增量模式下也回补本地从未建成的表 (全历史, 极慢, 默认关闭)')
    ap.add_argument('--budget-seconds', type=float, default=None,
                    help=f'增量模式时间预算秒数 (默认 {DEFAULT_BUDGET_SECONDS:.0f}); 耗尽后剩余表标 DEFERRED(budget)')
    return ap.parse_args(argv)


def select_tables(table_arg):
    all_names = [t['name'] for t in TABLES]
    if not table_arg:
        return list(TABLES)
    wanted = [t.strip() for t in table_arg.split(',') if t.strip()]
    unknown = [w for w in wanted if w not in all_names]
    if unknown:
        print(f"⚠️ 未知表名: {unknown}")
        print(f"   可用表名: {','.join(all_names)}")
    return [t for t in TABLES if t['name'] in wanted]


def order_tables(selected):
    """核心表优先 (PRIORITY 顺序), 其余保持注册顺序; 同名表稳定排序"""
    def key(item):
        i, spec = item
        try:
            return (PRIORITY.index(spec['name']), i)
        except ValueError:
            return (len(PRIORITY), i)
    return [s for _i, s in sorted(enumerate(selected), key=key)]


def core_coverage(end):
    """核心表覆盖判定 → (目标交易日, {表名: (本地最新日期, 是否达标)})"""
    target = last_trading_day(end)
    out = {}
    for spec in TABLES:
        if spec['name'] not in CORE_TABLES:
            continue
        mx = table_max_date(spec)
        out[spec['name']] = (mx, bool(mx and mx >= target))
    return target, out


# ---------------------------------------------------------------- 股票代码来源
def codes_for(spec, args, ctx):
    """股票类表的代码清单: 本地优先 (dry-run 绝不联网), 缺失时非 dry-run 才在线取"""
    src = spec.get('codes_from', 'stock_basic')
    key = 'cb' if src == 'cb_basic' else 'all'
    if key in ctx:
        return ctx[key]
    codes = []
    if src == 'stock_basic':
        p = f'{OUTPUT}/stock_basic.parquet'
        if os.path.exists(p):
            df = read_parquet(p, ['ts_code'])
            if df is not None:
                codes = [str(c) for c in df['ts_code'].dropna().unique().tolist()]
        if not codes and not args.dry_run:
            df = pro.stock_basic(exchange='', list_status='L', fields='ts_code')
            if df is not None and len(df):
                codes = [str(c) for c in df['ts_code'].tolist()]
                if not os.path.exists(p):
                    save(df, p, 'stock_basic')
    else:  # cb_basic
        p = f'{OUTPUT}/cb_basic.parquet'
        if os.path.exists(p):
            df = read_parquet(p, ['ts_code'])
            if df is not None:
                codes = [str(c) for c in df['ts_code'].dropna().unique().tolist()]
        if not codes:
            p2 = f'{OUTPUT}/cb_daily.parquet'      # 退而求其次: 现有 cb_daily 里出现过的 code
            if os.path.exists(p2):
                df = read_parquet(p2, ['ts_code'])
                if df is not None:
                    codes = [str(c) for c in df['ts_code'].dropna().unique().tolist()]
        if not codes and not args.dry_run:
            df = pro.cb_basic()
            if df is not None and len(df):
                codes = [str(c) for c in df['ts_code'].tolist()]
                if not os.path.exists(p):
                    save(df, p, 'cb_basic')
    ctx[key] = codes
    return codes


def read_old_safe(path, spec, name):
    """读现有完整文件; 文件存在却读不出来时返回哨兵 'FAIL', 避免用增量数据覆盖已有文件"""
    if not os.path.exists(path):
        return None
    old = read_parquet(path)
    if old is None:
        print(f"  ❌ {name}: 现有文件读取失败, 为避免覆盖已存在数据, 放弃写入")
        return READ_FAILED
    return old


# ---------------------------------------------------------------- 增量: 各类表
def inc_date_table(spec, args, end):
    name = spec['name']
    path, alt = resolve_path(spec)
    if alt:
        print(f"  📂 目标文件不存在, 覆盖情况取自 {alt}")
    cols = parquet_columns(path) if os.path.exists(path) else None
    last, n_last, total = coverage_of_date_table(spec, path, cols)
    if os.path.exists(path) and cols and 'trade_date' not in cols:
        print(f"  ❌ {name}: 现有文件缺少 trade_date 列, 无法增量, 跳过该表")
        return
    # 单文件不存在时, 覆盖情况必须取自 <name>/date=* 分区目录 (adj_factor 的活布局);
    # 不修这里就会一直误判『文件不存在』→ 每次全历史回补 1645 天 → 定时任务超时。
    part_root = None
    cov_printed = False
    if last is None:
        part_root = partition_dir_of(spec)
        if part_root:
            pds = partition_dates(part_root)
            last = pds[-1]
            print(f"  已覆盖: {last} (取自 date=* 分区 {part_root}, 共 {len(pds)} 个分区) "
                  f"→ 只补缺口, 不重下历史")
            cov_printed = True
    plan_start = next_day(last) if last else START
    # 非 optional 表缺文件时保留首次建表语义 (全历史回填), 但起点不早于 START
    if plan_start < START:
        plan_start = START
    dates, note = plan_trade_dates(plan_start, end, use_api=not args.dry_run)
    if last and not cov_printed:
        bits = []
        if total is not None:
            bits.append(f"共 {total:,} 行")
        if n_last:
            bits.append(f"最近一日 {n_last:,} 只股票")
        print(f"  已覆盖: {last}" + (f" ({', '.join(bits)})" if bits else ""))
    elif not last:
        print("  已覆盖: 无 (文件不存在或为空) → 计划全量区间")
    if not dates:
        print(f"  ✅ 无需下载: 已覆盖至 {last}, 晚于 --end({end})")
        return
    tail = f"; {note}" if note else ""
    per_day = f", 每个交易日约 {n_last:,} 只股票" if n_last else ""
    print(f"  计划下载: {dates[0]} ~ {dates[-1]}, {len(dates)} 个交易日{per_day}{tail}")
    if args.dry_run:
        return
    new = batch_by_date(spec.get('api', name), date_col=spec.get('date_col', 'trade_date'),
                        name=name, dates=dates)
    if new is None or len(new) == 0:
        print(f"  ⚠️ {name}: 计划区间无数据返回")
        return
    new = clean_df(new)
    if part_root:
        write_partitions(new, part_root, spec, name)
        return
    if os.path.exists(path):
        write_streamed(path, new, spec, name)
        return
    merged = merge_rows(None, new, spec, name)
    write_atomic(merged, path, name)


def inc_stock_table(spec, args, end, ctx):
    name = spec['name']
    path, alt = resolve_path(spec)
    if alt:
        print(f"  📂 目标文件不存在, 覆盖情况取自 {alt}")
    cols = parquet_columns(path) if os.path.exists(path) else None
    covered = coverage_of_stock_table(spec, path, cols)
    if spec['expected'] == 'quarter':
        thr = latest_expected_quarter(end)
        thr_desc = f'最新预期季末 {thr} (滞后 {QUARTER_LAG_DAYS} 天)'
    else:
        thr = last_trading_day(end, use_api=not args.dry_run)
        thr_desc = f'最近交易日 {thr}'
    codes = codes_for(spec, args, ctx) or sorted(covered)
    if not codes:
        print(f"  ⚠️ {name}: 本地无代码清单 (dry-run 不联网), 无法给出计划")
        return
    need = [c for c in codes if covered.get(c, '') < thr]
    fresh = sum(1 for c in need if not covered.get(c))
    print(f"  已覆盖: {len(codes) - len(need)}/{len(codes)} 只"
          + (f", 文件内最新日期 {max(covered.values())}" if covered else " (文件不存在或无可判定日期列)")
          + f"; 阈值={thr_desc}")
    if not need:
        print(f"  ✅ 无需下载: 全部已覆盖至 {thr}")
        return
    # 重叠窗口起点夹到 START 附近, 避免老股票(最新日期贴近 START)的窗口早于项目起点
    windows = [(c, max(days_ago(covered[c], 365), START) if covered.get(c) else START, end) for c in need]
    first_start = min(w[1] for w in windows)
    print(f"  计划下载: {len(need)} 只 (其中 {fresh} 只无历史, 从 {START} 起; "
          f"其余从自身最新日期前 365 天重叠, 窗口末端 {end}), 并集 {first_start} ~ {end}"
          + (f"; 例: {need[:3]}" if len(need) <= 20 else ""))
    if args.dry_run:
        return
    new = batch_by_stock_window(spec['api'], windows, name=name)
    if new is None or len(new) == 0:
        print(f"  ⚠️ {name}: 无数据返回")
        return
    new = clean_df(new)
    stat = file_stat(path)
    old = read_old_safe(path, spec, name)
    if old is READ_FAILED:
        return
    merged = merge_rows(old, new, spec, name)
    write_atomic(merged, path, name, expect_stat=stat)


def inc_code_table(spec, args, end):
    """指数日线/申万行业日线: 按代码增量"""
    name = spec['name']
    path, alt = resolve_path(spec)
    if alt:
        print(f"  📂 目标文件不存在, 覆盖情况取自 {alt}")
    cols = parquet_columns(path) if os.path.exists(path) else None
    covered = coverage_of_code_table(spec, path, cols)
    thr = last_trading_day(end, use_api=not args.dry_run)
    codes = spec['codes']
    need = [c for c in codes if covered.get(c, '') < thr]
    fresh = sum(1 for c in need if not covered.get(c))
    print(f"  已覆盖: {len(codes) - len(need)}/{len(codes)} 个代码"
          + (f", 文件内最新日期 {max(covered.values())}" if covered else " (文件不存在)")
          + f"; 阈值=最近交易日 {thr}")
    if not need:
        print(f"  ✅ 无需下载: 全部已覆盖至 {thr}")
        return
    windows = [(c, next_day(covered[c]) if covered.get(c) else START, end) for c in need]
    first_start = min(w[1] for w in windows)
    print(f"  计划下载: {len(need)} 个代码 (其中 {fresh} 个无历史, 从 {START} 起; "
          f"其余从自身最新日期次日起), 窗口并集 {first_start} ~ {end}; 例: {need[:3]}")
    if args.dry_run:
        return
    new = batch_by_stock_window(spec['api'], windows, name=name)
    if new is None or len(new) == 0:
        print(f"  ⚠️ {name}: 无数据返回")
        return
    new = clean_df(new)
    stat = file_stat(path)
    old = read_old_safe(path, spec, name)
    if old is READ_FAILED:
        return
    merged = merge_rows(old, new, spec, name)
    write_atomic(merged, path, name, expect_stat=stat)


def inc_quarter_table(spec, args, end):
    """基金持仓: 按季度增量, 已完整覆盖的季度跳过"""
    name = spec['name']
    path, _ = resolve_path(spec)
    cols = parquet_columns(path) if os.path.exists(path) else None
    covered = set()
    if not os.path.exists(path):
        print("  已覆盖: 无 (文件不存在) → 全部季度需下载")
    elif cols and 'end_date' in cols:
        cur = read_parquet(path, ['end_date'])
        if cur is not None:
            covered = {str(x) for x in cur['end_date'].dropna().unique().tolist()}
        print(f"  已覆盖: {len(covered)} 个季度"
              + (f" ({min(covered)}~{max(covered)})" if covered else "") + "")
    else:
        print(f"  ⚠️ {name}: 现有文件缺少 end_date 列 → 全部季度按未覆盖处理")
    quarters = [q for q in quarter_ends(START, end) if q not in covered]
    if not quarters:
        print(f"  ✅ 无需下载: 所有季度已覆盖")
        return
    print(f"  计划下载: {len(quarters)} 个季度, {quarters[0]} ~ {quarters[-1]} "
          f"(含 --end({end}) 所在季度, 未出数的季度由接口空返回跳过)")
    if args.dry_run:
        return
    new = fetch_fund_portfolio(quarters)
    if new is None or len(new) == 0:
        print(f"  ⚠️ {name}: 无数据返回")
        return
    new = clean_df(new)
    stat = file_stat(path)
    old = read_old_safe(path, spec, name)
    if old is READ_FAILED:
        return
    merged = merge_rows(old, new, spec, name)
    write_atomic(merged, path, name, expect_stat=stat)


def run_incremental(spec, args, end, ctx):
    """返回状态: 'ok' | 'deferred_missing' | 'deferred_budget'"""
    name = spec['name']
    # optional 表: 目标文件缺失时跳过, 绝不做全历史回填
    # (根因: index_dailybasic 目标文件从不存在, 旧逻辑每次回退到 1644 个交易日逐日下载, ~65s/天)
    if spec.get('optional'):
        p, _alt = resolve_path(spec)
        if not os.path.exists(p):
            print(f"⏭ {name}: 文件缺失且为 optional, 跳过 (不做全历史回填)")
            return 'deferred_missing'
    # 从未建成的表 (无单文件也无 date=* 分区): 默认不发任何请求。
    # 旧行为每晚会从零回补 1645 个交易日/整表 → 3600s 定时任务必然超时, 且核心表被挤在后面。
    if not args.include_missing and not has_local_data(spec):
        print(f"⏭ {name}: SKIP (missing, use --include-missing) — 本地无任何数据, 不回补全历史")
        return 'deferred_missing'
    kind = spec['kind']
    if kind == 'date':
        inc_date_table(spec, args, end)
    elif kind == 'stock':
        inc_stock_table(spec, args, end, ctx)
    elif kind == 'code':
        inc_code_table(spec, args, end)
    elif kind == 'quarter':
        inc_quarter_table(spec, args, end)
    else:  # legacy 表不在增量范围内, 沿用 skip_if_exists 的一次性下载
        p, name = spec['path'], spec['name']
        if skip_if_exists(p, name):
            return 'ok'
        if args.dry_run:
            print(f"  [dry-run] 文件不存在 → 将整表下载一次 (沿用 skip_if_exists 行为)")
            return 'ok'
        try:
            save(spec['fetch'](end), p, name)
        except Exception as e:
            print(f"  ❌ {name}: {str(e)[:80]}")
    return 'ok'


# ---------------------------------------------------------------- full 模式 (原行为)
def run_full(spec, args, end, ctx):
    p, name = spec['path'], spec['name']
    if skip_if_exists(p, name):
        return
    if args.dry_run:
        print(f"  [dry-run] 文件不存在 → full 模式将整表下载 ({START} ~ {end}), 不执行")
        return
    kind = spec['kind']
    if kind == 'legacy':
        if spec.get('tolerant'):        # 原脚本对这几张表单独 try/except, 其余异常直接中断
            try:
                save(spec['fetch'](end), p, name)
            except Exception as e:
                print(f"  ❌ {name}: {str(e)[:80]}")
        else:
            save(spec['fetch'](end), p, name)
    elif kind == 'date':
        df = batch_by_date(spec.get('api', name), date_col=spec.get('date_col', 'trade_date'), name=name)
        save(df, p, name)
    elif kind == 'stock':
        codes = codes_for(spec, args, ctx)
        if spec.get('range'):
            df = batch_by_stock(spec['api'], codes, name=name, start_date=START, end_date=end)
        else:
            df = batch_by_stock(spec['api'], codes, name=name)
        save(df, p, name)
    elif kind == 'code':
        all_dfs = []
        for c in spec['codes']:
            try:
                df = getattr(pro, spec['api'])(ts_code=c, start_date=START, end_date=end)
                if df is not None and len(df) > 0:
                    all_dfs.append(df)
            except Exception:
                pass
        if all_dfs:
            save(pd.concat(all_dfs, ignore_index=True), p, name)
    elif kind == 'quarter':
        try:
            df = fetch_fund_portfolio(quarter_ends(START, end))
            if df is not None:      # 原行为: 无数据时不调用 save
                save(df, p, name)
        except Exception as e:
            if spec.get('tolerant'):
                print(f"  ❌ {name}: {str(e)[:80]}")
            else:
                raise


# ---------------------------------------------------------------- 表注册表
# kind: date=按交易日, stock=按 ts_code, code=固定代码列表, quarter=按季度, legacy=一次性接口
# key:  业务主键候选列 (按实际存在的列取), 缺省退回 (ts_code, trade_date)/trade_date
def _stock(name, title, coverage, expected, key, date_col, range_=True, codes_from='stock_basic'):
    return dict(name=name, title=title, kind='stock', api=name,
                path=f'{OUTPUT}/{name}.parquet', coverage=coverage, expected=expected,
                key=key, date_col=date_col, range=range_, codes_from=codes_from)


def _date(name, title, path=None, api=None, optional=False):
    return dict(name=name, title=title, kind='date', api=api or name,
                path=path or f'{OUTPUT}/{name}.parquet', date_col='trade_date',
                optional=optional)


TABLES = [
    dict(name='stock_basic', title='[0] 获取股票列表', kind='legacy',
         path=f'{OUTPUT}/stock_basic.parquet', api='stock_basic',
         fetch=lambda end: pro.stock_basic(exchange='', list_status='L',
                                           fields='ts_code,symbol,name,area,industry,market,list_date,exchange')),
    _date('daily', '[1] daily (日线行情)'),
    _date('weekly', '[2] weekly (周线)'),
    _date('monthly', '[3] monthly (月线)'),
    _date('adj_factor', '[4] adj_factor (复权因子)'),
    _date('daily_basic', '[5] daily_basic (每日指标)'),
    _date('moneyflow', '[6] moneyflow (资金流向)'),
    _date('moneyflow_ind', '[6A] moneyflow_ind (行业资金流)'),
    _date('moneyflow_hsgt', '[6B] moneyflow_hsgt (沪深港通资金流向)'),
    _date('margin_detail', '[7] margin_detail (融资融券)'),
    _date('hk_hold', '[8] hk_hold (沪深港通持股)'),
    _date('top_list', '[9] top_list (龙虎榜)'),
    _date('top_inst', '[10] top_inst (龙虎榜机构)'),
    _stock('income', '[11] income (利润表)',
           ['announce_date', 'end_date'], 'quarter', ['ts_code', 'end_date', 'report_type'], 'end_date'),
    _stock('balancesheet', '[12] balancesheet (资产负债表)',
           ['announce_date', 'end_date'], 'quarter', ['ts_code', 'end_date', 'report_type'], 'end_date'),
    _stock('cashflow', '[13] cashflow (现金流量表)',
           ['announce_date', 'end_date'], 'quarter', ['ts_code', 'end_date', 'report_type'], 'end_date'),
    _stock('fina_indicator', '[14] fina_indicator (财务指标)',
           ['ann_date', 'end_date'], 'quarter', ['ts_code', 'end_date', 'report_type'], 'end_date'),
    _stock('dividend', '[15] dividend (分红)',
           ['ann_date', 'end_date'], 'quarter', ['ts_code', 'end_date', 'ann_date', 'div_proc'], 'end_date'),
    _stock('forecast', '[16] forecast (业绩预告)',
           ['ann_date', 'end_date'], 'quarter', ['ts_code', 'end_date', 'ann_date'], 'end_date'),
    _stock('stk_holdernumber', '[17] stk_holdernumber (股东户数)',
           ['ann_date', 'end_date'], 'quarter', ['ts_code', 'end_date'], 'end_date'),
    _stock('top10_holders', '[18] top10_holders (十大股东)',
           ['ann_date', 'end_date'], 'quarter', ['ts_code', 'end_date', 'holder_name'], 'end_date'),
    _stock('top10_floatholders', '[19] top10_floatholders (十大流通股东)',
           ['ann_date', 'end_date'], 'quarter', ['ts_code', 'end_date', 'holder_name'], 'end_date'),
    _stock('stk_managers', '[20] stk_managers (高管)',
           ['ann_date'], 'quarter', ['ts_code', 'ann_date', 'name', 'title'], 'ann_date', range_=False),
    _stock('stk_rewards', '[21] stk_rewards (高管薪酬)',
           ['ann_date', 'end_date'], 'quarter', ['ts_code', 'end_date', 'name'], 'end_date', range_=False),
    _date('stk_limit', '[22] stk_limit (涨跌停价)'),
    _date('suspend_d', '[23] suspend_d (停复牌)'),
    dict(name='index_daily', title='[24] index_daily (指数日线)', kind='code', api='index_daily',
         path=f'{INDEX_OUTPUT}/index_daily.parquet', alt_paths=[f'{OUTPUT}/index_daily.parquet'],
         codes=INDEX_CODES, date_col='trade_date'),
    # index_weight 无 ts_code 列, 主键必须带上 index_code+con_code, 否则会被 (trade_date) 去重压成每天一行
    dict(name='index_weight', title='[25] index_weight (指数权重)', kind='date', api='index_weight',
         path=f'{INDEX_OUTPUT}/index_weight.parquet', alt_paths=[f'{OUTPUT}/index_weight.parquet'],
         date_col='trade_date', key=['index_code', 'con_code', 'trade_date']),
    _date('index_dailybasic', '[26] index_dailybasic (指数每日指标)',
          path=f'{INDEX_OUTPUT}/index_dailybasic.parquet', optional=True),
    dict(name='sw_daily', title='[27] sw_daily (申万行业日线)', kind='code', api='sw_daily',
         path=f'{INDEX_OUTPUT}/sw_daily.parquet', alt_paths=[f'{OUTPUT}/sw_daily.parquet'],
         codes=SW_CODES, date_col='trade_date'),
    dict(name='cb_basic', title='[28] cb_basic (可转债列表)', kind='legacy',
         path=f'{OUTPUT}/cb_basic.parquet', api='cb_basic', tolerant=True,
         fetch=lambda end: pro.cb_basic()),
    _stock('cb_daily', '[29] cb_daily (可转债日线)',
           ['trade_date'], 'trade', ['ts_code', 'trade_date'], 'trade_date', codes_from='cb_basic'),
    dict(name='fund_nav', title='[30] fund_nav (基金净值)', kind='legacy',
         path=f'{OUTPUT}/fund_nav_510300.parquet', api='fund_nav', tolerant=True,
         fetch=lambda end: pro.fund_nav(ts_code='510300.SH')),
    dict(name='fund_portfolio', title='[30A] fund_portfolio (公募基金持仓)', kind='quarter',
         path=f'{OUTPUT}/fund_portfolio.parquet', api='fund_portfolio', date_col='end_date', tolerant=True,
         key=['ts_code', 'end_date', 'symbol', 'ann_date']),
    dict(name='fund_share', title='[30B] fund_share (基金份额)', kind='legacy',
         path=f'{OUTPUT}/fund_share.parquet', api='fund_share', tolerant=True,
         fetch=lambda end: batch_by_date('fund_share', name='fund_share')),
    dict(name='us_daily', title='[31] us_daily (美股日线)', kind='legacy',
         path=f'{US_OUTPUT}/us_daily.parquet', api='us_daily', api_name='us_daily',
         fetch=lambda end: batch_by_stock('us_daily', US_STOCKS, name='us_daily',
                                          start_date=START, end_date=end)),
    _date('shibor', '[32] shibor (上海银行间同业拆放利率)', path=f'{MACRO_OUTPUT}/shibor.parquet'),
    _date('repurchase', '[33] repurchase (国债回购利率)', path=f'{MACRO_OUTPUT}/repurchase.parquet'),
    dict(name='repo_daily', title='[34] repo_daily (国债回购日线)', kind='legacy',
         path=f'{MACRO_OUTPUT}/repo_daily.parquet', api='repo_daily', tolerant=True,
         fetch=lambda end: pro.repo_daily(ts_code='204001.SH', start_date=START, end_date=end)),
]


# ---------------------------------------------------------------- main
def main(argv=None):
    global _DEADLINE
    args = parse_args(argv)
    end = args.end or (END if args.mode == 'full' else datetime.now().strftime('%Y%m%d'))
    selected = order_tables(select_tables(args.tables))
    t0 = time.time()
    budget = None
    if args.mode == 'incremental':
        budget = DEFAULT_BUDGET_SECONDS if args.budget_seconds is None else float(args.budget_seconds)
        _DEADLINE = t0 + budget
    print("=" * 70)
    print(f"Zer0share {'全量下载 v2' if args.mode == 'full' else '增量刷新 v2'} (tushare: {HTTP_URL})")
    print(f"  模式: {args.mode}   时间: {START} ~ {end}   表: {len(selected)}/{len(TABLES)} 张"
          + ("   [DRY-RUN: 不联网/不写盘]" if args.dry_run else ""))
    if args.mode == 'incremental':
        print(f"  时间预算: {budget:.0f}s (--budget-seconds)   include_missing={args.include_missing}")
    print(f"  执行顺序: {', '.join(s['name'] for s in selected)}")
    print("=" * 70)
    if not args.dry_run:
        ensure_dirs()
    ctx = {}
    failures = []
    deferred_missing = []
    deferred_budget = []
    for idx, spec in enumerate(selected, 1):
        name = spec['name']
        if budget is not None and budget_exhausted():
            print(f"\n[{idx}/{len(selected)}] {name}: DEFERRED(budget)")
            deferred_budget.append(name)
            continue
        print(f"\n{spec['title']} ({idx}/{len(selected)})...")
        if budget is not None and not args.dry_run:
            print(f"  预算剩余: {budget_left():.0f}s")
        try:
            if args.mode == 'full':
                run_full(spec, args, end, ctx)      # 沿用原行为: 异常直接中断
            else:
                try:
                    status = run_incremental(spec, args, end, ctx)
                except Exception as e:
                    print(f"  ❌ {name}: {str(e)[:120]}")
                    traceback.print_exc()
                    failures.append(name)
                else:
                    if status == 'deferred_missing':
                        deferred_missing.append(name)
        except Exception as e:
            print(f"  ❌ {name}: {str(e)[:120]}")
            raise

    elapsed = time.time() - t0
    print(f"\n{'=' * 70}")
    if args.dry_run:
        print(f"Dry-run 完成 ({elapsed:.1f}s): 仅打印计划, 未发起网络请求, 未写入任何文件")
        print(f"  核心表: {', '.join(CORE_TABLES)}   预算: "
              + (f"{budget:.0f}s" if budget else "不限"))
        print(f"  SKIP(missing): {len(deferred_missing)} 张 → {','.join(deferred_missing) or '无'}")
        return 0

    target, cov = core_coverage(end)
    core_ok = all(ok for _mx, ok in cov.values())
    print(f"PARTIAL_OK core_ok={core_ok}  core_target={target}  "
          f"budget={('%ds' % budget) if budget else 'none'}  elapsed={elapsed:.1f}s")
    for name, (mx, ok) in cov.items():
        print(f"  core {name:<12} max_date={mx or '缺'}  {'OK' if ok else 'GAP'}")
    print(f"  deferred_missing({len(deferred_missing)}): {', '.join(deferred_missing) or '无'}")
    print(f"  deferred_budget({len(deferred_budget)}): {', '.join(deferred_budget) or '无'}")
    print(f"  failures({len(failures)}): {', '.join(failures) or '无'}")
    if failures:
        print(f"⚠️ 增量刷新部分表失败: {failures}")
    print(f"All done. Total time: {elapsed/60:.1f} min")

    print(f"\nFiles:")
    for root, dirs, files in os.walk('/quantdata/zer0share/data'):
        for f in sorted(files):
            if not f.endswith('.parquet'):
                continue
            rel = os.path.relpath(os.path.join(root, f), '/quantdata/zer0share/data')
            if 'date=' in rel or rel.count(os.sep) > 1:   # 分区文件跳过, 否则几千行噪声
                continue
            path = os.path.join(root, f)
            sz = os.path.getsize(path) / 1e6
            if sz > 0.01:
                print(f"  {rel}: {sz:.1f} MB")

    # 行情下载完成后增量重建板块资金流缓存 (独立 try/except: 构建失败不影响下载 exit code)
    # 预算已耗尽或剩余不足时直接跳过, 保证整体在窗口内结束 (该子步骤自带 7200s 超时)
    left = budget_left()
    if left is not None and left < 60:
        print(f"⏭ build_sector_fundflow: 预算剩余 {left:.0f}s < 60s, 跳过")
    else:
        try:
            import subprocess, sys
            script = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'build_sector_fundflow.py')
            to = int(min(7200, left)) if left is not None else 7200
            r = subprocess.run([sys.executable, script, '--incremental'], capture_output=True,
                               text=True, timeout=to)
            print(r.stdout)
            if r.returncode != 0:
                print(f"⚠️ build_sector_fundflow 退出码 {r.returncode}: {r.stderr[:300]}")
        except Exception as e:
            print(f"⚠️ build_sector_fundflow 失败(不影响行情下载): {e}")

    if failures:
        print("EXIT 1 (脚本异常/表失败)")
        return 1
    print(f"EXIT {0 if core_ok else 3} (核心表{'齐全' if core_ok else '有缺口'})")
    return 0 if core_ok else 3


if __name__ == '__main__':
    sys.exit(main())

