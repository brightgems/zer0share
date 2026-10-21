"""
sync_plan.py — 统一 `sync` 命令的表清单与引擎路由。

背景: 历史上 `sync` (zer0share.sync, 每日分区 + MetaStore 记账) 与
`sync-bulk` (zer0share.bulk, 原 download_all_v2 的增量语义) 是两套并行命令,
表名不同 (bulk `daily` ↔ catalog `daily_kline`)、落盘规范不同 (单文件 vs
date=* 分区), 造成同一份数据两个拷贝、query 读不到其中一份。

现在两套都写 catalog 声明的规范布局 (catalog.parquet_pattern 是唯一权威),
因此合并成**一个** `sync` 命令:
  - 表名统一用 catalog 名字 (daily_kline / basic / …)
  - 每张表按能力路由到对应引擎:
      * ENGINE_BULK — bulk 引擎: 缺口检测 + 内存安全的原子合并, 适合
        "按日/按股票/按代码" 的大表, 以及 bulk 独有的表
      * ENGINE_JOB  — job 引擎 (zer0share.sync): 交易日历驱动 + 重试 +
        通知, 适合快照表 / 多交易所 / 期货期权 ETF
  - 两引擎都写 date=* 分区或 data.parquet, 互不冲突。

ENGINE_BULK / ENGINE_JOB 只是"谁来拉数据"的分工, 不影响落盘格式。
"""
from __future__ import annotations

from dataclasses import dataclass, field

# ------------------------------------------------------------------ 引擎标识
ENGINE_BULK = "bulk"   # zer0share.bulk: 缺口检测 + 原子合并 + 内存安全
ENGINE_JOB = "job"     # zer0share.sync: 日历驱动 + 重试 + 通知

# ------------------------------------------------------------------ 表别名
# 旧命令的表名 → catalog 规范名。保留别名是为了不破坏既有脚本/习惯用法。
TABLE_ALIASES: dict[str, str] = {
    # bulk 侧旧名 → catalog 名
    "daily": "daily_kline",
    "stock_basic": "basic",
    # sw_classify / sw_member 由 industry 这一个 job 负责 (同一接口, 同一落盘根),
    # 没有独立的 job 注册项, 因此指向它。
    "sw_classify": "industry",
    "sw_member": "industry",
}

# bulk 引擎负责的表 (catalog 名)。
#
# 判定依据 (来自代码/磁盘事实, 不是猜测):
#
#   1. bulk 独有的表 — job 注册表里根本没有 (财报/资金流/港股/宏观/可转债 …)
#   2. 两边都有、且 bulk 侧实现更贴合的日频表:
#      bulk 按磁盘 parquet footer 统计做**缺口检测**(只读 row group 统计, 不整表
#      载入); job 侧依赖 MetaStore 记账 — 记账表为空时会从 first_date 全量重下。
#      这几张表磁盘已有多年历史, 交给 bulk 才能只补缺口。
#
# 明确不归 bulk 的例外 (交给 job 引擎):
#   - basic / etf_basic / opt_basic 等快照表 (无日期维度)
#   - index_weight (index_code=*/date=*/ 两级分区, 只有 IndexWeightStore 会写)
#   - trade_cal (exchange=*/ 布局)
#   - index_daily / sw_daily: bulk 侧仍写历史单文件位置
#     (stock/index_daily.parquet 等), 与 catalog 规范位置冲突
BULK_TABLES: frozenset[str] = frozenset({
    # ── 两边都有, 走 bulk (缺口检测更准) ──
    "daily_kline", "adj_factor", "daily_basic",
    "stk_limit", "suspend_d",
    # ── bulk 独有 ──
    "weekly", "monthly",
    "moneyflow", "moneyflow_ind", "moneyflow_hsgt",
    "margin_detail", "hk_hold", "top_list", "top_inst",
    "income", "balancesheet", "cashflow", "fina_indicator", "dividend",
    "forecast", "stk_holdernumber", "top10_holders", "top10_floatholders",
    "stk_managers", "stk_rewards",
    "cb_basic", "cb_daily",
    "fund_nav", "fund_portfolio", "fund_share",
    "us_daily",
    "shibor", "repurchase", "repo_daily",
    "index_dailybasic",
})

# job 引擎负责的表 (catalog 名)。剩下的全部归 job 引擎:
#   快照表 (无日期维度) / 期货 / 期权 / ETF / 行业分类 / 自然日表 / 多交易所表
JOB_TABLES: frozenset[str] = frozenset({
    # 快照 / 日历 / 两级分区
    "basic", "trade_cal", "index_weight",
    # 股票维度小表 (job 侧有 retry + 通知, 且已按 catalog 路径落盘)
    "stock_st", "share_float", "block_trade", "margin",
    # 指数 / 行业
    "index_daily", "idx_anns", "sw_daily", "ci_member", "industry",
    # ETF
    "etf_basic", "etf_index", "fund_daily", "fund_adj",
    "etf_share_size", "etf_sh_cons",
    # RiceQuant
    "ricequant_basic", "ricequant_stock_minute",
    "ricequant_etf_basic", "ricequant_etf_minute",
    # 期货
    "fut_basic", "fut_daily", "fut_holding", "fut_wsr", "fut_settle",
    "fut_mapping", "ft_limit", "fut_weekly", "fut_monthly",
    "fut_index_daily", "fut_weekly_detail",
    # 期权
    "opt_basic", "opt_daily",
})

# ------------------------------------------------------------------ 主题分组
# 统一后的主题。旧的 --stock/--futures/--options/--etf/--ricequant 保留为别名。
TOPICS: dict[str, tuple[str, ...]] = {
    "quote": (
        "daily_kline", "weekly", "monthly", "adj_factor", "daily_basic",
        "stk_limit", "suspend_d", "stock_st", "share_float", "block_trade",
    ),
    "fundflow": (
        "moneyflow", "moneyflow_ind", "moneyflow_hsgt",
        "margin", "margin_detail", "hk_hold",
    ),
    "toplist": ("top_list", "top_inst"),
    "finance": (
        "income", "balancesheet", "cashflow", "fina_indicator", "dividend",
        "forecast", "stk_holdernumber", "top10_holders", "top10_floatholders",
        "stk_managers", "stk_rewards",
    ),
    "index": (
        "index_daily", "index_weight", "index_dailybasic", "sw_daily",
        "idx_anns", "industry", "ci_member",
    ),
    "bond": ("cb_basic", "cb_daily"),
    "fund": (
        "fund_nav", "fund_portfolio", "fund_share",
        "fund_daily", "fund_adj", "etf_basic", "etf_index",
        "etf_share_size", "etf_sh_cons",
    ),
    "us": ("us_daily",),
    "macro": ("shibor", "repurchase", "repo_daily"),
    "futures": (
        "fut_basic", "fut_daily", "fut_holding", "fut_wsr", "fut_settle",
        "fut_mapping", "ft_limit", "fut_weekly", "fut_monthly",
        "fut_index_daily", "fut_weekly_detail",
    ),
    "options": ("opt_basic", "opt_daily"),
    "etf": (
        "etf_basic", "etf_index", "fund_daily", "fund_adj",
        "etf_share_size", "etf_sh_cons",
    ),
    "ricequant": (
        "ricequant_basic", "ricequant_stock_minute",
        "ricequant_etf_basic", "ricequant_etf_minute",
    ),
    "basic": ("basic", "trade_cal"),
}

# 旧主题开关 → 新主题 (兼容老用法)
LEGACY_TOPIC_ALIASES: dict[str, str] = {
    "stock": "quote",
    "futures": "futures",
    "options": "options",
    "etf": "etf",
    "ricequant": "ricequant",
}

# 旧主题开关 (--stock/--futures/...) 各自对应的表清单。
#
# 这些开关保持**原语义不变** (与合并前的 sync 命令一致): 它们是"按市场分组"
# 的历史视图, 而不是新的功能主题。因此这里的表清单照搬旧 CLI 的常量,
# 不随 TOPICS 重构而变化 —— 否则 `--etf` 之类的老脚本会突然多拉或少拉表。
LEGACY_FLAG_TABLES: dict[str, tuple[str, ...]] = {
    "stock": (
        "basic", "trade_cal", "daily_kline", "adj_factor", "daily_basic",
        "stock_st", "suspend_d", "stk_limit", "share_float", "block_trade",
        "margin", "index_daily", "index_weight", "idx_anns", "industry",
        "sw_daily", "ci_member",
    ),
    "futures": (
        "fut_basic", "fut_daily", "fut_holding", "fut_wsr", "fut_settle",
        "fut_mapping", "ft_limit", "fut_weekly", "fut_monthly",
        "fut_index_daily", "fut_weekly_detail",
    ),
    "options": ("opt_basic", "opt_daily"),
    "etf": (
        "fund_daily", "fund_adj", "etf_share_size", "etf_sh_cons",
        "etf_basic", "etf_index",
    ),
    "ricequant": (
        "ricequant_basic", "ricequant_stock_minute",
        "ricequant_etf_basic", "ricequant_etf_minute",
    ),
}

ALL_TOPICS: tuple[str, ...] = tuple(TOPICS)


@dataclass(frozen=True)
class TablePlan:
    """一张表在统一 sync 中的计划。"""
    name: str                 # catalog 规范名
    engine: str               # ENGINE_BULK / ENGINE_JOB
    topics: tuple[str, ...] = field(default=())   # 所属主题

    @property
    def is_bulk(self) -> bool:
        return self.engine == ENGINE_BULK

    @property
    def is_job(self) -> bool:
        return self.engine == ENGINE_JOB


def normalize_table(name: str) -> str:
    """把别名/大小写不敏感的表名归一到 catalog 规范名。"""
    n = name.strip()
    low = n.lower()
    if low in TABLE_ALIASES:
        return TABLE_ALIASES[low]
    return n


def resolve_topic(name: str) -> str | None:
    """主题名归一化 (含旧开关别名); 未知主题返回 None。"""
    t = name.strip().lower()
    if t in TOPICS:
        return t
    return LEGACY_TOPIC_ALIASES.get(t)


def _build_plans() -> dict[str, TablePlan]:
    by_topic: dict[str, list[str]] = {}
    for topic, tables in TOPICS.items():
        for t in tables:
            by_topic.setdefault(t, []).append(topic)

    plans: dict[str, TablePlan] = {}
    for name in sorted(set(by_topic) | BULK_TABLES | JOB_TABLES):
        if name in BULK_TABLES:
            engine = ENGINE_BULK
        elif name in JOB_TABLES:
            engine = ENGINE_JOB
        else:
            # 没在任何一组里 → 默认交给 job 引擎 (它是通用日历驱动实现)
            engine = ENGINE_JOB
        plans[name] = TablePlan(
            name=name,
            engine=engine,
            topics=tuple(sorted(by_topic.get(name, ()))),
        )
    return plans


PLANS: dict[str, TablePlan] = _build_plans()


def engine_of(name: str) -> str:
    return PLANS[normalize_table(name)].engine


def table_names() -> list[str]:
    return sorted(PLANS)


def tables_for_topic(topic: str) -> list[str]:
    resolved = resolve_topic(topic)
    if resolved is None:
        return []
    return list(TOPICS[resolved])


def all_table_names() -> list[str]:
    """全部表名 (catalog 规范名), 供 --all 使用。"""
    return table_names()


def bulk_table_names() -> list[str]:
    """需要交给 bulk 引擎的表名 (已归一为 catalog 名)。"""
    return [n for n, p in PLANS.items() if p.is_bulk]
