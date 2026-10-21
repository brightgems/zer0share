"""
sync_runner.py — 统一 `sync` 命令的执行层。

按 sync_plan.PLANS 的路由把每张表交给对应引擎:
  * ENGINE_BULK → zer0share.bulk (缺口检测 + 内存安全原子合并)
  * ENGINE_JOB  → zer0share.pipeline.Pipeline (日历驱动 + 重试 + 通知)

两个引擎写同一套 catalog 规范布局, 因此可以混跑而不产生重复拷贝。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from types import SimpleNamespace

from loguru import logger

from zer0share import sync_plan as SP


class DateRangeNotSupported(ValueError):
    """表不支持 --start-date/--end-date (快照表 / 多交易所表)。"""


@dataclass
class SyncReport:
    """一次统一 sync 的执行结果。"""
    done: list[str] = field(default_factory=list)
    failed: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    bulk_exit: int | None = None

    @property
    def ok(self) -> bool:
        return not self.failed

    def summary(self) -> str:
        parts = [f"完成 {len(self.done)} 张"]
        if self.skipped:
            parts.append(f"跳过 {len(self.skipped)} 张")
        if self.failed:
            parts.append(f"失败 {len(self.failed)} 张")
        return "｜".join(parts)


def _bulk_table_specs(cfg, tables: list[str]):
    """把 catalog 规范表名映射到 bulk 侧的注册项。

    bulk 的注册表用旧表名 (daily / stock_basic), 这里双向映射:
      catalog 名 → bulk 注册名 → 返回该 spec
    bulk 里没有的表返回 None。
    """
    from zer0share.bulk import build_tables, dirs_for

    specs = {s["name"]: s for s in build_tables(dirs_for(cfg), cfg)}
    reverse = {SP.TABLE_ALIASES.get(k, k): v for k, v in specs.items()}
    out = []
    for t in tables:
        spec = reverse.get(t)
        if spec is not None:
            out.append(spec)
        else:
            logger.debug(f"bulk 引擎无注册项: {t}")
    return out


def run_bulk_tables(cfg, tables: list[str], *, dry_run: bool = False,
                    budget_seconds: float | None = None,
                    start: str | None = None,
                    end: str | None = None,
                    include_missing: bool = False) -> tuple[list[str], list[str], int | None]:
    """跑 bulk 引擎负责的表。

    返回 (成功表名, 失败表名, bulk 退出码)。
    直接复用 bulk.main 的循环逻辑, 但只跑给定的那几张表。
    """
    import datetime as dt
    import time

    from zer0share import bulk

    specs = _bulk_table_specs(cfg, tables)
    if not specs:
        return [], [], None

    bulk._DEADLINE = None
    budget = None
    if not dry_run:
        budget = 780.0 if budget_seconds is None else float(budget_seconds)
        bulk._DEADLINE = time.time() + budget

    dirs = bulk.dirs_for(cfg)
    pro = bulk.make_pro(cfg)
    end_date = end or dt.datetime.now().strftime("%Y%m%d")
    assert end_date is not None

    # 注意: 不能用 class 体内赋值 — 类体不参与闭包作用域, `dry_run = dry_run`
    # 在类体里是 NameError。用 SimpleNamespace 承载 argparse 风格的属性。
    args = SimpleNamespace(
        mode="incremental",
        tables="",
        topic="",
        start=start,
        end=end_date,
        dry_run=dry_run,
        include_missing=include_missing,
        budget_seconds=budget,
    )
    ctx: dict = {}
    done: list[str] = []
    failed: list[str] = []
    selected = bulk.order_tables(specs)
    for spec in selected:
        name = spec["name"]
        canonical = SP.TABLE_ALIASES.get(name, name)
        if budget is not None and bulk.budget_exhausted():
            logger.info(f"⏭ {canonical}: DEFERRED(budget)")
            continue
        try:
            status = bulk.run_incremental(pro, spec, args, end_date, ctx, dirs)
            if status == "ok":
                done.append(canonical)
        except Exception as exc:  # noqa: BLE001 — 单表失败不应中断整批
            logger.info(f"❌ {canonical}: {str(exc)[:160]}")
            failed.append(canonical)
    return done, failed, (0 if not failed else 1)


def run_job_tables(cfg, tables: list[str], *, start_date: str | None = None,
                   end_date: str | None = None,
                   pipeline_factory=None) -> tuple[list[str], list[str]]:
    """跑 job 引擎 (zer0share.sync) 负责的表。返回 (成功, 失败)。

    pipeline_factory: 可选的 pipeline 工厂 (默认自建)。CLI 传入 zer0share.cli
    的 _make_pipeline, 这样测试可以 patch 该入口, 也让 job 引擎与 CLI 共用
    同一套配置/数据源构造逻辑 ( RiceQuant 开关、notifier 等)。
    """
    done: list[str] = []
    failed: list[str] = []

    if pipeline_factory is not None:
        with pipeline_factory() as pipeline:
            return _run_job_tables_with(pipeline, tables,
                                         start_date=start_date, end_date=end_date)

    from zer0share.pipeline import Pipeline
    from zer0share.sources import DataSources, RiceQuantFetcher, TushareFetcher
    from zer0share.notifier import NotifierProtocol, build_notifier

    sources = DataSources(
        tushare=TushareFetcher(cfg.tushare_token, cfg.tushare_http_url),
        ricequant=(
            RiceQuantFetcher(
                username=cfg.ricequant.username,
                password=cfg.ricequant.password,
                license_key=cfg.ricequant.license_key,
            )
            if cfg.ricequant.enabled
            else None
        ),
    )
    notifier: NotifierProtocol = build_notifier(cfg.notifier)
    with Pipeline(cfg, sources, notifier) as pipeline:
        return _run_job_tables_with(pipeline, tables,
                                     start_date=start_date, end_date=end_date)


def _run_job_tables_with(pipeline, tables: list[str], *,
                         start_date: str | None = None,
                         end_date: str | None = None) -> tuple[list[str], list[str]]:
    """在给定 pipeline 上逐表执行 job 引擎的任务。

    不支持日期区间的表 (快照表 / 多交易所表) 在给出区间时报错 —— 早期版本
    直接传进 pipeline, 结果由 job 内部抛 ValueError, CLI 层拿到的 exit code
    与提示信息都不明确。这里提前拦下, 由 CLI 转成 UsageError。
    """
    done: list[str] = []
    failed: list[str] = []
    registry = pipeline.registry
    for t in tables:
        if t not in registry:
            logger.info(f"⏭ {t}: job 引擎无注册项, 跳过")
            continue
        job = registry[t]
        if (start_date is not None or end_date is not None) and not job.supports_date_range:
            raise DateRangeNotSupported(
                f"{t} 不支持日期区间 "
                "(date range options are only supported for daily partitioned tables)"
            )
        try:
            pipeline.run(t, start_date=start_date, end_date=end_date)
            done.append(t)
        except Exception as exc:  # noqa: BLE001 — 单表失败不应中断整批
            logger.info(f"❌ {t}: {str(exc)[:160]}")
            failed.append(t)
    return done, failed


def partition_by_engine(tables: list[str]) -> tuple[list[str], list[str]]:
    """按引擎把表清单分成 (bulk 表, job 表)。"""
    bulk_tables, job_tables = [], []
    for t in tables:
        canonical = SP.normalize_table(t)
        plan = SP.PLANS.get(canonical)
        if plan is None:
            job_tables.append(canonical)
        elif plan.is_bulk:
            bulk_tables.append(canonical)
        else:
            job_tables.append(canonical)
    return bulk_tables, job_tables


def run_sync(cfg, tables: list[str], *, start_date: str | None = None,
             end_date: str | None = None, dry_run: bool = False,
             budget_seconds: float | None = None,
             include_missing: bool = False,
             pipeline_factory=None) -> SyncReport:
    """执行统一 sync。"""
    report = SyncReport()
    canonical = [SP.normalize_table(t) for t in tables]
    bulk_tables, job_tables = partition_by_engine(canonical)

    logger.info(f"统一 sync: 共 {len(canonical)} 张表 "
                f"(bulk 引擎 {len(bulk_tables)} / job 引擎 {len(job_tables)})")

    if bulk_tables:
        logger.info(f"→ bulk 引擎: {', '.join(bulk_tables)}")
        b_done, b_failed, exit_code = run_bulk_tables(
            cfg, bulk_tables, dry_run=dry_run, budget_seconds=budget_seconds,
            start=start_date, end=end_date, include_missing=include_missing)
        report.done.extend(b_done)
        report.failed.extend(b_failed)
        report.bulk_exit = exit_code

    if job_tables:
        logger.info(f"→ job 引擎: {', '.join(job_tables)}")
        j_done, j_failed = run_job_tables(cfg, job_tables, start_date=start_date,
                                          end_date=end_date,
                                          pipeline_factory=pipeline_factory)
        report.done.extend(j_done)
        report.failed.extend(j_failed)

    logger.info(f"统一 sync 结束: {report.summary()}")
    return report
