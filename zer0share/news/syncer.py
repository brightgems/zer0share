"""新闻同步：按 (来源, 自然日) 拉取，返回条数达到上限就把时间窗对半切开重拉。

- 断点续跑：完整拉完的日期记入 sync_day，下次跳过（refresh=True 时强制重拉）；
- 当天及以后的日期不标记完成，因为当天的新闻还在增加；
- 权限类错误（TushareApiError）直接抛出，网络错误记入 fetch_log 后继续下一个窗口。
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
from loguru import logger

from zer0share.news.archive import RawArchive
from zer0share.news.client import TushareApiError, TushareHttpClient
from zer0share.news.specs import NewsApiSpec
from zer0share.news.store import FetchLogEntry, NewsRecord, NewsStore

_FMT = "%Y-%m-%d %H:%M:%S"
_TZ = ZoneInfo("Asia/Shanghai")


@dataclass
class SyncSummary:
    days: int = 0               # 本次完整同步的天数
    rows: int = 0               # 接口返回的总行数
    new: int = 0
    seen: int = 0
    revised: int = 0
    failed_windows: int = 0     # 网络等错误导致失败的窗口
    saturated_windows: int = 0  # 切到最小窗口仍被截断的窗口（可能漏数）

    def __str__(self) -> str:
        return (f"{self.days} 天，返回 {self.rows} 行：新增 {self.new}，重复 {self.seen}，"
                f"正文修订 {self.revised}｜失败窗口 {self.failed_windows}，截断窗口 {self.saturated_windows}")


class NewsSyncer:
    def __init__(
        self,
        client: TushareHttpClient,
        store: NewsStore,
        archive: RawArchive | None = None,
        min_window: timedelta = timedelta(minutes=1),
    ):
        self._client = client
        self._store = store
        self._archive = archive
        self._min_window = min_window  # 切窗的下限，防止无限切分

    def sync(
        self,
        spec: NewsApiSpec,
        start_date: str,
        end_date: str,
        sources: list[str] | None = None,
        with_content: bool = False,
        refresh: bool = False,
    ) -> SyncSummary:
        """同步 [start_date, end_date]（YYYYMMDD，含两端）内指定来源的新闻。"""
        summary = SyncSummary()
        today = datetime.now(_TZ).strftime("%Y%m%d")
        for src in sources or spec.sources:
            done = set() if refresh else self._store.done_days(spec.name, src)
            for day in _days(start_date, end_date):
                if day in done:
                    continue
                failed_before, rows_before = summary.failed_windows, summary.rows
                start = datetime.strptime(day, "%Y%m%d")
                self._fetch(spec, src, day, start, start + timedelta(days=1, seconds=-1), with_content, summary)
                if summary.failed_windows == failed_before and day < today:
                    self._store.mark_day_done(spec.name, src, day, summary.rows - rows_before)
                    summary.days += 1
        logger.info(f"{spec.name} 同步完成：{summary}")
        return summary

    def _fetch(
        self,
        spec: NewsApiSpec,
        src: str,
        day: str,
        start: datetime,
        end: datetime,
        with_content: bool,
        summary: SyncSummary,
    ) -> None:
        """拉取一个时间窗；被截断且窗口还能再切时，拆成两半递归拉取。"""
        win = (start.strftime(_FMT), end.strftime(_FMT))
        try:
            df = self._client.query(
                spec.name, {"src": src, "start_date": win[0], "end_date": win[1]}, spec.request_fields(with_content)
            )
        except TushareApiError:
            raise
        except Exception as e:  # 网络、解析等临时错误：记下来，本日不标记完成，下次重试
            logger.warning(f"{spec.name}/{src} {win[0]}~{win[1]} 失败：{e}")
            self._store.log_fetch(FetchLogEntry(spec.name, src, *win, None, error=str(e)[:500]))
            summary.failed_windows += 1
            return

        truncated = len(df) >= spec.row_cap
        if truncated and end - start > self._min_window:
            mid = start + (end - start) // 2
            self._fetch(spec, src, day, start, mid, with_content, summary)
            self._fetch(spec, src, day, mid + timedelta(seconds=1), end, with_content, summary)
            return

        if truncated:
            logger.warning(f"{spec.name}/{src} {win[0]}~{win[1]} 已到最小窗口仍被截断，可能漏数")
            summary.saturated_windows += 1
        self._store.log_fetch(FetchLogEntry(spec.name, src, *win, len(df), saturated=truncated))
        if self._archive is not None:
            self._archive.append(spec.name, src, day, df, *win)
        stats = self._store.upsert(_to_records(spec, src, df))
        summary.rows += len(df)
        summary.new += stats.new
        summary.seen += stats.seen
        summary.revised += stats.revised


def _to_records(spec: NewsApiSpec, src: str, df: pd.DataFrame) -> list[NewsRecord]:
    """把接口返回统一成 NewsRecord。缺秒的时间补 ':00'（同花顺偶尔只到分钟）。"""
    records = []
    for row in df.to_dict(orient="records"):
        pub_time = str(row[spec.time_field])
        if len(pub_time) == 16:
            pub_time += ":00"
        records.append(NewsRecord(
            api=spec.name,
            src=src,
            pub_time=pub_time,
            title=_text(row.get("title")) or "",
            content=_text(row.get("content")),
            channels=_text(row.get("channels")),
        ))
    return records


def _text(value) -> str | None:
    """接口里的空值可能是 None、NaN 或字符串 'None'，统一成 None。"""
    if value is None or (isinstance(value, float) and pd.isna(value)) or value in ("", "None"):
        return None
    return str(value)


def _days(start_date: str, end_date: str) -> list[str]:
    start, end = datetime.strptime(start_date, "%Y%m%d"), datetime.strptime(end_date, "%Y%m%d")
    return [(start + timedelta(days=i)).strftime("%Y%m%d") for i in range((end - start).days + 1)]
