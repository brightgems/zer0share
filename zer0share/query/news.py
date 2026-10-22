"""新闻本地查询：以只读方式打开 SQLite 主库，返回与 Tushare 同名字段的 DataFrame。"""

import pandas as pd

from zer0share.news import MAJOR_NEWS, NEWS, NEWS_DB, NewsApiSpec, NewsStore
from zer0share.query import QueryContext
from zer0share.query.repository import parse_fields

# 除 Tushare 原有字段外，额外提供 news_id（去重主键）和 first_seen_at（首次入库时间，检查事后补录用）
_EXTRA = ["news_id", "first_seen_at"]
_COLUMNS = {
    MAJOR_NEWS.name: ["title", "content", "pub_time", "src", *_EXTRA],
    NEWS.name: ["datetime", "title", "content", "channels", "src", *_EXTRA],
}
_DEFAULT_FIELDS = {
    MAJOR_NEWS.name: ["title", "pub_time", "src", "news_id"],
    NEWS.name: ["datetime", "title", "content", "src", "news_id"],
}


def major_news(ctx: QueryContext, **kwargs) -> pd.DataFrame:
    return _query(ctx, MAJOR_NEWS, **kwargs)


def news(ctx: QueryContext, **kwargs) -> pd.DataFrame:
    return _query(ctx, NEWS, **kwargs)


def _query(
    ctx: QueryContext,
    spec: NewsApiSpec,
    start_date: str,
    end_date: str,
    src: str | None = None,
    keyword: str | None = None,
    fields=None,
) -> pd.DataFrame:
    """start_date / end_date 为北京时间 YYYYMMDD（含两端）；keyword 按标题全文检索（至少 3 个字）。"""
    columns = parse_fields(fields, _COLUMNS[spec.name]) if fields else _DEFAULT_FIELDS[spec.name]
    db_path = ctx.data_dir.joinpath(*NEWS_DB)
    if not db_path.exists():
        raise FileNotFoundError(f"新闻库不存在：{db_path}，请先运行 python main.py news sync")
    with NewsStore(db_path, readonly=True) as store:
        df = store.query(spec.name, start_date, end_date, src=src, keyword=keyword)
    return df.rename(columns={"pub_time": spec.time_field})[columns]
