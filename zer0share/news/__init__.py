"""新闻数据：Tushare 新闻接口 → 原始 JSONL 归档 + SQLite 主库。

新闻逐条到达、会重复、会修订、需要检索，所以不走行情数据的 Parquet 分区存储。
"""

from zer0share.news.archive import RawArchive
from zer0share.news.client import TushareApiError, TushareHttpClient
from zer0share.news.specs import MAJOR_NEWS, NEWS, NEWS_APIS, NewsApiSpec
from zer0share.news.store import NewsRecord, NewsStore
from zer0share.news.syncer import NewsSyncer, SyncSummary

NEWS_DB = ("news", "news.db")   # 相对 data_dir 的主库路径
NEWS_RAW = ("news", "raw")      # 相对 data_dir 的原始归档目录

__all__ = [
    "MAJOR_NEWS", "NEWS", "NEWS_APIS", "NEWS_DB", "NEWS_RAW",
    "NewsApiSpec", "NewsRecord", "NewsStore", "NewsSyncer", "RawArchive",
    "SyncSummary", "TushareApiError", "TushareHttpClient",
]
