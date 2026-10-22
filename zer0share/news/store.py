"""新闻主库（SQLite）。

表：
- news_item      一条新闻一行，news_id 由 (api, src, pub_time, 标题) 哈希得到，重复拉取自然去重；
- news_revision  同一条新闻正文变化时，保存被替换掉的旧正文；
- fetch_log      每次请求一行（时间窗、条数、是否截断、是否出错），用于排查缺口；
- sync_day       已完整同步的 (api, src, 日期)，用于断点续跑；
- news_fts       标题全文索引（trigram 分词，支持中文子串检索，关键词至少 3 个字）。
"""

import hashlib
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd

_SCHEMA = """
CREATE TABLE IF NOT EXISTS news_item (
    news_id       TEXT PRIMARY KEY,
    api           TEXT NOT NULL,
    src           TEXT NOT NULL,
    pub_time      TEXT NOT NULL,          -- 北京时间 'YYYY-MM-DD HH:MM:SS'
    title         TEXT NOT NULL,
    content       TEXT,
    channels      TEXT,
    content_hash  TEXT,
    first_seen_at TEXT NOT NULL,          -- 首次拉到的时间；远晚于 pub_time 说明是事后补录
    last_seen_at  TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_news_api_time ON news_item(api, pub_time);

CREATE TABLE IF NOT EXISTS news_revision (
    news_id      TEXT NOT NULL,
    content      TEXT,
    content_hash TEXT,
    replaced_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS fetch_log (
    api        TEXT NOT NULL,
    src        TEXT NOT NULL,
    win_start  TEXT NOT NULL,
    win_end    TEXT NOT NULL,
    n_rows     INTEGER,
    saturated  INTEGER NOT NULL DEFAULT 0,  -- 已切到最小窗口仍被截断：可能漏数
    error      TEXT,
    fetched_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sync_day (
    api         TEXT NOT NULL,
    src         TEXT NOT NULL,
    day         TEXT NOT NULL,             -- YYYYMMDD
    n_rows      INTEGER NOT NULL,
    finished_at TEXT NOT NULL,
    PRIMARY KEY (api, src, day)
);

CREATE VIRTUAL TABLE IF NOT EXISTS news_fts USING fts5(
    title, content='news_item', content_rowid='rowid', tokenize='trigram'
);
CREATE TRIGGER IF NOT EXISTS news_fts_ai AFTER INSERT ON news_item BEGIN
    INSERT INTO news_fts(rowid, title) VALUES (new.rowid, new.title);
END;
CREATE TRIGGER IF NOT EXISTS news_fts_ad AFTER DELETE ON news_item BEGIN
    INSERT INTO news_fts(news_fts, rowid, title) VALUES ('delete', old.rowid, old.title);
END;
"""


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _hash(text: str | None) -> str | None:
    return hashlib.sha1(text.encode()).hexdigest() if text else None


@dataclass(frozen=True)
class NewsRecord:
    """统一后的一条新闻。标题为空的快讯用正文开头代替标题参与去重。"""

    api: str
    src: str
    pub_time: str
    title: str
    content: str | None = None
    channels: str | None = None

    @property
    def news_id(self) -> str:
        key = self.title or (self.content or "")[:200]
        return _hash(f"{self.api}|{self.src}|{self.pub_time}|{key}")[:16]


@dataclass
class UpsertStats:
    new: int = 0
    seen: int = 0      # 已存在（重复拉到）
    revised: int = 0   # 已存在且正文变了


@dataclass(frozen=True)
class FetchLogEntry:
    api: str
    src: str
    win_start: str
    win_end: str
    n_rows: int | None
    saturated: bool = False
    error: str | None = None


class NewsStore:
    def __init__(self, db_path: str | Path, readonly: bool = False):
        self.db_path = Path(db_path)
        if readonly:
            self._conn = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True)
        else:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            self._conn = sqlite3.connect(self.db_path)
            self._conn.execute("PRAGMA journal_mode=WAL")  # 写入时允许其他进程并发读
            self._conn.executescript(_SCHEMA)
        self._conn.execute("PRAGMA busy_timeout=30000")

    # ---- 写入 ----

    def upsert(self, records: list[NewsRecord]) -> UpsertStats:
        """幂等写入：新条目插入；已有条目刷新 last_seen_at，正文变化时先把旧正文存入 news_revision。"""
        stats, now = UpsertStats(), _now()
        existing = self._existing_hashes([r.news_id for r in records])
        with self._conn:
            for r in records:
                new_hash = _hash(r.content)
                if r.news_id not in existing:
                    self._conn.execute(
                        "INSERT OR IGNORE INTO news_item VALUES (?,?,?,?,?,?,?,?,?,?)",
                        (r.news_id, r.api, r.src, r.pub_time, r.title, r.content, r.channels, new_hash, now, now),
                    )
                    existing[r.news_id] = new_hash
                    stats.new += 1
                    continue
                stats.seen += 1
                old_hash = existing[r.news_id]
                if new_hash and old_hash and new_hash != old_hash:
                    self._conn.execute(
                        "INSERT INTO news_revision SELECT news_id, content, content_hash, ? FROM news_item WHERE news_id=?",
                        (now, r.news_id),
                    )
                    stats.revised += 1
                self._conn.execute(
                    "UPDATE news_item SET last_seen_at=?, content=COALESCE(?, content),"
                    " content_hash=COALESCE(?, content_hash), channels=COALESCE(?, channels) WHERE news_id=?",
                    (now, r.content, new_hash, r.channels, r.news_id),
                )
                existing[r.news_id] = new_hash or old_hash
        return stats

    def log_fetch(self, entry: FetchLogEntry) -> None:
        with self._conn:
            self._conn.execute(
                "INSERT INTO fetch_log VALUES (?,?,?,?,?,?,?,?)",
                (entry.api, entry.src, entry.win_start, entry.win_end, entry.n_rows,
                 int(entry.saturated), entry.error, _now()),
            )

    def mark_day_done(self, api: str, src: str, day: str, n_rows: int) -> None:
        with self._conn:
            self._conn.execute(
                "INSERT OR REPLACE INTO sync_day VALUES (?,?,?,?,?)", (api, src, day, n_rows, _now())
            )

    def done_days(self, api: str, src: str) -> set[str]:
        rows = self._conn.execute("SELECT day FROM sync_day WHERE api=? AND src=?", (api, src))
        return {day for (day,) in rows}

    # ---- 查询 ----

    def query(
        self,
        api: str,
        start_date: str,
        end_date: str,
        src: str | None = None,
        keyword: str | None = None,
    ) -> pd.DataFrame:
        """按北京时间日期区间（YYYYMMDD，含两端）查询，可按来源、标题关键词过滤。"""
        sql = "SELECT i.* FROM news_item i"
        params: list = []
        if keyword:
            if len(keyword) < 3:
                raise ValueError("keyword 至少 3 个字（trigram 全文索引的限制）")
            sql += " JOIN news_fts f ON f.rowid = i.rowid AND news_fts MATCH ?"
            params.append(f'"{keyword}"')
        sql += " WHERE i.api=? AND i.pub_time BETWEEN ? AND ?"
        params += [api, f"{_dash(start_date)} 00:00:00", f"{_dash(end_date)} 23:59:59"]
        if src:
            sql += " AND i.src=?"
            params.append(src)
        return pd.read_sql_query(sql + " ORDER BY i.pub_time DESC", self._conn, params=params)

    def close(self) -> None:
        self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def _existing_hashes(self, ids: list[str]) -> dict[str, str | None]:
        found: dict[str, str | None] = {}
        for i in range(0, len(ids), 900):  # 分批，避免超出 SQLite 参数个数上限
            chunk = ids[i:i + 900]
            marks = ",".join("?" * len(chunk))
            found.update(self._conn.execute(
                f"SELECT news_id, content_hash FROM news_item WHERE news_id IN ({marks})", chunk
            ))
        return found


def _dash(yyyymmdd: str) -> str:
    return f"{yyyymmdd[:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:]}"
