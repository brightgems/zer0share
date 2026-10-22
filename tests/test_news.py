"""新闻子包测试：SQLite 主库、切窗同步、本地查询、配置。"""

from datetime import datetime

import pandas as pd
import pytest

from zer0share.api import LocalPro
from zer0share.config import load_config
from zer0share.news import NEWS_DB, NewsApiSpec, NewsRecord, NewsStore, NewsSyncer, RawArchive, TushareApiError
from tests.test_config import VALID_TOML

SPEC = NewsApiSpec(
    name="major_news", time_field="pub_time", sources=("A", "B"), row_cap=3,
    fields="title,pub_time,src", content_fields="title,content,pub_time,src",
)


def rec(title, pub_time="2025-03-25 09:30:00", content=None, src="A"):
    return NewsRecord(api="major_news", src=src, pub_time=pub_time, title=title, content=content)


class FakeClient:
    """模拟 Tushare：按时间窗和来源过滤，按时间倒序返回，最多 row_cap 条。"""

    def __init__(self, items, fail_windows=(), api_error=False):
        self.items = items                 # [(src, pub_time, title)]
        self.fail_windows = set(fail_windows)
        self.api_error = api_error
        self.calls = []

    def query(self, api_name, params, fields):
        self.calls.append((params["src"], params["start_date"], params["end_date"]))
        if self.api_error:
            raise TushareApiError("没有接口访问权限")
        if params["start_date"][:10] in self.fail_windows:
            raise ConnectionError("timeout")
        rows = sorted(
            (it for it in self.items if it[0] == params["src"] and params["start_date"] <= it[1] <= params["end_date"]),
            key=lambda it: it[1], reverse=True,
        )[:SPEC.row_cap]
        return pd.DataFrame([{"title": t, "pub_time": p, "src": s} for s, p, t in rows])


@pytest.fixture
def store(tmp_path):
    with NewsStore(tmp_path / "news.db") as s:
        yield s


class TestStore:
    def test_duplicates_are_idempotent(self, store):
        assert store.upsert([rec("恒瑞医药:新药获批")]).new == 1
        stats = store.upsert([rec("恒瑞医药:新药获批")])
        assert (stats.new, stats.seen) == (0, 1)
        assert len(store.query("major_news", "20250325", "20250325")) == 1

    def test_content_change_recorded_as_revision(self, store):
        store.upsert([rec("标题", content="旧正文")])
        assert store.upsert([rec("标题", content="新正文")]).revised == 1
        assert store.query("major_news", "20250325", "20250325")["content"].tolist() == ["新正文"]
        old = store._conn.execute("SELECT content FROM news_revision").fetchall()
        assert old == [("旧正文",)]

    def test_filling_missing_content_is_not_revision(self, store):
        store.upsert([rec("标题")])
        assert store.upsert([rec("标题", content="正文")]).revised == 0

    def test_query_by_date_src_and_keyword(self, store):
        store.upsert([
            rec("恒瑞医药与默沙东达成合作"),
            rec("上海莱士拟收购股权", src="B"),
            rec("次日新闻", pub_time="2025-03-26 08:00:00"),
        ])
        assert len(store.query("major_news", "20250325", "20250325")) == 2
        assert store.query("major_news", "20250325", "20250326", src="B")["title"].tolist() == ["上海莱士拟收购股权"]
        assert store.query("major_news", "20250325", "20250326", keyword="默沙东")["title"].tolist() == ["恒瑞医药与默沙东达成合作"]
        with pytest.raises(ValueError):
            store.query("major_news", "20250325", "20250325", keyword="恒瑞")

    def test_untitled_flash_news_dedup_by_content(self, store):
        store.upsert([rec("", content="快讯A"), rec("", content="快讯B")])
        assert len(store.query("major_news", "20250325", "20250325")) == 2


class TestSyncer:
    ITEMS = [("A", f"2025-03-25 {h:02d}:00:00", f"新闻{h}") for h in range(10)]  # 10 条 > row_cap=3

    def test_splits_truncated_windows_until_complete(self, store, tmp_path):
        summary = NewsSyncer(FakeClient(self.ITEMS), store, RawArchive(tmp_path / "raw")).sync(
            SPEC, "20250325", "20250325", sources=["A"]
        )
        assert (summary.new, summary.days, summary.saturated_windows) == (10, 1, 0)
        assert (tmp_path / "raw" / "major_news" / "A" / "20250325.jsonl.gz").exists()

    def test_done_days_are_skipped(self, store):
        NewsSyncer(FakeClient(self.ITEMS), store).sync(SPEC, "20250325", "20250325", sources=["A"])
        client = FakeClient(self.ITEMS)
        NewsSyncer(client, store).sync(SPEC, "20250325", "20250325", sources=["A"])
        assert client.calls == []
        NewsSyncer(client, store).sync(SPEC, "20250325", "20250325", sources=["A"], refresh=True)
        assert client.calls

    def test_failed_window_leaves_day_unfinished(self, store):
        summary = NewsSyncer(FakeClient(self.ITEMS, fail_windows={"2025-03-25"}), store).sync(
            SPEC, "20250325", "20250325", sources=["A"]
        )
        assert (summary.failed_windows, summary.days) == (1, 0)
        assert store.done_days("major_news", "A") == set()

    def test_permission_error_is_raised(self, store):
        with pytest.raises(TushareApiError):
            NewsSyncer(FakeClient([], api_error=True), store).sync(SPEC, "20250325", "20250325", sources=["A"])

    def test_today_is_not_marked_done(self, store):
        today = datetime.now().strftime("%Y%m%d")
        NewsSyncer(FakeClient([]), store).sync(SPEC, today, today, sources=["A"])
        assert store.done_days("major_news", "A") == set()


def test_local_pro_major_news(tmp_path):
    with NewsStore(tmp_path.joinpath(*NEWS_DB)) as s:
        s.upsert([rec("恒瑞医药:新药获批")])
    df = LocalPro(tmp_path).major_news(start_date="20250325", end_date="20250325")
    assert df.columns.tolist() == ["title", "pub_time", "src", "news_id"]
    assert df["title"].tolist() == ["恒瑞医药:新药获批"]


def test_local_pro_news_missing_db(tmp_path):
    with pytest.raises(FileNotFoundError):
        LocalPro(tmp_path).news(start_date="20250325", end_date="20250325")


class TestNewsConfig:
    def test_defaults_to_main_token(self, tmp_path):
        cfg_file = tmp_path / "settings.toml"
        cfg_file.write_text(VALID_TOML, encoding="utf-8")
        news = load_config(cfg_file).news
        assert (news.token, news.http_url, news.with_content) == ("test_token", "http://api.tushare.pro", False)

    def test_separate_news_account(self, tmp_path):
        cfg_file = tmp_path / "settings.toml"
        cfg_file.write_text(VALID_TOML + '\n[tushare_news]\ntoken = "news_token"\nwith_content = true\n', encoding="utf-8")
        news = load_config(cfg_file).news
        assert (news.token, news.with_content) == ("news_token", True)
