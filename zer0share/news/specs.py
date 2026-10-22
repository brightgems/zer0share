"""Tushare 新闻接口的描述：字段名、来源列表、单次返回上限。"""

from dataclasses import dataclass


@dataclass(frozen=True)
class NewsApiSpec:
    name: str                  # Tushare 接口名
    time_field: str            # 发布时间字段
    sources: tuple[str, ...]   # 可选的 src 参数
    row_cap: int               # 单次返回上限；返回条数达到它就认为被截断，需要切窗
    fields: str                # 默认请求的字段
    content_fields: str        # 带正文时请求的字段

    def request_fields(self, with_content: bool) -> str:
        return self.content_fields if with_content else self.fields


# 长篇通讯。正文是整页 HTML，体积大，默认不取。
# 文档写单次上限 400，实测单来源一天可返回 600+，按 400 判断截断更保守（最多多切几次窗）。
MAJOR_NEWS = NewsApiSpec(
    name="major_news",
    time_field="pub_time",
    sources=("新华网", "凤凰财经", "同花顺", "新浪财经", "华尔街见闻", "中证网", "财新网", "第一财经", "财联社"),
    row_cap=400,
    fields="title,pub_time,src",
    content_fields="title,content,pub_time,src",
)

# 快讯。很多条目没有标题、内容就是正文，所以正文始终要取。
NEWS = NewsApiSpec(
    name="news",
    time_field="datetime",
    sources=("sina", "wallstreetcn", "10jqka", "eastmoney", "yuncaijing", "fenghuang", "jinrongjie", "cls", "yicai"),
    row_cap=1500,
    fields="datetime,title,content,channels",
    content_fields="datetime,title,content,channels",
)

NEWS_APIS: dict[str, NewsApiSpec] = {spec.name: spec for spec in (MAJOR_NEWS, NEWS)}
