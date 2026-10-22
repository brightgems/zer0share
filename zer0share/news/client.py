"""Tushare HTTP 客户端：新闻接口可能用另一个账号和地址，所以不复用主 fetcher 的 SDK 实例。"""

import time

import pandas as pd
import requests
from loguru import logger


class TushareApiError(RuntimeError):
    """接口返回业务错误（如无权限）。这类错误重试无用，直接抛出。"""


class TushareHttpClient:
    def __init__(self, token: str, url: str, sleep_seconds: float = 0.5, retries: int = 3):
        self._token = token
        self._url = url
        self._sleep = sleep_seconds    # 每次请求后的间隔，避免触发限频
        self._retries = retries
        self._http = requests.Session()  # 与 Tushare SDK 一致用 requests，代理等环境设置行为相同

    def query(self, api_name: str, params: dict, fields: str) -> pd.DataFrame:
        """调用一次接口，返回 DataFrame。网络错误按指数退避重试。"""
        payload = {"api_name": api_name, "token": self._token, "params": params, "fields": fields}
        for attempt in range(1, self._retries + 1):
            try:
                body = self._http.post(self._url, json=payload, timeout=60).json()
                break
            except (requests.RequestException, ValueError) as e:
                if attempt == self._retries:
                    raise
                logger.warning(f"{api_name} 请求失败（第 {attempt} 次）：{e}")
                time.sleep(2 ** attempt)
        time.sleep(self._sleep)
        if body.get("code") != 0:
            raise TushareApiError(f"{api_name}: {body.get('msg')}")
        data = body["data"]
        return pd.DataFrame(data["items"], columns=data["fields"])

    def close(self) -> None:
        self._http.close()
