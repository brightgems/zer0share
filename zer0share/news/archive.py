"""原始返回归档：接口返回什么就原样存什么，只追加不修改。主库出问题时可从这里重建。"""

import gzip
import json
from datetime import datetime
from pathlib import Path

import pandas as pd


class RawArchive:
    def __init__(self, root: str | Path):
        self.root = Path(root)

    def append(self, api: str, src: str, day: str, df: pd.DataFrame, win_start: str, win_end: str) -> None:
        """追加一次请求的全部行到 {root}/{api}/{src}/{day}.jsonl.gz，每行带请求时间窗和抓取时间。"""
        if df.empty:
            return
        path = self.root / api / src / f"{day}.jsonl.gz"
        path.parent.mkdir(parents=True, exist_ok=True)
        meta = {"fetched_at": datetime.now().isoformat(timespec="seconds"), "win_start": win_start, "win_end": win_end}
        with gzip.open(path, "at", encoding="utf-8") as f:
            for row in df.to_dict(orient="records"):
                f.write(json.dumps({**meta, "row": row}, ensure_ascii=False, default=str) + "\n")
