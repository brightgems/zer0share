#!/usr/bin/env python3
"""
下载融资融券、沪深港通持股、龙虎榜数据
Tushare API: jiaoch.top
"""
import tushare as ts
import pandas as pd
import os, time
from datetime import datetime

TOKEN = 'c70e826037bee3efad6c526058e54ac53c4cd62c4c0bf9615c065af4'
HTTP_URL = "https://jiaoch.top/"
OUTPUT_DIR = '/quantdata/zer0share/data/stock'
os.makedirs(OUTPUT_DIR, exist_ok=True)

pro = ts.pro_api(TOKEN)
pro._DataApi__http_url = HTTP_URL

START_DATE = '20200101'
END_DATE = '20260930'

def save_parquet(df, name):
    path = os.path.join(OUTPUT_DIR, f'{name}.parquet')
    df.to_parquet(path, index=False)
    print(f"  Saved: {path} ({len(df):,} rows, {os.path.getsize(path)/1e6:.1f} MB)")

def download_margin():
    """融资融券交易明细 (按日期分批)"""
    print("\n[1/3] margin_detail (融资融券明细)...")
    all_dfs = []
    trade_dates = pd.bdate_range('2020-01-01', '2026-09-30')
    for i, dt in enumerate(trade_dates):
        d = dt.strftime('%Y%m%d')
        try:
            df = pro.margin_detail(trade_date=d)
            if df is not None and len(df) > 0:
                all_dfs.append(df)
            if (i+1) % 100 == 0:
                total = sum(len(x) for x in all_dfs)
                print(f"  ...{d} ({total:,} rows)")
        except Exception as e:
            print(f"  Error {d}: {str(e)[:60]}")
            time.sleep(1)
    if all_dfs:
        df = pd.concat(all_dfs, ignore_index=True)
        save_parquet(df, 'margin_detail')
        return df
    print("  No margin data")
    return None

def download_hk_hold():
    """沪深港通持股明细 (按日期分批)"""
    print("\n[2/3] hk_hold (沪深港通持股)...")
    all_dfs = []
    trade_dates = pd.bdate_range('2020-01-01', '2026-09-30')
    for i, dt in enumerate(trade_dates):
        d = dt.strftime('%Y%m%d')
        try:
            # 北向持股
            df = pro.hk_hold(trade_date=d)
            if df is not None and len(df) > 0:
                all_dfs.append(df)
            if (i+1) % 100 == 0:
                total = sum(len(x) for x in all_dfs)
                print(f"  ...{d} ({total:,} rows)")
        except Exception as e:
            print(f"  Error {d}: {str(e)[:60]}")
            time.sleep(1)
    if all_dfs:
        df = pd.concat(all_dfs, ignore_index=True)
        save_parquet(df, 'hk_hold')
        return df
    print("  No hk_hold data")
    return None

def download_top_list():
    """龙虎榜每日明细 (按日期分批)"""
    print("\n[3/3] top_list (龙虎榜)...")
    all_dfs = []
    trade_dates = pd.bdate_range('2020-01-01', '2026-09-30')
    for i, dt in enumerate(trade_dates):
        d = dt.strftime('%Y%m%d')
        try:
            df = pro.top_list(trade_date=d)
            if df is not None and len(df) > 0:
                all_dfs.append(df)
            if (i+1) % 100 == 0:
                total = sum(len(x) for x in all_dfs)
                print(f"  ...{d} ({total:,} rows)")
        except Exception as e:
            print(f"  Error {d}: {str(e)[:60]}")
            time.sleep(1)
    if all_dfs:
        df = pd.concat(all_dfs, ignore_index=True)
        save_parquet(df, 'top_list')
        return df
    print("  No top_list data")
    return None

def main():
    print("="*60)
    print("融资融券 + 沪深港通 + 龙虎榜 下载")
    print(f"  API: {HTTP_URL}")
    print(f"  区间: {START_DATE} ~ {END_DATE}")
    print(f"  输出: {OUTPUT_DIR}")
    print("="*60)

    t0 = time.time()

    download_margin()
    download_hk_hold()
    download_top_list()

    print(f"\n{'='*60}")
    print(f"All done. Total time: {time.time()-t0:.0f}s")
    print(f"Files in {OUTPUT_DIR}:")
    for f in sorted(os.listdir(OUTPUT_DIR)):
        path = os.path.join(OUTPUT_DIR, f)
        if os.path.isfile(path) and f.endswith('.parquet'):
            print(f"  {f}: {os.path.getsize(path)/1e6:.1f} MB")

if __name__ == '__main__':
    main()
