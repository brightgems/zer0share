#!/usr/bin/env python3
"""
Tushare 全量数据下载 (新API: jiaoch.top)
重点下载因子清洗所需的表:
1. stock_basic — 股票列表 (含ST/退市标记)
2. stock_st — ST历史标记
3. stk_limit — 涨跌停价 (精确识别涨跌停日)
4. suspend_d — 停牌历史
5. daily_basic — 每日市值/估值 (全量)
6. index_weight — 指数成分股权重 (CSI300/500/1000)
7. index_daily — 指数日线
8. trade_cal — 交易日历
"""
import tushare as ts
import pandas as pd
import os, time
from datetime import datetime, timedelta

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
    print(f"  Saved: {path} ({len(df)} rows, {os.path.getsize(path)/1e6:.1f} MB)")

def download_stock_basic():
    """1. 股票列表 (含ST/退市)"""
    print("\n[1/8] stock_basic...")
    df = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_status,list_date,delist_date')
    # 也拉退市的
    df_d = pro.stock_basic(exchange='', list_status='D', fields='ts_code,symbol,name,area,industry,list_status,list_date,delist_date')
    df = pd.concat([df, df_d], ignore_index=True)
    save_parquet(df, 'stock_basic')
    return df

def download_stock_st():
    """2. ST历史标记"""
    print("\n[2/8] stock_st (ST history)...")
    # 按日期分批下载
    all_dfs = []
    trade_dates = pd.bdate_range('2020-01-01', '2026-09-30')
    for dt in trade_dates:
        d = dt.strftime('%Y%m%d')
        try:
            df = pro.stock_st(trade_date=d)
            if df is not None and len(df) > 0:
                all_dfs.append(df)
        except:
            pass
    if all_dfs:
        df = pd.concat(all_dfs, ignore_index=True)
        save_parquet(df, 'stock_st')
        return df
    print("  No ST data")
    return None

def download_stk_limit():
    """3. 涨跌停价"""
    print("\n[3/8] stk_limit (涨跌停)...")
    all_dfs = []
    trade_dates = pd.bdate_range('2020-01-01', '2026-09-30')
    batch = []
    for dt in trade_dates:
        d = dt.strftime('%Y%m%d')
        try:
            df = pro.stk_limit(trade_date=d)
            if df is not None and len(df) > 0:
                batch.append(df)
            if len(batch) >= 50:
                all_dfs.extend(batch)
                batch = []
                print(f"  ...{d} ({sum(len(x) for x in all_dfs)} rows)")
        except Exception as e:
            print(f"  Error {d}: {str(e)[:50]}")
    if batch:
        all_dfs.extend(batch)
    if all_dfs:
        df = pd.concat(all_dfs, ignore_index=True)
        save_parquet(df, 'stk_limit')
        return df
    print("  No stk_limit data")
    return None

def download_suspend_d():
    """4. 停牌历史"""
    print("\n[4/8] suspend_d (停牌)...")
    all_dfs = []
    trade_dates = pd.bdate_range('2020-01-01', '2026-09-30')
    for dt in trade_dates[::10]:  # 每10天采样一次减少API调用
        d = dt.strftime('%Y%m%d')
        try:
            df = pro.suspend_d(trade_date=d)
            if df is not None and len(df) > 0:
                all_dfs.append(df)
        except:
            pass
    if all_dfs:
        df = pd.concat(all_dfs, ignore_index=True)
        save_parquet(df, 'suspend_d')
        return df
    print("  No suspend data")
    return None

def download_daily_basic():
    """5. 每日市值/估值 (全量)"""
    print("\n[5/8] daily_basic (市值)...")
    all_dfs = []
    trade_dates = pd.bdate_range('2020-01-01', '2026-09-30')
    for i, dt in enumerate(trade_dates):
        d = dt.strftime('%Y%m%d')
        try:
            df = pro.daily_basic(trade_date=d)
            if df is not None and len(df) > 0:
                all_dfs.append(df)
            if (i+1) % 100 == 0:
                print(f"  ...{d} ({sum(len(x) for x in all_dfs)} rows)")
        except Exception as e:
            print(f"  Error {d}: {str(e)[:50]}")
            time.sleep(1)
    if all_dfs:
        df = pd.concat(all_dfs, ignore_index=True)
        save_parquet(df, 'daily_basic')
        return df
    print("  No daily_basic data")
    return None

def download_index_weight():
    """6. 指数成分股权重"""
    print("\n[6/8] index_weight (成分股)...")
    indices = ['000300.SH', '000905.SH', '000852.SH', '000985.SH']  # CSI300/500/1000/全A
    all_dfs = []
    for idx in indices:
        try:
            df = pro.index_weight(index_code=idx)
            if df is not None and len(df) > 0:
                all_dfs.append(df)
                print(f"  {idx}: {len(df)} rows")
        except Exception as e:
            print(f"  {idx}: {str(e)[:50]}")
    if all_dfs:
        df = pd.concat(all_dfs, ignore_index=True)
        save_parquet(df, 'index_weight')
        return df
    print("  No index_weight data")
    return None

def download_index_daily():
    """7. 指数日线"""
    print("\n[7/8] index_daily (指数日线)...")
    indices = ['000300.SH', '000905.SH', '000852.SH', '000985.SH', '399006.SZ']
    all_dfs = []
    for idx in indices:
        try:
            df = pro.index_daily(ts_code=idx, start_date=START_DATE, end_date=END_DATE)
            if df is not None and len(df) > 0:
                all_dfs.append(df)
                print(f"  {idx}: {len(df)} rows")
        except Exception as e:
            print(f"  {idx}: {str(e)[:50]}")
    if all_dfs:
        df = pd.concat(all_dfs, ignore_index=True)
        save_parquet(df, 'index_daily')
        return df
    print("  No index_daily data")
    return None

def download_trade_cal():
    """8. 交易日历"""
    print("\n[8/8] trade_cal (交易日历)...")
    df = pro.trade_cal(exchange='SSE', start_date=START_DATE, end_date=END_DATE)
    save_parquet(df, 'trade_cal')
    return df

def main():
    print("="*60)
    print(f"Tushare 全量数据下载")
    print(f"  API: {HTTP_URL}")
    print(f"  区间: {START_DATE} ~ {END_DATE}")
    print(f"  输出: {OUTPUT_DIR}")
    print("="*60)
    
    t0 = time.time()
    
    # 先下载快的小表
    download_stock_basic()
    download_trade_cal()
    download_index_weight()
    download_index_daily()
    
    # 再下载大的按日分批表
    download_stock_st()
    download_suspend_d()
    
    # 最大的表最后
    download_daily_basic()
    download_stk_limit()
    
    print(f"\n{'='*60}")
    print(f"All done. Total time: {time.time()-t0:.0f}s")
    print(f"Files in {OUTPUT_DIR}:")
    for f in sorted(os.listdir(OUTPUT_DIR)):
        path = os.path.join(OUTPUT_DIR, f)
        if os.path.isfile(path):
            print(f"  {f}: {os.path.getsize(path)/1e6:.1f} MB")

if __name__ == '__main__':
    main()
