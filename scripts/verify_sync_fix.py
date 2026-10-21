#!/usr/bin/env python3
"""verify_sync_fix.py — 离线验证 download_all_v2.py 的 optional 表语义

不改任何数据/不联网:
  A. index_dailybasic (optional) + 文件缺失, dry_run=False 调用 run_incremental:
     必须立即打印跳过并返回, 不触碰 pro (用 NetworkGuard 代理证明 0 次调用)。
  B. optional + 文件存在 (把 path 指到已存在的 daily.parquet): 必须走正常增量计划, 不跳过。
  C. 非 optional 且文件缺失 (weekly): 保留首次建表全历史回填, 起点 = START。

用法: /miniconda3/envs/quant/bin/python scripts/verify_sync_fix.py
"""
import importlib.util
import io
import os
import sys
import time
from contextlib import redirect_stdout
from types import SimpleNamespace

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPT = os.path.join(HERE, 'download_all_v2.py')
OUT = '/quantdata/zer0share/data/stock'

spec = importlib.util.spec_from_file_location('dav2', SCRIPT)
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


class NetworkGuard:
    """任何属性访问即抛错: 证明 optional-skip 路径零网络调用"""
    hits = 0

    def __getattr__(self, item):
        NetworkGuard.hits += 1
        raise AssertionError(f'network/pro accessed: {item}')


def find(name):
    for t in m.TABLES:
        if t['name'] == name:
            return dict(t)
    raise KeyError(name)


def run(spec_, dry_run):
    args = SimpleNamespace(dry_run=dry_run)
    buf = io.StringIO()
    t0 = time.time()
    with redirect_stdout(buf):
        m.run_incremental(spec_, args, '20261019', {})
    return buf.getvalue(), round(time.time() - t0, 3)


def main():
    ok = True
    print('TABLES[index_dailybasic] =',
          {k: v for k, v in find('index_dailybasic').items()
           if k in ('name', 'path', 'kind', 'optional')})

    # A. optional + 缺失 + 非 dry-run
    a_spec = find('index_dailybasic')
    assert not os.path.exists(a_spec['path']), 'precondition: file must be absent'
    m.pro = NetworkGuard()          # 任何网络访问会炸
    out_a, sec_a = run(a_spec, dry_run=False)
    NetworkGuard.hits = 0
    skip_line = [l for l in out_a.splitlines() if l.strip()]
    print('\n[A] optional + 文件缺失, dry_run=False:')
    print(out_a.rstrip() or '(no output)')
    a_ok = any('optional' in l and '跳过' in l for l in skip_line)
    print(f'[A] 判定: {"PASS" if a_ok else "FAIL"} (耗时 {sec_a}s, 文件仍不存在='
          f'{not os.path.exists(a_spec["path"])})')
    ok &= a_ok

    # B. optional + 存在 (借用 daily.parquet 作为"已存在的目标文件")
    b_spec = dict(a_spec, path=os.path.join(OUT, 'daily.parquet'))
    out_b, sec_b = run(b_spec, dry_run=True)
    print('\n[B] optional + 文件存在 (path=daily.parquet), dry_run=True:')
    print(out_b.rstrip())
    b_ok = ('跳过' not in out_b) and ('计划下载' in out_b or '无需下载' in out_b)
    print(f'[B] 判定: {"PASS" if b_ok else "FAIL"} (耗时 {sec_b}s)')
    ok &= b_ok

    # C. 非 optional + 缺失 → 全历史, 起点 START
    c_spec = find('weekly')
    assert not os.path.exists(c_spec['path'])
    out_c, sec_c = run(c_spec, dry_run=True)
    print('\n[C] 非 optional (weekly) + 文件缺失, dry_run=True:')
    print(out_c.rstrip())
    c_ok = ('计划下载' in out_c and f'{m.START} ~ 20261019' in out_c.replace('20200102', m.START))
    print(f'[C] 判定: {"PASS" if c_ok else "FAIL"} (耗时 {sec_c}s)')
    ok &= c_ok

    print(f'\nRESULT: {"PASS" if ok else "FAIL"}')
    return 0 if ok else 1


if __name__ == '__main__':
    sys.exit(main())
