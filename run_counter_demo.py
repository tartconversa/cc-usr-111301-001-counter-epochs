#!/usr/bin/env python3
"""全场景验证演示（以 ``python -I run_counter_demo.py`` 运行）。

演示数据全部写入临时目录，DWD 库落在 .workspace-output/demo-* 下，
fixtures 只以路径参数传入；结束后保留最后一个库供手动查验。

覆盖：8 位回绕典例、上界放宽后的多圈歧义、迟到中间点的邻接替换、
重复行/平级冲突隔离、重启跨纪元、跨日未分配、迟到高 revision 的
分区级增量重算、提交前崩溃与进程恢复。每步打印区间键与各状态数量。
"""

import json
import os
import sys
import tempfile
from pathlib import Path

ROOT = os.path.dirname(os.path.abspath(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from src.counter_model import ASSIGNED  # noqa: E402
from src.counter_pipeline import build_run  # noqa: E402
from src.counter_store import ABORTED, COMMITTED, CounterStore  # noqa: E402

HEADER = "device,counter,sample_id,revision,event_seconds,boot_id,raw\n"
ORIGIN = "2026-11-10T23:59:59+08:00"
TZ = "Asia/Shanghai"


def r(device, sample, revision, t, boot, raw):
    return f"{device},bytes,{sample},{revision},{t},{boot},{raw}\n"


def write_inputs(workdir: Path, name: str, rows, rate=20.0):
    workdir.mkdir(parents=True, exist_ok=True)
    samples = workdir / f"{name}_samples.csv"
    counters = workdir / f"{name}_counters.json"
    samples.write_text(HEADER + "".join(rows), encoding="utf-8")
    counters.write_text(
        json.dumps(
            {
                "counters": {"bytes": {"bits": 8, "unit": "byte",
                                       "maximum_rate_per_second": rate}},
                "time_origin": ORIGIN,
                "daily_timezone": TZ,
            }
        ),
        encoding="utf-8",
    )
    return samples, counters


def short(fact_key: str) -> str:
    return fact_key[:10]


def show_facts(store: CounterStore, title: str):
    facts = store.current_facts()
    counts = store.count_current_by_status()
    print(f"  [{title}] 当前事实 {len(facts)} 条 | known={counts['known']} unknown={counts['unknown']}")
    for f in facts:
        interval = f"({','.join(f['left_samples'].split('|'))} -> {','.join(f['right_samples'].split('|'))})"
        span = f"t={f['start_seconds']}..{f['end_seconds']}"
        if f["status"] == "known":
            value = f"delta={f['delta']}"
        else:
            value = f"UNKNOWN({f['reason']})"
        assign = f["assignment_day"] if f["assignment_state"] == ASSIGNED else "UNASSIGNED(跨日)"
        print(
            f"    key={short(f['fact_key'])} {interval:<14} {span:<12} "
            f"{value:<28} 归属={assign or '-'} boot={f['left_boot_id']}->{f['right_boot_id']}"
        )
    return facts


def heading(text: str):
    print("\n" + "=" * 78)
    print(text)
    print("=" * 78)


def snapshot(store: CounterStore):
    return {
        "n": len(store.current_facts()),
        "counts": store.count_current_by_status(),
        "keys": sorted((f["fact_key"], f["is_current"]) for f in
                       store.conn.execute("SELECT fact_key, is_current FROM interval_facts")),
    }


def main():
    out_root = Path(ROOT) / ".workspace-output"
    out_root.mkdir(exist_ok=True)
    tmp = tempfile.TemporaryDirectory(prefix="ods-input-", dir=out_root)
    workdir = Path(tmp.name)
    db_dir = tempfile.mkdtemp(prefix="demo-dwd-", dir=out_root)
    print(f"输入临时目录: {workdir}\nDWD 库目录 : {db_dir}")

    # ------------------------------------------------------------------ #
    heading("场景 0/1：8 位回绕典例 -> 上界放宽多圈歧义 -> 迟到中间点邻接替换")
    db = str(Path(db_dir) / "s1.db")
    v1 = [r("d1", "s0", 1, 0, "boot1", 250), r("d1", "s1", 1, 1, "boot1", 5)]
    samples, counters = write_inputs(workdir, "s1", v1)

    rep1 = build_run(str(samples), str(counters), db)
    print(f"  运行1 {rep1.run_id} status={rep1.status} 受影响分区={rep1.changed_partitions}")
    store = CounterStore(db)
    before = show_facts(store, "上界 20/秒：raw 250->5")
    before_keys = {f["fact_key"] for f in before}

    rep2 = build_run(str(samples), str(counters), db, rate_overrides={"bytes": 300.0})
    print(f"\n  运行2 {rep2.run_id} 配置版本变化 -> 受影响分区={rep2.changed_partitions}")
    show_facts(store, "上界 300/秒：候选 11 与 267，多圈歧义")

    v3 = v1 + [r("d1", "s05", 1, 0.5, "boot1", 255)]
    samples3, _ = write_inputs(workdir, "s1b", v3)
    rep3 = build_run(str(samples3), str(counters), db)
    print(f"\n  运行3 {rep3.run_id} 补入 t=0.5 读 255 -> 受影响分区={rep3.changed_partitions}")
    after = show_facts(store, "邻接替换后：两个小区间替代一个大区间")
    after_keys = {f["fact_key"] for f in after}

    replaced = store.conn.execute(
        "SELECT fact_key, is_current, superseded_run_id FROM interval_facts "
        "WHERE fact_key IN (%s)" % ",".join("?" * len(before_keys)),
        tuple(before_keys),
    ).fetchall()
    print("\n  邻接替换前后区间键对照：")
    print(f"    撤销的旧大区间: {[short(k) for k in sorted(before_keys)]} "
          f"-> is_current 全部为 {sorted({row['is_current'] for row in replaced})}")
    print(f"    新建小区间    : {[short(k) for k in sorted(after_keys - before_keys)]}")
    overlap = before_keys & after_keys
    print(f"    新旧同时 current 的键: {overlap if overlap else '无（原子替换，无重复相加）'}")
    print(f"    增量合计: {sum(f['delta'] for f in after)}（替换前为 11，合计守恒）")
    old = store.facts_of_run(rep1.run_id)
    print(f"    旧运行 {rep1.run_id} 仍可查: {len(old)} 条事实, delta={old[0]['delta']}")
    store.close()

    # ------------------------------------------------------------------ #
    heading("场景 2：完全重复去重留痕；同 revision 平级异值 -> 采样键隔离")
    db = str(Path(db_dir) / "s2.db")
    dup = [
        r("d1", "s0", 1, 0, "boot1", 10),
        r("d1", "s0", 1, 0, "boot1", 10),
        r("d1", "s1", 1, 1, "boot1", 30),
    ]
    samples, counters = write_inputs(workdir, "s2", dup)
    rep = build_run(str(samples), str(counters), db)
    store = CounterStore(db)
    show_facts(store, "重复行去重后")
    v = {x["sample_id"]: x for x in store.verdicts_of_run(rep.run_id)}
    print(f"    s0 裁决={v['s0']['verdict']} 重复次数={v['s0']['duplicate_count']}")

    tie = dup + [r("d1", "s1", 1, 1, "boot1", 99)]
    samples2, _ = write_inputs(workdir, "s2b", tie)
    rep2 = build_run(str(samples2), str(counters), db)
    v2 = {x["sample_id"]: x for x in store.verdicts_of_run(rep2.run_id)}
    print(f"\n  补入平级异值后: s1 裁决={v2['s1']['verdict']} 原因={v2['s1']['reason']}")
    show_facts(store, "隔离屏障生效：相关邻接全部未知")
    store.close()

    # ------------------------------------------------------------------ #
    heading("场景 3：重启跨纪元（boot_id 变化的区间未知，首点不是从 0 的增量）")
    db = str(Path(db_dir) / "s3.db")
    rows = [
        r("d1", "s0", 1, 0, "boot1", 250),
        r("d1", "s1", 1, 1, "boot1", 5),
        r("d1", "s2", 1, 2, "boot2", 4),
        r("d1", "s3", 1, 3, "boot2", 8),
    ]
    samples, counters = write_inputs(workdir, "s3", rows)
    rep = build_run(str(samples), str(counters), db)
    store = CounterStore(db)
    show_facts(store, "同纪元可差分；跨 boot 边界未知")
    store.close()

    # ------------------------------------------------------------------ #
    heading("场景 4：跨日区间（已知增量也保留未分配，不按时长拆分）")
    db = str(Path(db_dir) / "s4.db")
    rows = [
        r("d1", "s0", 1, 0, "boot1", 250),
        r("d1", "s05", 1, 0.5, "boot1", 255),
        r("d1", "s1", 1, 1, "boot1", 5),
    ]
    samples, counters = write_inputs(workdir, "s4", rows)
    build_run(str(samples), str(counters), db)
    store = CounterStore(db)
    show_facts(store, "origin=23:59:59(+08)，t=1 跨入次日")
    print("  按日汇总:")
    for row in store.daily_summary_current():
        print(f"    {row['device']} {row['counter']} day={row['day']}: "
              f"known_delta={row['known_delta']} known={row['known_intervals']} "
              f"unknown={row['unknown_intervals']} reasons={row['unknown_reasons']}")
    store.close()

    # ------------------------------------------------------------------ #
    heading("场景 5：迟到高 revision 更正 -> 只重算受影响设备 counter")
    db = str(Path(db_dir) / "s5.db")
    rows = [
        r("d1", "s2", 1, 2, "boot2", 4),
        r("d1", "s3", 1, 3, "boot2", 8),
        r("d2", "s9", 1, 0, "boot9", 1),
        r("d2", "s9b", 1, 1, "boot9", 3),
    ]
    samples, counters = write_inputs(workdir, "s5", rows)
    rep1 = build_run(str(samples), str(counters), db)
    store = CounterStore(db)
    show_facts(store, "首次运行：d1/d2 两个分区")
    rows2 = rows + [r("d1", "s3", 2, 3, "boot2", 9)]
    samples2, _ = write_inputs(workdir, "s5b", rows2)
    rep2 = build_run(str(samples2), str(counters), db)
    print(f"\n  迟到 revision=2 后: changed={rep2.changed_partitions} unchanged={rep2.unchanged_partitions}")
    show_facts(store, "仅 d1.bytes 邻接被替换，d2 事实行归属首次运行")
    d2 = next(f for f in store.current_facts() if f["device"] == "d2")
    print(f"    d2 事实 run_id={d2['run_id']}（等于首次运行 {rep1.run_id}：未被触碰）")
    store.close()

    # ------------------------------------------------------------------ #
    heading("场景 6：提交前崩溃原子回滚 + 进程重启恢复遗留 running")
    db = str(Path(db_dir) / "s6.db")
    rows = [r("d1", "s0", 1, 0, "boot1", 250), r("d1", "s1", 1, 1, "boot1", 5)]
    samples, counters = write_inputs(workdir, "s6", rows)
    rep1 = build_run(str(samples), str(counters), db)
    store = CounterStore(db)
    snap_before = snapshot(store)
    store.close()

    rows2 = rows + [r("d1", "s05", 1, 0.5, "boot1", 255)]
    samples2, _ = write_inputs(workdir, "s6b", rows2)
    try:
        build_run(str(samples2), str(counters), db, crash_before_commit=True)
    except RuntimeError as exc:
        print(f"  注入崩溃: {exc}")
    store = CounterStore(db)
    snap_after = snapshot(store)
    print(f"  崩溃后事实层与崩溃前一致: {snap_after == snap_before}（无半撤销半新增）")
    print(f"  运行注册表: {[(x['run_id'], x['status']) for x in store.list_runs()]}")

    zombie = store.start_run({
        "config_version": "zombie", "samples_sha256": None, "counters_sha256": None,
        "source_samples": "s", "source_counters": "c", "rate_overrides": "{}",
        "origin_iso": ORIGIN, "daily_timezone": TZ,
    })
    store.close()
    store = CounterStore(db)  # 模拟新进程启动
    recovered = store.recover_stale_runs()
    print(f"  模拟 kill -9 遗留 running={zombie}；新进程恢复: {recovered} -> {store.run_status(zombie)}")
    rep3 = build_run(str(samples2), str(counters), db)
    print(f"  干净重跑 {rep3.run_id} status={rep3.status}")
    show_facts(store, "恢复后邻接替换正常完成")
    print(f"  最终运行注册表:")
    for x in store.list_runs():
        print(f"    {x['run_id']} {x['status']:<9} {x['started_at']}")
    store.close()

    print("\n全部场景验证完成。")
    print(f"（DWD 库保留于 {db_dir}，可用 python -I run_counter_cli.py daily --db <库> 查验）")


if __name__ == "__main__":
    main()
