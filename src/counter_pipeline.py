"""ODS→DWD 转换编排与命令行入口。

一次运行的顺序：

1. 打开存储，先做进程恢复（遗留 running → aborted）；
2. 加载输入与有效配置，计算输入/配置哈希；
3. 解析、按 (device, counter) 分区、同键裁决、建节点、差分成区间事实；
4. 用分区输入指纹决定受影响范围；
5. **单个事务**完成所有受影响分区的旧邻接撤销与新邻接建立，随后提交运行。
   事务中途任何异常都整体回滚并把运行登记为 aborted，外部读者永远只看到
   上一次已提交版本，绝不会读到半撤销半新增的事实层。
"""

from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import dataclass

from .counter_model import (
    adjudicate,
    build_nodes,
    build_partition_facts,
    config_fingerprint,
    effective_config,
    partition_fingerprint,
    parse_observations,
)
from .counter_reader import load
from .counter_store import COMMITTED, CounterStore


@dataclass
class RunReport:
    run_id: str
    status: str
    config_version: str
    changed_partitions: list
    unchanged_partitions: list
    recovered_runs: list
    fact_counts: dict
    verdict_counts: dict


def _sha256(path: str) -> str:
    with open(path, "rb") as stream:
        return hashlib.sha256(stream.read()).hexdigest()


def _group(observations):
    out: dict = {}
    for obs in observations:
        out.setdefault((obs.device, obs.counter), []).append(obs)
    return out


def build_run(
    samples_path: str,
    counters_path: str,
    db_path: str,
    rate_overrides: dict | None = None,
    crash_before_commit: bool = False,
) -> RunReport:
    """执行一次完整转换；crash_before_commit 用于验证原子性与进程恢复。"""
    # 1) 无副作用的纯计算先行：输入/配置错误在打开存储之前抛出，不留残骸。
    counters_doc = load(counters_path)
    rows = load(samples_path)
    cfg = effective_config(counters_doc, rate_overrides)
    config_version = config_fingerprint(cfg)
    origin_iso = counters_doc["time_origin"]
    tz_name = counters_doc.get("daily_timezone")
    observations, rejects = parse_observations(rows, cfg)

    planned = []
    for (device, counter), part_obs in sorted(_group(observations).items()):
        winners, barriers, verdicts = adjudicate(part_obs)
        nodes_map = build_nodes(winners, barriers)
        facts = build_partition_facts(
            nodes_map[(device, counter)], cfg[counter],
            config_version, origin_iso, tz_name,
        )
        fp = partition_fingerprint(part_obs, config_version)
        planned.append((device, counter, part_obs, fp, facts, verdicts))

    # 2) 存储段全程受 try/finally 保护，任何异常都登记 aborted 并关闭连接。
    store = CounterStore(db_path)
    try:
        recovered = store.recover_stale_runs()
        run_id = store.start_run(
            {
                "config_version": config_version,
                "samples_sha256": _sha256(samples_path),
                "counters_sha256": _sha256(counters_path),
                "source_samples": samples_path,
                "source_counters": counters_path,
                "rate_overrides": json.dumps(rate_overrides or {}, sort_keys=True),
                "origin_iso": origin_iso,
                "daily_timezone": tz_name,
            }
        )
        previous_fingerprints = store.current_fingerprints()

        changed: list = []
        unchanged: list = []
        try:
            with store.transaction():
                # 先写坏行拒收留痕（不属于任何分区，挂在 run 上）。
                if rejects:
                    store.conn.executemany(
                        """INSERT INTO sample_verdicts(run_id, device, counter, sample_id,
                               revision, event_seconds, boot_id, raw, verdict, reason,
                               superseded_revisions, duplicate_count)
                           VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                        [
                            (
                                run_id, v.device, v.counter, v.sample_id, v.revision,
                                v.event_seconds, v.boot_id, v.raw, v.verdict, v.reason,
                                "|".join(map(str, v.superseded_revisions)),
                                v.duplicate_count,
                            )
                            for v in rejects
                        ],
                    )

                for device, counter, part_obs, fp, facts, verdicts in planned:
                    if previous_fingerprints.get((device, counter)) == fp:
                        store.register_unchanged_partition(
                            run_id, device, counter, fp, verdicts
                        )
                        unchanged.append((device, counter))
                    else:
                        store.replace_partition(
                            run_id, device, counter, part_obs, config_version, facts, verdicts
                        )
                        changed.append((device, counter))

                if crash_before_commit:
                    raise RuntimeError("注入崩溃：事务提交前")
            store.commit_run(run_id)
            status = COMMITTED
        except BaseException:
            # 事务已在 context manager 中整体回滚；仅登记运行结局后向上抛。
            store.abort_run(run_id)
            raise

        return RunReport(
            run_id=run_id,
            status=status,
            config_version=config_version,
            changed_partitions=changed,
            unchanged_partitions=unchanged,
            recovered_runs=recovered,
            fact_counts=store.count_current_by_status(),
            verdict_counts=_verdict_counts(store, run_id),
        )
    finally:
        store.close()


def _verdict_counts(store: CounterStore, run_id: str) -> dict:
    rows = store.conn.execute(
        "SELECT verdict, COUNT(*) AS n FROM sample_verdicts WHERE run_id = ? GROUP BY verdict",
        (run_id,),
    ).fetchall()
    return {row["verdict"]: row["n"] for row in rows}


def main(argv=None):
    parser = argparse.ArgumentParser(description="ODS→DWD 计数器纪元差分转换")
    sub = parser.add_subparsers(dest="command", required=True)

    p_run = sub.add_parser("run", help="执行一次转换")
    p_run.add_argument("--samples", required=True)
    p_run.add_argument("--counters", required=True)
    p_run.add_argument("--db", required=True)
    p_run.add_argument(
        "--rate-override",
        action="append",
        default=[],
        help="格式 counter=每秒速率，可重复；覆盖会产生新配置版本",
    )
    p_run.add_argument("--crash-before-commit", action="store_true")

    p_facts = sub.add_parser("facts", help="查看当前事实")
    p_facts.add_argument("--db", required=True)
    p_facts.add_argument("--run-id")

    p_daily = sub.add_parser("daily", help="当前事实的按日汇总")
    p_daily.add_argument("--db", required=True)

    p_runs = sub.add_parser("runs", help="运行注册表")
    p_runs.add_argument("--db", required=True)

    args = parser.parse_args(argv)

    if args.command == "run":
        overrides = {}
        for item in args.rate_override:
            name, value = item.split("=", 1)
            overrides[name.strip()] = float(value)
        report = build_run(
            args.samples,
            args.counters,
            args.db,
            rate_overrides=overrides,
            crash_before_commit=args.crash_before_commit,
        )
        print(json.dumps(report.__dict__, ensure_ascii=False, indent=2, default=list))
        return

    with CounterStore(args.db) as store:
        if args.command == "runs":
            print(json.dumps(store.list_runs(), ensure_ascii=False, indent=2))
        elif args.command == "facts":
            data = store.facts_of_run(args.run_id) if args.run_id else store.current_facts()
            print(json.dumps(data, ensure_ascii=False, indent=2))
        elif args.command == "daily":
            print(json.dumps(store.daily_summary_current(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
