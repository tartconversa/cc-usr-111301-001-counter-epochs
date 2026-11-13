"""ODS→DWD 计数器转换的契约测试。

覆盖：8 位回绕典例与多圈歧义、重复与平级冲突隔离、重启纪元、跨日归属、
迟到修订的邻接替换、进程崩溃恢复与事务原子性、首点不计增量。
"""

import json
import tempfile
import unittest
from pathlib import Path

from src.counter_model import (
    ASSIGNED,
    KNOWN,
    UNKNOWN,
    UNASSIGNED_BUCKET,
    BOOT_BOUNDARY,
    MULTI_TURN_AMBIGUOUS,
    QUARANTINE_BARRIER,
    RATE_CAP_EXCEEDED,
    SIMULTANEOUS_CONFLICT,
    QUARANTINED,
    REJECTED,
    candidate_deltas,
    resolve_interval,
    summarize_daily,
)
from src.counter_pipeline import build_run
from src.counter_store import ABORTED, COMMITTED, CounterStore

HEADER = "device,counter,sample_id,revision,event_seconds,boot_id,raw\n"
ORIGIN = "2026-11-10T23:59:59+08:00"
TZ = "Asia/Shanghai"


def write_inputs(directory: Path, rows: list[str], rate: float = 20.0, bits: int = 8):
    # 每个用例使用独立子目录，夹具文件互不覆盖。
    directory.mkdir(parents=True, exist_ok=True)
    samples = directory / "samples.csv"
    samples.write_text(HEADER + "".join(rows), encoding="utf-8")
    counters = directory / "counters.json"
    counters.write_text(
        json.dumps(
            {
                "counters": {
                    "bytes": {
                        "bits": bits,
                        "unit": "byte",
                        "maximum_rate_per_second": rate,
                    }
                },
                "time_origin": ORIGIN,
                "daily_timezone": TZ,
            }
        ),
        encoding="utf-8",
    )
    return samples, counters


def row(device, sample, revision, t, boot, raw):
    return f"{device},bytes,{sample},{revision},{t},{boot},{raw}\n"


def key_of(fact_row) -> tuple:
    return (fact_row["left_samples"], fact_row["right_samples"])


class WrapMathTests(unittest.TestCase):
    def test_8bit_wrap_canonical_example(self):
        # t=0 读 250、t=1 读 5，速率上界 20/秒：唯一候选是模回绕差 11。
        self.assertEqual(candidate_deltas(250, 5, dt=1.0, bits=8, rate=20.0), [11])
        status, delta, reason = resolve_interval(250, 5, 1.0, {
            "bits": 8, "unit": "byte", "maximum_rate_per_second": 20.0})
        self.assertEqual((status, delta, reason), (KNOWN, 11, None))
        # 绝不是 5-250 的负值，也不是其绝对值 245。
        self.assertNotIn(delta, (-245, 245))

    def test_high_rate_makes_two_candidates_unknown(self):
        # 上界 300/秒：11 与 267 都可能，必须判未知，不得猜造。
        self.assertEqual(candidate_deltas(250, 5, 1.0, 8, 300.0), [11, 267])
        status, delta, reason = resolve_interval(250, 5, 1.0, {
            "bits": 8, "unit": "byte", "maximum_rate_per_second": 300.0})
        self.assertEqual(status, UNKNOWN)
        self.assertIsNone(delta)
        self.assertEqual(reason, MULTI_TURN_AMBIGUOUS)

    def test_rising_raw_can_still_be_multiturn_ambiguous(self):
        # raw 上升也可能多圈：10 与 266 都在速率上界内。
        self.assertEqual(candidate_deltas(10, 20, 1.0, 8, 300.0), [10, 266])

    def test_cap_exceeded_has_no_candidate(self):
        candidates = candidate_deltas(250, 5, 1.0, 8, 0.5)
        self.assertEqual(candidates, [])
        status, _, reason = resolve_interval(250, 5, 1.0, {
            "bits": 8, "unit": "byte", "maximum_rate_per_second": 0.5})
        self.assertEqual((status, reason), (UNKNOWN, RATE_CAP_EXCEEDED))

    def test_no_change_with_wide_cap_is_ambiguous_not_zero(self):
        # raw 不变且上界容得下一整圈：0 与 256 都是候选，不能武断为 0。
        self.assertEqual(candidate_deltas(7, 7, 1.0, 8, 300.0), [0, 256])


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        # 输入文件与 DWD 库各自独立目录，避免用例间夹具覆盖。
        self.input_dir = Path(self._tmp.name) / "input"
        self.db = str(Path(self._tmp.name) / "dwd.db")
        self._stores = []

    def open_store(self):
        store = CounterStore(self.db)
        self._stores.append(store)
        return store

    def tearDown(self):
        for store in self._stores:
            store.close()
        self._tmp.cleanup()

    def _run(self, rows, rate=20.0, overrides=None, crash=False, name="case"):
        case_dir = self.input_dir / name
        samples, counters = write_inputs(case_dir, rows, rate=rate)
        return build_run(
            str(samples), str(counters), self.db,
            rate_overrides=overrides, crash_before_commit=crash,
        ), samples, counters

    def test_canonical_then_midpoint_replaces_adjacency(self):
        """题目典例：250→5 得 11；上界 300 时未知；插入 255 后 5+6 替代 11。"""
        rows_v1 = [row("d1", "s0", 1, 0, "boot1", 250),
                   row("d1", "s1", 1, 1, "boot1", 5)]
        report1, samples, counters = self._run(rows_v1, name="canonical_v1")
        self.assertEqual(report1.status, COMMITTED)
        store = self.open_store()
        facts1 = store.current_facts()
        self.assertEqual(len(facts1), 1)
        self.assertEqual((facts1[0]["status"], facts1[0]["delta"]), (KNOWN, 11))
        big_key = facts1[0]["fact_key"]

        # 速率上界放宽到 300：同一邻接变成多圈歧义未知。
        report2, _, _ = self._run(rows_v1, overrides={"bytes": 300.0}, name="canonical_v2")
        facts2 = store.current_facts()
        self.assertEqual(len(facts2), 1)
        self.assertEqual(facts2[0]["status"], UNKNOWN)
        self.assertEqual(facts2[0]["reason"], MULTI_TURN_AMBIGUOUS)
        self.assertNotEqual(facts2[0]["config_version"], facts1[0]["config_version"])

        # 迟到中间点 t=0.5 读 255，恢复 20/秒上界：两个小区间替代大区间。
        rows_v3 = rows_v1 + [row("d1", "s05", 1, 0.5, "boot1", 255)]
        report3, _, _ = self._run(rows_v3, name="canonical_v3")
        self.assertEqual(report3.changed_partitions, [("d1", "bytes")])
        current = store.current_facts()
        self.assertEqual(len(current), 2)
        self.assertEqual(sorted(f["delta"] for f in current), [5, 6])
        self.assertTrue(all(f["status"] == KNOWN for f in current))
        # 合计仍是 11，没有把旧大区间与新小区间同时相加。
        self.assertEqual(sum(f["delta"] for f in current), 11)

        # 旧大区间事实被标 supersede，不再 current；新旧不并存。
        old_big = store.conn.execute(
            "SELECT * FROM interval_facts WHERE fact_key = ?", (big_key,)
        ).fetchall()
        self.assertTrue(all(r["is_current"] == 0 for r in old_big))
        new_keys = {key_of(f) for f in current}
        self.assertEqual(new_keys, {("s0", "s05"), ("s05", "s1")})

        # 旧运行版本仍可查。
        old_run_facts = store.facts_of_run(report1.run_id)
        self.assertEqual(len(old_run_facts), 1)
        self.assertEqual(old_run_facts[0]["delta"], 11)

    def test_first_point_is_not_a_known_increment(self):
        report, _, _ = self._run([row("d1", "s0", 1, 0, "boot1", 250)], name="first_point")
        store = self.open_store()
        self.assertEqual(store.current_facts(), [])  # 首点之前不产生区间

    def test_boot_boundary_restarts_epoch(self):
        rows = [
            row("d1", "s0", 1, 0, "boot1", 250),
            row("d1", "s1", 1, 1, "boot1", 5),    # +11 同纪元
            row("d1", "s2", 1, 2, "boot2", 4),    # 重启：跨纪元未知
            row("d1", "s3", 1, 3, "boot2", 8),    # +4 同纪元
        ]
        self._run(rows, name="boot")
        store = self.open_store()
        facts = store.current_facts()
        self.assertEqual(len(facts), 3)  # 4 个节点 3 条邻接，首点无区间
        by_key = {key_of(f): f for f in facts}
        self.assertEqual(by_key[("s0", "s1")]["delta"], 11)
        self.assertEqual(by_key[("s1", "s2")]["status"], UNKNOWN)
        self.assertEqual(by_key[("s1", "s2")]["reason"], BOOT_BOUNDARY)
        self.assertIsNone(by_key[("s1", "s2")]["delta"])
        self.assertEqual(by_key[("s2", "s3")]["delta"], 4)

    def test_duplicate_rows_dedup_with_trace_and_revision_tie_quarantined(self):
        rows = [
            row("d1", "s0", 1, 0, "boot1", 10),
            row("d1", "s0", 1, 0, "boot1", 10),   # 完全重复
            row("d1", "s1", 1, 1, "boot1", 30),   # 唯一候选 +20
        ]
        report, _, _ = self._run(rows, name="duplicate")
        store = self.open_store()
        verdicts = {v["sample_id"]: v for v in store.verdicts_of_run(report.run_id)}
        self.assertEqual(verdicts["s0"]["duplicate_count"], 1)
        self.assertEqual(len(store.current_facts()), 1)
        self.assertEqual(store.current_facts()[0]["delta"], 20)

        # 平级冲突：同 sample_id 同 revision 不同 raw，整键隔离。
        rows_tie = rows + [row("d1", "s1", 1, 1, "boot1", 99)]
        report2, _, _ = self._run(rows_tie, name="duplicate_tie")
        verdicts2 = {v["sample_id"]: v for v in store.verdicts_of_run(report2.run_id)}
        self.assertEqual(verdicts2["s1"]["verdict"], QUARANTINED)
        facts = store.current_facts()
        self.assertTrue(all(f["status"] == UNKNOWN for f in facts))
        self.assertTrue(all(f["reason"] == QUARANTINE_BARRIER for f in facts))

    def test_simultaneous_different_values_not_arbitrarily_chosen(self):
        # 同刻异值发生在中间时刻 t=1（sA=20 vs sB=80），左右各一条邻接均未知。
        rows = [
            row("d1", "s0", 1, 0, "boot1", 10),
            row("d1", "sA", 1, 1, "boot1", 20),
            row("d1", "sB", 1, 1, "boot1", 80),
            row("d1", "s2", 1, 2, "boot1", 90),
        ]
        self._run(rows, name="simconflict")
        store = self.open_store()
        facts = store.current_facts()
        self.assertEqual(len(facts), 2)
        self.assertTrue(all(f["reason"] == SIMULTANEOUS_CONFLICT for f in facts))
        self.assertTrue(all(f["status"] == UNKNOWN for f in facts))

    def test_cross_day_known_delta_stays_unassigned(self):
        # origin 23:59:59(+08)：t=0 与 t=0.5 在 11-10，t>=1 跨入 11-11。
        rows = [
            row("d1", "s0", 1, 0, "boot1", 250),
            row("d1", "s05", 1, 0.5, "boot1", 255),  # 同日 +5
            row("d1", "s1", 1, 1, "boot1", 5),       # 跨日 +6，未分配
        ]
        self._run(rows, name="crossday")
        store = self.open_store()
        facts = store.current_facts()
        assigned = [f for f in facts if f["assignment_state"] == ASSIGNED]
        cross = [f for f in facts if f["assignment_state"] != ASSIGNED]
        self.assertEqual(len(assigned), 1)
        self.assertEqual(assigned[0]["assignment_day"], "2026-11-10")
        self.assertEqual(assigned[0]["delta"], 5)
        self.assertEqual(len(cross), 1)
        self.assertEqual(cross[0]["delta"], 6)  # 已知但不按时长拆分

        summary = store.daily_summary_current()
        by_day = {r["day"]: r for r in summary}
        self.assertEqual(by_day["2026-11-10"]["known_delta"], 5)
        self.assertEqual(by_day[UNASSIGNED_BUCKET]["known_delta"], 6)
        # 跨日增量没有被按比例摊到 11-11。
        self.assertNotIn("2026-11-11", by_day)

    def test_late_revision_only_reprocesses_affected_partition(self):
        rows = [
            row("d1", "s2", 1, 2, "boot2", 4),
            row("d1", "s3", 1, 3, "boot2", 8),
            row("d2", "s9", 1, 0, "boot9", 1),
            row("d2", "s9b", 1, 1, "boot9", 3),
        ]
        report1, _, _ = self._run(rows, name="laterev_v1")
        store = self.open_store()
        before = {f["fact_key"]: f for f in store.current_facts()}
        self.assertEqual(
            next(f for f in before.values()
                 if f["device"] == "d1" and f["right_samples"] == "s3")["delta"],
            4,
        )

        # 迟到高 revision 更正 s3：8 -> 9。
        rows2 = rows + [row("d1", "s3", 2, 3, "boot2", 9)]
        report2, samples, counters = self._run(rows2, name="laterev_v2")
        self.assertEqual(report2.changed_partitions, [("d1", "bytes")])
        self.assertEqual(report2.unchanged_partitions, [("d2", "bytes")])

        after = {f["fact_key"]: f for f in store.current_facts()}
        d1_fact = next(f for f in after.values() if f["device"] == "d1")
        self.assertEqual(d1_fact["delta"], 5)
        self.assertEqual(d1_fact["right_revision"], 2)
        # d2 事实行完全未被触碰（同一 run 身份仍是首次运行）。
        d2_fact = next(f for f in after.values() if f["device"] == "d2")
        self.assertEqual(d2_fact["run_id"], report1.run_id)
        self.assertEqual(d2_fact["current_run_id"], report1.run_id)

        verdicts = {v["sample_id"]: v for v in store.verdicts_of_run(report2.run_id)}
        self.assertEqual(verdicts["s3"]["superseded_revisions"], "1")
        # 旧版本仍可查，值为更正前的 4。
        old = store.facts_of_run(report1.run_id)
        self.assertEqual(next(f for f in old if f["device"] == "d1")["delta"], 4)

    def test_crash_before_commit_leaves_no_half_state_and_recovers(self):
        rows = [row("d1", "s0", 1, 0, "boot1", 250),
                row("d1", "s1", 1, 1, "boot1", 5)]
        report1, _, _ = self._run(rows, name="crash_v1")
        store = self.open_store()
        before_keys = {f["fact_key"] for f in store.current_facts()}
        before_counts = store.count_current_by_status()

        # 第二次运行在提交前崩溃：事务整体回滚，运行登记为 aborted。
        crashed_rows = rows + [row("d1", "s05", 1, 0.5, "boot1", 255)]
        samples, counters = write_inputs(self.input_dir / "crash_v2", crashed_rows)
        with self.assertRaises(RuntimeError):
            build_run(str(samples), str(counters), self.db, crash_before_commit=True)
        store = self.open_store()
        self.assertEqual(
            {f["fact_key"] for f in store.current_facts()}, before_keys
        )  # 没有半撤销半新增
        self.assertEqual(store.count_current_by_status(), before_counts)
        crashed = [r for r in store.list_runs() if r["status"] == ABORTED]
        self.assertEqual(len(crashed), 1)

        # 模拟进程被 kill -9：runs 行遗留 running，下次启动裁决为 aborted。
        store = self.open_store()
        zombie = store.start_run({
            "config_version": "x", "samples_sha256": None, "counters_sha256": None,
            "source_samples": "s", "source_counters": "c", "rate_overrides": "{}",
            "origin_iso": ORIGIN, "daily_timezone": TZ,
        })
        store = self.open_store()
        recovered = store.recover_stale_runs()
        self.assertEqual(recovered, [zombie])
        self.assertEqual(store.run_status(zombie), ABORTED)

        # 干净重跑成功，邻接替换正常完成。
        report3 = build_run(str(samples), str(counters), self.db)
        self.assertEqual(report3.recovered_runs, [])  # 上一步已恢复
        facts = store.current_facts()
        self.assertEqual(sorted(f["delta"] for f in facts), [5, 6])

    def test_rejected_rows_are_kept_not_dropped(self):
        rows = [
            row("d1", "s0", 1, 0, "boot1", 10),
            row("d1", "sBad", 1, 1, "boot1", 300),  # 超出 8 位无符号范围
            "d1,bytes,sBroken,1,not-a-time,boot1,5\n",
        ]
        report, _, _ = self._run(rows, name="reject")
        store = self.open_store()
        rejects = [v for v in store.verdicts_of_run(report.run_id)
                   if v["verdict"] == REJECTED]
        self.assertEqual(len(rejects), 2)
        # 唯一合法观测不产生区间，但数据没有被静默吞掉。
        self.assertEqual(len(store.current_facts()), 0)

    def test_pure_compute_failure_touches_no_store(self):
        # 未配置的计数器在打开存储之前就抛错：不产生 db 文件，无 running 残骸。
        case_dir = self.input_dir / "unconfigured"
        samples, counters = write_inputs(
            case_dir,
            [row("d1", "s0", 1, 0, "boot1", 1), row("d1", "s1", 1, 1, "boot1", 2)],
        )
        doc = json.loads(counters.read_text(encoding="utf-8"))
        doc["counters"]["other"] = doc["counters"].pop("bytes")
        counters.write_text(json.dumps(doc), encoding="utf-8")
        with self.assertRaises(ValueError):
            build_run(str(samples), str(counters), self.db)
        self.assertFalse(Path(self.db).exists())

    def test_daily_summary_counts_unknown_within_day(self):
        rows = [
            row("d1", "s1", 1, 1, "boot1", 5),
            row("d1", "s2", 1, 2, "boot2", 4),  # 重启未知，同在 11-11
            row("d1", "s3", 2, 3, "boot2", 9),  # +5 已知
        ]
        self._run(rows, name="daily_unknown")
        store = self.open_store()
        summary = store.daily_summary_current()
        self.assertEqual(len(summary), 1)
        bucket = summary[0]
        self.assertEqual(bucket["day"], "2026-11-11")
        self.assertEqual(bucket["known_delta"], 5)
        self.assertEqual(bucket["known_intervals"], 1)
        self.assertEqual(bucket["unknown_intervals"], 1)
        self.assertEqual(bucket["unknown_reasons"], {BOOT_BOUNDARY: 1})


if __name__ == "__main__":
    unittest.main()
