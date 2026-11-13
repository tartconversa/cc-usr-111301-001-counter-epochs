"""DWD 事实层的 SQLite 存储与增量失效算法。

索引设计（写入时建齐，查询走覆盖路径）：

* ``idx_facts_lookup(device, counter, is_current, run_id)``——取某分区当前事实；
* ``idx_facts_factkey(fact_key, is_current)``——邻接替换前后按稳定身份对比；
* ``idx_facts_run(run_id)``——复算任意一次旧运行的完整事实集；
* ``idx_verdicts_run(run_id)``、``idx_partitions_device(device, counter)``。

增量失效算法：

1. 每次运行开始把非本进程遗留的 ``running`` 运行判为 ``aborted``（进程恢复）；
2. 计算每个 (device, counter) 分区的输入指纹（观测序列 + 配置版本）；
3. 指纹未变的分区原样保留，新版本运行不触碰其事实行；
4. 指纹变化的分区在**同一个事务**内：旧当前事实标记 ``superseded``、
   旧分区行标记 replaced、写入新事实行（current）与新分区行。提交失败则
   整体回滚，事实层永远不会出现“一半撤销一半新增”的中间态；
5. 旧运行的事实行永不物理删除，配合 run 注册表可查任意历史版本。
"""

from __future__ import annotations

import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone

from .counter_model import (
    IntervalFact,
    Verdict,
    partition_fingerprint,
    summarize_daily,
)

RUNNING = "running"
COMMITTED = "committed"
ABORTED = "aborted"

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    run_id          TEXT PRIMARY KEY,
    started_at      TEXT NOT NULL,
    finished_at     TEXT,
    status          TEXT NOT NULL,
    config_version  TEXT NOT NULL,
    samples_sha256  TEXT,
    counters_sha256 TEXT,
    source_samples  TEXT NOT NULL,
    source_counters TEXT NOT NULL,
    rate_overrides  TEXT NOT NULL,
    origin_iso      TEXT NOT NULL,
    daily_timezone  TEXT
);
CREATE TABLE IF NOT EXISTS partitions (
    run_id          TEXT NOT NULL,
    device          TEXT NOT NULL,
    counter         TEXT NOT NULL,
    input_fingerprint TEXT NOT NULL,
    state           TEXT NOT NULL,
    replaced_by_run TEXT,
    PRIMARY KEY (run_id, device, counter)
);
CREATE TABLE IF NOT EXISTS sample_verdicts (
    run_id            TEXT NOT NULL,
    device            TEXT NOT NULL,
    counter           TEXT NOT NULL,
    sample_id         TEXT NOT NULL,
    revision          INTEGER NOT NULL,
    event_seconds     REAL,
    boot_id           TEXT,
    raw               INTEGER,
    verdict           TEXT NOT NULL,
    reason            TEXT,
    superseded_revisions TEXT NOT NULL,
    duplicate_count   INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS interval_facts (
    run_id             TEXT NOT NULL,
    fact_key           TEXT NOT NULL,
    device             TEXT NOT NULL,
    counter            TEXT NOT NULL,
    unit               TEXT NOT NULL,
    left_samples       TEXT NOT NULL,
    right_samples      TEXT NOT NULL,
    left_revision      INTEGER NOT NULL,
    right_revision     INTEGER NOT NULL,
    left_boot_id       TEXT NOT NULL,
    right_boot_id      TEXT NOT NULL,
    start_seconds      REAL NOT NULL,
    end_seconds        REAL NOT NULL,
    start_event_at     TEXT NOT NULL,
    end_event_at       TEXT NOT NULL,
    status             TEXT NOT NULL,
    delta              INTEGER,
    reason             TEXT,
    config_version     TEXT NOT NULL,
    is_current         INTEGER NOT NULL,
    current_run_id     TEXT NOT NULL,
    superseded_run_id  TEXT,
    assignment_day     TEXT,
    assignment_state   TEXT NOT NULL,
    PRIMARY KEY (run_id, fact_key)
);
CREATE INDEX IF NOT EXISTS idx_facts_lookup
    ON interval_facts(device, counter, is_current, current_run_id);
CREATE INDEX IF NOT EXISTS idx_facts_factkey
    ON interval_facts(fact_key, is_current);
CREATE INDEX IF NOT EXISTS idx_facts_run ON interval_facts(run_id);
CREATE INDEX IF NOT EXISTS idx_verdicts_run ON sample_verdicts(run_id);
CREATE INDEX IF NOT EXISTS idx_partitions_device
    ON partitions(device, counter, state);
"""


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def new_run_id() -> str:
    return "r" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:8]


class CounterStore:
    def __init__(self, path: str):
        # check_same_thread=False 由调用方自行保证串行；本工具为单进程批处理。
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.executescript(SCHEMA)
        self.conn.commit()

    def close(self):
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False

    # -- 进程恢复 ---------------------------------------------------------- #

    def recover_stale_runs(self) -> list[str]:
        """把非本进程遗留的 running 运行判为 aborted，返回被恢复的 run_id 列表。"""
        stale = [
            row["run_id"]
            for row in self.conn.execute(
                "SELECT run_id FROM runs WHERE status = ?", (RUNNING,)
            )
        ]
        for run_id in stale:
            self.conn.execute(
                "UPDATE runs SET status = ?, finished_at = ? WHERE run_id = ?",
                (ABORTED, now_iso(), run_id),
            )
            # 该运行的事实/分区行从未作为 current 提交，无需翻转 is_current。
        self.conn.commit()
        return stale

    def start_run(self, meta: dict) -> str:
        run_id = new_run_id()
        self.conn.execute(
            """INSERT INTO runs(run_id, started_at, status, config_version,
                  samples_sha256, counters_sha256, source_samples, source_counters,
                  rate_overrides, origin_iso, daily_timezone)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (
                run_id, now_iso(), RUNNING, meta["config_version"],
                meta.get("samples_sha256"), meta.get("counters_sha256"),
                meta["source_samples"], meta["source_counters"],
                meta["rate_overrides"], meta["origin_iso"], meta.get("daily_timezone"),
            ),
        )
        self.conn.commit()
        return run_id

    # -- 单事务邻接替换 ---------------------------------------------------- #

    @contextmanager
    def transaction(self):
        """单个原子事务：正常退出提交，任何异常整体回滚。"""
        try:
            yield self.conn
        except BaseException:
            self.conn.rollback()
            raise
        else:
            self.conn.commit()

    def replace_partition(
        self,
        run_id: str,
        device: str,
        counter: str,
        observations,
        config_version: str,
        facts: list[IntervalFact],
        verdicts: list[Verdict],
    ):
        """在调用方持有的事务内完成一个分区的“撤销旧邻接 + 建立新邻接”。"""
        conn = self.conn
        fingerprint = partition_fingerprint(observations, config_version)

        # 1) 旧当前事实整体下线（同一条 UPDATE，不会留下半撤销状态）。
        conn.execute(
            """UPDATE interval_facts
                  SET is_current = 0, superseded_run_id = ?
                WHERE device = ? AND counter = ? AND is_current = 1""",
            (run_id, device, counter),
        )
        # 2) 旧分区行标记 replaced。
        conn.execute(
            """UPDATE partitions SET state = 'replaced', replaced_by_run = ?
                WHERE device = ? AND counter = ?""",
            (run_id, device, counter),
        )
        # 3) 新事实行写入，current 归属本次运行。
        conn.executemany(
            """INSERT INTO interval_facts(run_id, fact_key, device, counter, unit,
                   left_samples, right_samples, left_revision, right_revision,
                   left_boot_id, right_boot_id, start_seconds, end_seconds,
                   start_event_at, end_event_at, status, delta, reason,
                   config_version, is_current, current_run_id, superseded_run_id,
                   assignment_day, assignment_state)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,?,NULL,?,?)""",
            [
                (
                    run_id, f.fact_key, f.device, f.counter, f.unit,
                    "|".join(f.left_samples), "|".join(f.right_samples),
                    f.left_revision, f.right_revision, f.left_boot_id,
                    f.right_boot_id, f.start_seconds, f.end_seconds,
                    f.start_event_at, f.end_event_at, f.status, f.delta, f.reason,
                    f.config_version, run_id, f.assignment_day, f.assignment_state,
                )
                for f in facts
            ],
        )
        # 4) 新分区行。
        conn.execute(
            """INSERT INTO partitions(run_id, device, counter, input_fingerprint,
                   state, replaced_by_run) VALUES (?,?,?,?,'current',NULL)""",
            (run_id, device, counter, fingerprint),
        )
        # 5) 裁决留痕。
        conn.executemany(
            """INSERT INTO sample_verdicts(run_id, device, counter, sample_id,
                   revision, event_seconds, boot_id, raw, verdict, reason,
                   superseded_revisions, duplicate_count)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            [
                (
                    run_id, v.device, v.counter, v.sample_id, v.revision,
                    v.event_seconds, v.boot_id, v.raw, v.verdict, v.reason,
                    "|".join(map(str, v.superseded_revisions)), v.duplicate_count,
                )
                for v in verdicts
            ],
        )

    def register_unchanged_partition(
        self, run_id: str, device: str, counter: str, fingerprint: str, verdicts: list
    ):
        """指纹未变：不复制事实行，仅登记分区归属并留裁决痕。"""
        self.conn.execute(
            """INSERT INTO partitions(run_id, device, counter, input_fingerprint,
                   state, replaced_by_run) VALUES (?,?,?,?,'unchanged',NULL)""",
            (run_id, device, counter, fingerprint),
        )
        self.conn.executemany(
            """INSERT INTO sample_verdicts(run_id, device, counter, sample_id,
                   revision, event_seconds, boot_id, raw, verdict, reason,
                   superseded_revisions, duplicate_count)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            [
                (
                    run_id, v.device, v.counter, v.sample_id, v.revision,
                    v.event_seconds, v.boot_id, v.raw, v.verdict, v.reason,
                    "|".join(map(str, v.superseded_revisions)), v.duplicate_count,
                )
                for v in verdicts
            ],
        )

    def commit_run(self, run_id: str):
        with self.transaction():
            self.conn.execute(
                "UPDATE runs SET status = ?, finished_at = ? WHERE run_id = ?",
                (COMMITTED, now_iso(), run_id),
            )

    def abort_run(self, run_id: str):
        self.conn.rollback()
        self.conn.execute(
            "UPDATE runs SET status = ?, finished_at = ? WHERE run_id = ?",
            (ABORTED, now_iso(), run_id),
        )
        self.conn.commit()

    # -- 查询 -------------------------------------------------------------- #

    def current_fingerprints(self) -> dict[tuple[str, str], str]:
        return {
            (row["device"], row["counter"]): row["input_fingerprint"]
            for row in self.conn.execute(
                "SELECT device, counter, input_fingerprint FROM partitions WHERE state = 'current'"
            )
        }

    def run_status(self, run_id: str) -> str | None:
        row = self.conn.execute(
            "SELECT status FROM runs WHERE run_id = ?", (run_id,)
        ).fetchone()
        return row["status"] if row else None

    def list_runs(self) -> list[dict]:
        return [
            dict(r)
            for r in self.conn.execute(
                "SELECT run_id, started_at, finished_at, status, config_version FROM runs ORDER BY started_at"
            )
        ]

    def current_facts(self, device: str | None = None, counter: str | None = None):
        sql = "SELECT * FROM interval_facts WHERE is_current = 1"
        params: list = []
        if device is not None:
            sql += " AND device = ?"
            params.append(device)
        if counter is not None:
            sql += " AND counter = ?"
            params.append(counter)
        sql += " ORDER BY device, counter, start_seconds"
        return [dict(r) for r in self.conn.execute(sql, params)]

    def facts_of_run(self, run_id: str) -> list[dict]:
        return [
            dict(r)
            for r in self.conn.execute(
                "SELECT * FROM interval_facts WHERE run_id = ? ORDER BY device, counter, start_seconds",
                (run_id,),
            )
        ]

    def facts_around(self, run_id: str, device: str, counter: str):
        """邻接替换前后对比：被本次运行 supersede 的旧行 + 本次新行。"""
        return [
            dict(r)
            for r in self.conn.execute(
                """SELECT * FROM interval_facts
                    WHERE device = ? AND counter = ?
                      AND (run_id = ? OR superseded_run_id = ?)
                    ORDER BY start_seconds, run_id""",
                (device, counter, run_id, run_id),
            )
        ]

    def verdicts_of_run(self, run_id: str) -> list[dict]:
        return [
            dict(r)
            for r in self.conn.execute(
                "SELECT * FROM sample_verdicts WHERE run_id = ? ORDER BY device, counter, sample_id",
                (run_id,),
            )
        ]

    def daily_summary_current(self) -> list[dict]:
        facts = [_row_to_fact(r) for r in self.current_facts()]
        return summarize_daily(facts)

    def count_current_by_status(self) -> dict:
        rows = self.conn.execute(
            "SELECT status, COUNT(*) AS n FROM interval_facts WHERE is_current = 1 GROUP BY status"
        ).fetchall()
        out = {"known": 0, "unknown": 0}
        for row in rows:
            out[row["status"]] = row["n"]
        return out


def _row_to_fact(row: sqlite3.Row):
    d = dict(row)
    return IntervalFact(
        device=d["device"],
        counter=d["counter"],
        unit=d["unit"],
        left_samples=tuple(d["left_samples"].split("|")) if d["left_samples"] else (),
        right_samples=tuple(d["right_samples"].split("|")) if d["right_samples"] else (),
        left_revision=d["left_revision"],
        right_revision=d["right_revision"],
        left_boot_id=d["left_boot_id"],
        right_boot_id=d["right_boot_id"],
        start_seconds=d["start_seconds"],
        end_seconds=d["end_seconds"],
        start_event_at=d["start_event_at"],
        end_event_at=d["end_event_at"],
        status=d["status"],
        delta=d["delta"],
        reason=d["reason"],
        config_version=d["config_version"],
        fact_key=d["fact_key"],
        assignment_day=d["assignment_day"],
        assignment_state=d["assignment_state"],
    )
