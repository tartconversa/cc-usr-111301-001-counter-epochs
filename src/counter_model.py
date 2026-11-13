"""ODS→DWD 计数器转换的纯函数层。

职责边界（本模块不接触数据库、文件路径与硬件）：

1. 观测解析与同采样键裁决：高 revision 替代旧版，平级异值冲突隔离，
   完全相同的重复行去重但留痕；
2. 按事件时刻排列节点：同一时刻不同采样读到不同值时标记同时刻冲突，
   绝不任意选一个值；
3. 仅在同一 ``boot_id`` 的相邻有效节点之间差分。raw 下降时模 ``2**bits``
   的差值只是候选增量，还要与最大增长速率上界比较：候选 0 个为超速不可
   恢复，候选多个为多圈歧义，二者都输出未知状态，绝不猜造精确值，也绝不
   对负差取绝对值；
4. 区间事实统一为半开区间 ``(start, end]``，按日归属时只有两端完全落在
   同一日历日的区间才能精确归属，跨日区间保留为未分配，不按时长拆分。
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

# 浮点速率上界比较的容差，仅用于消除 rate*dt 的二进制浮点误差。
EPS = 1e-9

# 裁决结论
ACCEPTED = "accepted"
QUARANTINED = "quarantined"
REJECTED = "rejected"

# 区间状态
KNOWN = "known"
UNKNOWN = "unknown"

# 未知原因
BOOT_BOUNDARY = "boot_boundary"
SIMULTANEOUS_CONFLICT = "simultaneous_conflict"
QUARANTINE_BARRIER = "quarantine_barrier"
RATE_CAP_EXCEEDED = "rate_cap_exceeded"
MULTI_TURN_AMBIGUOUS = "multi_turn_ambiguous"

# 裁决/拒收原因
REVISION_TIE = "revision_tie"
RAW_OUT_OF_RANGE = "raw_out_of_range"
MALFORMED_ROW = "malformed_row"

# 按日归属
ASSIGNED = "assigned"
UNASSIGNED_CROSS_DAY = "unassigned_cross_day"
UNASSIGNED_BUCKET = "UNASSIGNED"


@dataclass(frozen=True)
class Observation:
    """一条 ODS 观测（已通过字段与无符号位宽校验）。"""

    device: str
    counter: str
    sample_id: str
    revision: int
    event_seconds: float
    boot_id: str
    raw: int


@dataclass(frozen=True)
class Verdict:
    """一个采样键的裁决留痕。"""

    device: str
    counter: str
    sample_id: str
    revision: int
    event_seconds: float | None
    boot_id: str | None
    raw: int | None
    verdict: str
    reason: str | None
    superseded_revisions: tuple[int, ...] = ()
    duplicate_count: int = 0


@dataclass(frozen=True)
class Node:
    """事件时刻序列上的一个节点；同刻同值的多个采样合并为一个节点。"""

    device: str
    counter: str
    event_seconds: float
    sample_ids: tuple[str, ...]
    revisions: tuple[int, ...]
    boot_id: str | None
    raw: int | None
    quarantined: bool = False
    sim_conflict: bool = False


@dataclass(frozen=True)
class IntervalFact:
    """一条 DWD 区间增量事实，身份由两端采样与配置版本决定。"""

    device: str
    counter: str
    unit: str
    left_samples: tuple[str, ...]
    right_samples: tuple[str, ...]
    left_revision: int
    right_revision: int
    left_boot_id: str
    right_boot_id: str
    start_seconds: float
    end_seconds: float
    start_event_at: str
    end_event_at: str
    status: str
    delta: int | None
    reason: str | None
    config_version: str
    fact_key: str
    assignment_day: str | None
    assignment_state: str


# --------------------------------------------------------------------------- #
# 配置指纹
# --------------------------------------------------------------------------- #


def effective_config(counters_doc: dict, rate_overrides: dict | None = None) -> dict:
    """把 counters.json 归一成 {counter: {bits, unit, maximum_rate_per_second}}。

    rate_overrides 仅用于实验性复算（例如 fixtures 中的 300/秒歧义场景），
    覆盖会改变配置指纹，从而让所有相关分区按新版本重算。
    """
    cfg: dict[str, dict] = {}
    for name, spec in counters_doc["counters"].items():
        cfg[name] = {
            "bits": int(spec["bits"]),
            "unit": str(spec["unit"]),
            "maximum_rate_per_second": float(spec["maximum_rate_per_second"]),
        }
    for name, rate in (rate_overrides or {}).items():
        if name not in cfg:
            raise KeyError(f"速率上界覆盖了未配置的计数器: {name}")
        cfg[name]["maximum_rate_per_second"] = float(rate)
    return cfg


def config_fingerprint(cfg: dict) -> str:
    """推导配置版本：有效配置的规范 JSON 哈希。"""
    payload = json.dumps(cfg, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def partition_fingerprint(observations: list[Observation], config_version: str) -> str:
    """分区输入指纹：原始观测（含被隔离/被替代行）的规范序列哈希。

    补点、更正、重复重放都改变或保持该指纹，驱动增量失效判定。
    """
    rows = sorted(
        (o.device, o.counter, o.sample_id, o.revision, o.event_seconds, o.boot_id, o.raw)
        for o in observations
    )
    payload = json.dumps([config_version, rows], separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def make_fact_key(
    device: str,
    counter: str,
    left_samples: tuple[str, ...],
    right_samples: tuple[str, ...],
    config_version: str,
) -> str:
    """区间事实的稳定身份：与运行无关，同几何邻接复算得到同一键。"""
    payload = json.dumps(
        [device, counter, sorted(left_samples), sorted(right_samples), config_version],
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


# --------------------------------------------------------------------------- #
# 解析与裁决
# --------------------------------------------------------------------------- #


def parse_observations(rows: list[dict], counter_specs: dict):
    """把读取器产出的字符串行解析为 Observation；坏行拒收但不静默丢弃。

    返回 ``(observations, rejects)``。计数器未在配置中声明属于配置缺口，
    直接抛错而不是猜测位宽。
    """
    observations: list[Observation] = []
    rejects: list[Verdict] = []
    for index, row in enumerate(rows):
        synthetic_id = f"<row:{index}>"
        try:
            device = str(row["device"]).strip()
            counter = str(row["counter"]).strip()
            sample_id = str(row["sample_id"]).strip()
            boot_id = str(row["boot_id"]).strip()
            revision = int(row["revision"])
            event_seconds = float(row["event_seconds"])
            raw = int(row["raw"])
            if not device or not counter or not sample_id or not boot_id:
                raise ValueError("空字符串标识字段")
            if revision < 1:
                raise ValueError("revision 必须 >= 1")
        except (KeyError, TypeError, ValueError) as exc:
            rejects.append(
                Verdict(
                    device=str(row.get("device", "")).strip() or synthetic_id,
                    counter=str(row.get("counter", "")).strip() or synthetic_id,
                    sample_id=sample_id if "sample_id" in row else synthetic_id,
                    revision=0,
                    event_seconds=None,
                    boot_id=None,
                    raw=None,
                    verdict=REJECTED,
                    reason=f"{MALFORMED_ROW}: {exc}",
                )
            )
            continue
        if counter not in counter_specs:
            raise ValueError(f"计数器 {counter} 未在 counters 配置中声明，无法差分")
        modulus = 1 << int(counter_specs[counter]["bits"])
        if not (0 <= raw < modulus):
            rejects.append(
                Verdict(
                    device=device,
                    counter=counter,
                    sample_id=sample_id,
                    revision=revision,
                    event_seconds=event_seconds,
                    boot_id=boot_id,
                    raw=raw,
                    verdict=REJECTED,
                    reason=f"{RAW_OUT_OF_RANGE}: 0<=raw<{modulus}",
                )
            )
            continue
        observations.append(
            Observation(device, counter, sample_id, revision, event_seconds, boot_id, raw)
        )
    return observations, rejects


def adjudicate(observations: list[Observation]):
    """同采样键裁决。

    规则：完全相同的重复行去重留痕；取最大 revision；最大 revision 若有多条
    且内容不一致，平级冲突，整个采样键隔离（不参与任何已知差分）。

    返回 ``(winners, barriers, verdicts)``：winners 为有效观测，barriers 为
    被隔离采样键的占位观测，verdicts 为每个采样键一条裁决留痕。
    """
    grouped: dict[tuple[str, str, str], list[Observation]] = {}
    for obs in observations:
        grouped.setdefault((obs.device, obs.counter, obs.sample_id), []).append(obs)

    winners: list[Observation] = []
    barriers: list[Observation] = []
    verdicts: list[Verdict] = []

    for (device, counter, sample_id), items in sorted(grouped.items()):
        # 完全重复（含 revision 全等）去重。
        distinct: dict[tuple, Observation] = {}
        duplicate_count = 0
        for obs in items:
            sig = (obs.revision, obs.event_seconds, obs.boot_id, obs.raw)
            if sig in distinct:
                duplicate_count += 1
            else:
                distinct[sig] = obs
        unique_items = sorted(distinct.values(), key=lambda o: o.revision)
        top = max(o.revision for o in unique_items)
        top_items = [o for o in unique_items if o.revision == top]
        superseded = tuple(sorted({o.revision for o in unique_items if o.revision < top}))
        head = top_items[0]

        if len(top_items) > 1:
            # 平级冲突：内容不同又同 revision，无法裁决，隔离。
            barriers.append(head)
            verdicts.append(
                Verdict(
                    device, counter, sample_id, top, head.event_seconds,
                    head.boot_id, head.raw, QUARANTINED, REVISION_TIE,
                    superseded, duplicate_count,
                )
            )
            continue

        winners.append(head)
        verdicts.append(
            Verdict(
                device, counter, sample_id, top, head.event_seconds,
                head.boot_id, head.raw, ACCEPTED, None,
                superseded, duplicate_count,
            )
        )

    return winners, barriers, verdicts


def build_nodes(
    winners: list[Observation], barriers: list[Observation]
) -> dict[tuple[str, str], list[Node]]:
    """按 (device, counter) 生成事件时刻节点序列。

    同一时刻的节点合并：raw 不一致（或 boot 不一致）即同时刻冲突；
    被隔离采样键所在时刻整体成为屏障。
    """
    partitions: dict[tuple[str, str], list[Observation]] = {}
    for obs in [*winners, *barriers]:
        partitions.setdefault((obs.device, obs.counter), []).append(obs)

    result: dict[tuple[str, str], list[Node]] = {}
    for key, items in sorted(partitions.items()):
        device, counter = key
        by_time: dict[float, list[Observation]] = {}
        barrier_ids: dict[float, set[str]] = {}
        winner_ids: set = {(o.device, o.counter, o.sample_id) for o in winners}
        for obs in sorted(items, key=lambda o: (o.event_seconds, o.sample_id)):
            by_time.setdefault(obs.event_seconds, []).append(obs)
            if (obs.device, obs.counter, obs.sample_id) not in winner_ids:
                barrier_ids.setdefault(obs.event_seconds, set()).add(obs.sample_id)

        nodes: list[Node] = []
        for t, at_t in sorted(by_time.items()):
            sample_ids = tuple(sorted({o.sample_id for o in at_t}))
            revisions = tuple(sorted({o.revision for o in at_t}))
            raws = {o.raw for o in at_t}
            boots = {o.boot_id for o in at_t}
            quarantined = bool(barrier_ids.get(t))
            sim_conflict = len(raws) > 1 or len(boots) > 1
            nodes.append(
                Node(
                    device=device,
                    counter=counter,
                    event_seconds=t,
                    sample_ids=sample_ids,
                    revisions=revisions,
                    boot_id=next(iter(boots)) if len(boots) == 1 else None,
                    raw=next(iter(raws)) if len(raws) == 1 else None,
                    quarantined=quarantined,
                    sim_conflict=sim_conflict,
                )
            )
        result[key] = nodes
    return result


# --------------------------------------------------------------------------- #
# 模回绕差分
# --------------------------------------------------------------------------- #


def candidate_deltas(raw_left: int, raw_right: int, dt: float, bits: int, rate: float):
    """枚举速率上界内全部候选增量 d = naive + k*2^bits（k >= 0）。

    raw 下降时 naive 是模 2^bits 的回绕差；raw 上升时 naive 就是普通差值，
    但上界若容得下多圈同样要把后续候选列出，交由调用方判歧义。
    """
    modulus = 1 << bits
    cap = rate * dt
    naive = (raw_right - raw_left) % modulus
    candidates: list[int] = []
    k = 0
    while True:
        delta = naive + k * modulus
        if delta > cap + EPS:
            break
        candidates.append(delta)
        k += 1
    return candidates


def resolve_interval(raw_left: int, raw_right: int, dt: float, spec: dict):
    """同纪元相邻节点的差分裁决，返回 (status, delta, reason)。"""
    candidates = candidate_deltas(
        raw_left, raw_right, dt, int(spec["bits"]), float(spec["maximum_rate_per_second"])
    )
    if not candidates:
        return UNKNOWN, None, RATE_CAP_EXCEEDED
    if len(candidates) > 1:
        return UNKNOWN, None, MULTI_TURN_AMBIGUOUS
    return KNOWN, candidates[0], None


# --------------------------------------------------------------------------- #
# 时刻与按日归属
# --------------------------------------------------------------------------- #


def _resolve_zone(tz_name: str | None, origin: datetime):
    if tz_name:
        try:
            return ZoneInfo(tz_name)
        except ZoneInfoNotFoundError:
            pass
    offset = origin.utcoffset() or timedelta(0)
    return timezone(offset)


def event_at(origin_iso: str, tz_name: str | None, seconds: float) -> datetime:
    origin = datetime.fromisoformat(origin_iso)
    if origin.tzinfo is None:
        origin = origin.replace(tzinfo=timezone.utc)
    zone = _resolve_zone(tz_name, origin)
    return (origin + timedelta(seconds=seconds)).astimezone(zone)


def assign_day(origin_iso: str, tz_name: str | None, start_seconds: float, end_seconds):
    """返回 (assignment_day|None, assignment_state)。

    只有两端落在同一日历日才精确归属；跨日一律未分配，不做时长比例拆分。
    """
    left = event_at(origin_iso, tz_name, start_seconds)
    right = event_at(origin_iso, tz_name, end_seconds)
    if left.date() == right.date():
        return left.date().isoformat(), ASSIGNED
    return None, UNASSIGNED_CROSS_DAY


# --------------------------------------------------------------------------- #
# 区间事实构建
# --------------------------------------------------------------------------- #


def build_partition_facts(
    nodes: list[Node],
    spec: dict,
    config_version: str,
    origin_iso: str,
    tz_name: str | None,
) -> list[IntervalFact]:
    """把一个 (device, counter) 的节点序列转成相邻区间事实。

    序列首个节点之前不产生任何区间：首点读数不是“从 0 开始”的已知增量。
    """
    facts: list[IntervalFact] = []
    if not nodes:
        return facts
    unit = str(spec["unit"])

    for left, right in zip(nodes, nodes[1:]):
        dt = right.event_seconds - left.event_seconds
        status, delta, reason = UNKNOWN, None, None

        # 原因优先级：隔离屏障 > 同时刻冲突 > 重启纪元 > 速率/回绕数学。
        if left.quarantined or right.quarantined:
            reason = QUARANTINE_BARRIER
        elif left.sim_conflict or right.sim_conflict:
            reason = SIMULTANEOUS_CONFLICT
        elif left.boot_id != right.boot_id or left.boot_id is None or right.boot_id is None:
            reason = BOOT_BOUNDARY
        elif dt <= 0:
            # 理论上同刻已合并；保留防御性分支，绝不产生零/负时长已知增量。
            reason = SIMULTANEOUS_CONFLICT
        else:
            status, delta, reason = resolve_interval(left.raw, right.raw, dt, spec)

        day, assignment_state = assign_day(
            origin_iso, tz_name, left.event_seconds, right.event_seconds
        )
        facts.append(
            IntervalFact(
                device=left.device,
                counter=left.counter,
                unit=unit,
                left_samples=left.sample_ids,
                right_samples=right.sample_ids,
                left_revision=max(left.revisions),
                right_revision=max(right.revisions),
                left_boot_id=left.boot_id or "",
                right_boot_id=right.boot_id or "",
                start_seconds=left.event_seconds,
                end_seconds=right.event_seconds,
                start_event_at=event_at(
                    origin_iso, tz_name, left.event_seconds
                ).isoformat(),
                end_event_at=event_at(
                    origin_iso, tz_name, right.event_seconds
                ).isoformat(),
                status=status,
                delta=delta,
                reason=reason,
                config_version=config_version,
                fact_key=make_fact_key(
                    left.device, left.counter, left.sample_ids, right.sample_ids, config_version
                ),
                assignment_day=day,
                assignment_state=assignment_state,
            )
        )
    return facts


# --------------------------------------------------------------------------- #
# 按日汇总
# --------------------------------------------------------------------------- #


def summarize_daily(facts: list[IntervalFact]) -> list[dict]:
    """按 (device, counter, 日) 汇总当前事实；跨日进 UNASSIGNED 桶。

    未知区间只计数不贡献增量；任何桶都不按时长凭空拆分跨日增量。
    """
    buckets: dict[tuple, dict] = {}

    def bucket(key):
        if key not in buckets:
            buckets[key] = {
                "device": key[0],
                "counter": key[1],
                "day": key[2],
                "known_intervals": 0,
                "known_delta": 0,
                "unknown_intervals": 0,
                "unknown_reasons": {},
            }
        return buckets[key]

    for fact in facts:
        day = fact.assignment_day if fact.assignment_state == ASSIGNED else UNASSIGNED_BUCKET
        row = bucket((fact.device, fact.counter, day))
        if fact.status == KNOWN:
            row["known_intervals"] += 1
            row["known_delta"] += fact.delta
        else:
            row["unknown_intervals"] += 1
            row["unknown_reasons"][fact.reason] = row["unknown_reasons"].get(fact.reason, 0) + 1

    return sorted(buckets.values(), key=lambda r: (r["device"], r["counter"], r["day"]))
