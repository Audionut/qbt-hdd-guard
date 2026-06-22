from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any


class GuardState:
    def __init__(self, state_dir: Path) -> None:
        self.state_dir = state_dir
        self.path = state_dir / "hdd-guard-state.json"
        self.data: dict[str, Any] = {
            "version": 1,
            "last_auto_unban": None,
            "unban_cycle": 0,
            "bans": {},
            "reputation": {},
            "ip_reputation": {},
            "activity_wake_history": {
                "by_endpoint": {},
                "by_ip": {},
            },
        }

    @classmethod
    def load(cls, state_dir: Path) -> "GuardState":
        state_dir.mkdir(parents=True, exist_ok=True)
        state = cls(state_dir)
        if state.path.exists():
            loaded = json.loads(state.path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                state.data.update(loaded)
        state.prune_empty_ban_records()
        state.prune_empty_ip_reputation()
        state.prune_empty_reputation()
        state.seed_activity_wake_history_from_reputation()
        state.save()
        return state

    def save(self) -> None:
        self.state_dir.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.data, indent=2, sort_keys=True), encoding="utf-8")
        tmp.replace(self.path)
        self._write_compat_lists()

    @property
    def ban_audit_file(self) -> Path:
        return self.state_dir / "ban-audit.jsonl"

    @property
    def near_miss_audit_file(self) -> Path:
        return self.state_dir / "near-miss-audit.jsonl"

    def append_ban_audit(self, record: dict[str, Any]) -> None:
        self.state_dir.mkdir(parents=True, exist_ok=True)
        with self.ban_audit_file.open("a", encoding="utf-8") as file:
            file.write(json.dumps(record, sort_keys=True, default=str))
            file.write("\n")

    def append_near_miss_audit(self, record: dict[str, Any]) -> None:
        self.state_dir.mkdir(parents=True, exist_ok=True)
        with self.near_miss_audit_file.open("a", encoding="utf-8") as file:
            file.write(json.dumps(record, sort_keys=True, default=str))
            file.write("\n")

    @property
    def unban_cycle(self) -> int:
        return int(self.data.get("unban_cycle", 0) or 0)

    @property
    def last_auto_unban(self) -> float | None:
        value = self.data.get("last_auto_unban")
        return None if value is None else float(value)

    def mark_auto_unban(self, timestamp: float, *, advance_cycle: bool) -> None:
        self.data["last_auto_unban"] = timestamp
        if advance_cycle:
            self.data["unban_cycle"] = self.unban_cycle + 1
        self.save()

    def ban_record(self, subject: str) -> dict[str, Any]:
        return self.data.setdefault("bans", {}).setdefault(subject, {})

    def get_ban_record(self, subject: str) -> dict[str, Any]:
        record = self.data.get("bans", {}).get(subject, {})
        return record if isinstance(record, dict) else {}

    def is_permanent(self, subject: str) -> bool:
        return bool(self.get_ban_record(subject).get("permanent", False))

    def is_banned_this_cycle(self, subject: str) -> bool:
        record = self.get_ban_record(subject)
        return int(record.get("count", 0) or 0) > 0 and record.get("last_counted_cycle") == self.unban_cycle

    def permanent_subjects(self) -> set[str]:
        return {subject for subject, record in self.data.get("bans", {}).items() if record.get("permanent")}

    def record_ban(self, subject: str, timestamp: float, permanent_after: int) -> tuple[int, bool, bool]:
        record = self.ban_record(subject)
        counted = record.get("last_counted_cycle") != self.unban_cycle
        if counted:
            record["count"] = int(record.get("count", 0) or 0) + 1
            record["last_counted_cycle"] = self.unban_cycle
        record["last_ban_time"] = timestamp
        promoted = False
        if counted and permanent_after and int(record.get("count", 0)) >= permanent_after:
            promoted = not record.get("permanent", False)
            record["permanent"] = True
            record["last_permanent_time"] = timestamp
            record["permanent_generation"] = int(record.get("permanent_generation", 0) or 0) + 1
        self.save()
        return int(record.get("count", 0) or 0), counted, promoted

    def expire_first_generation_permanent(self, now: float, max_age: float) -> list[str]:
        if max_age <= 0:
            return []
        expired: list[str] = []
        for subject, record in self.data.get("bans", {}).items():
            if not record.get("permanent"):
                continue
            if int(record.get("permanent_generation", 1) or 1) > 1:
                continue
            ts = float(record.get("last_permanent_time", now) or now)
            if now - ts >= max_age:
                record["permanent"] = False
                record["expired_permanent_time"] = now
                expired.append(subject)
        if expired:
            self.save()
        return expired

    def reputation(self, endpoint: str) -> dict[str, Any]:
        return self.data.setdefault("reputation", {}).setdefault(
            endpoint,
            {
                "first_seen": time.time(),
                "last_seen": 0.0,
                "session_count": 0,
                "bad_session_count": 0,
                "low_speed_session_count": 0,
                "burst_session_count": 0,
                "etw_matched_session_count": 0,
                "multi_torrent_bad_window_count": 0,
                "total_uploaded": 0,
                "total_etw_read_bytes": 0,
                "total_connected_seconds": 0.0,
                "total_low_speed_seconds": 0.0,
                "recent_productive_session_count": 0,
                "last_productive_seen": None,
                "reputation_score": 0.0,
                "score_decay_timestamp": time.time(),
            },
        )

    def get_reputation(self, endpoint: str) -> dict[str, Any]:
        record = self.data.get("reputation", {}).get(endpoint, {})
        return record if isinstance(record, dict) else {}

    def ip_reputation(self, ip: str) -> dict[str, Any]:
        return self.data.setdefault("ip_reputation", {}).setdefault(
            ip,
            {
                "distinct_endpoints": [],
                "distinct_bad_endpoints": [],
                "total_bad_sessions": 0,
                "total_etw_matched_sessions": 0,
                "total_uploaded": 0,
                "total_etw_read_bytes": 0,
                "last_bad_seen": None,
                "ip_reputation_score": 0.0,
            },
        )

    def get_ip_reputation(self, ip: str) -> dict[str, Any]:
        record = self.data.get("ip_reputation", {}).get(ip, {})
        return record if isinstance(record, dict) else {}

    def activity_wake_history(self) -> dict[str, dict[str, list[dict[str, Any]]]]:
        history = self.data.setdefault("activity_wake_history", {})
        if not isinstance(history, dict):
            history = {}
            self.data["activity_wake_history"] = history
        by_endpoint = history.setdefault("by_endpoint", {})
        by_ip = history.setdefault("by_ip", {})
        if not isinstance(by_endpoint, dict):
            by_endpoint = {}
            history["by_endpoint"] = by_endpoint
        if not isinstance(by_ip, dict):
            by_ip = {}
            history["by_ip"] = by_ip
        return {"by_endpoint": by_endpoint, "by_ip": by_ip}

    def set_activity_wake_history(
        self,
        *,
        by_endpoint: dict[str, list[dict[str, Any]]],
        by_ip: dict[str, list[dict[str, Any]]],
    ) -> None:
        self.data["activity_wake_history"] = {
            "by_endpoint": by_endpoint,
            "by_ip": by_ip,
        }
        self.save()

    def seed_activity_wake_history_from_reputation(self) -> None:
        history = self.activity_wake_history()
        by_endpoint = history["by_endpoint"]
        by_ip = history["by_ip"]
        changed = False
        for endpoint, rep in self.data.get("reputation", {}).items():
            if not isinstance(rep, dict) or endpoint in by_endpoint:
                continue
            count = int(rep.get("activity_wake_churn_session_count", 0) or 0)
            if count <= 0:
                continue
            ip = str(rep.get("ip", "") or _endpoint_ip(endpoint))
            ts = float(rep.get("last_seen", 0.0) or time.time())
            total_payload = int(rep.get("total_uploaded", 0) or 0)
            total_etw = int(rep.get("total_etw_read_bytes", 0) or 0)
            events: list[dict[str, Any]] = []
            for index in range(count):
                payload = total_payload if index == 0 else 0
                etw = total_etw if index == 0 else 0
                events.append(
                    {
                        "ts": ts,
                        "start": ts,
                        "end": ts,
                        "duration": 0.0,
                        "endpoint": endpoint,
                        "ip": ip,
                        "port": _endpoint_port(endpoint),
                        "torrent_hash": "",
                        "payload": payload,
                        "average_speed": 0.0,
                        "max_speed": 0,
                        "etw_bytes": etw,
                        "raw_etw_bytes": etw,
                        "etw_count": 1 if etw > 0 else 0,
                        "torrent_wake": True,
                        "tiny_burst": payload > 0,
                        "wake_weight": 2 if payload <= 0 and etw > 0 else 1,
                        "synthetic_from_reputation": True,
                    }
                )
            by_endpoint[endpoint] = events
            by_ip.setdefault(ip, []).extend(events)
            changed = True
        if changed:
            self.data["activity_wake_history"] = {
                "by_endpoint": by_endpoint,
                "by_ip": by_ip,
            }

    def decay_reputation(self, endpoint: str, now: float, half_life_seconds: float) -> None:
        rep = self.reputation(endpoint)
        last = float(rep.get("score_decay_timestamp", now) or now)
        if half_life_seconds <= 0 or now <= last:
            rep["score_decay_timestamp"] = now
            return
        periods = int((now - last) // half_life_seconds)
        if periods > 0:
            rep["reputation_score"] = float(rep.get("reputation_score", 0.0) or 0.0) / (2 ** periods)
            rep["score_decay_timestamp"] = last + periods * half_life_seconds

    def prune_stale_reputation(self, now: float, max_age: float) -> None:
        if max_age <= 0:
            return
        banned = set(self.data.get("bans", {}))
        reps = self.data.get("reputation", {})
        stale = [
            endpoint
            for endpoint, rep in reps.items()
            if endpoint not in banned and now - float(rep.get("last_seen", now) or now) > max_age
        ]
        for endpoint in stale:
            reps.pop(endpoint, None)
        if stale:
            self.save()

    def prune_empty_ban_records(self) -> int:
        bans = self.data.get("bans", {})
        if not isinstance(bans, dict):
            self.data["bans"] = {}
            return 0
        empty = [
            subject
            for subject, record in bans.items()
            if isinstance(record, dict)
            and int(record.get("count", 0) or 0) <= 0
            and not record.get("permanent")
            and not record.get("last_ban_time")
        ]
        for subject in empty:
            bans.pop(subject, None)
        return len(empty)

    def prune_empty_reputation(self) -> int:
        reps = self.data.get("reputation", {})
        if not isinstance(reps, dict):
            self.data["reputation"] = {}
            return 0
        empty = [
            endpoint
            for endpoint, record in reps.items()
            if isinstance(record, dict)
            and int(record.get("bad_session_count", 0) or 0) <= 0
            and int(record.get("burst_session_count", 0) or 0) <= 0
            and int(record.get("activity_wake_churn_session_count", 0) or 0) <= 0
            and int(record.get("etw_matched_session_count", 0) or 0) <= 0
            and int(record.get("recent_productive_session_count", 0) or 0) <= 0
            and int(record.get("total_uploaded", 0) or 0) <= 0
            and int(record.get("total_etw_read_bytes", 0) or 0) <= 0
        ]
        for endpoint in empty:
            reps.pop(endpoint, None)
        return len(empty)

    def prune_empty_ip_reputation(self) -> int:
        reps = self.data.get("ip_reputation", {})
        if not isinstance(reps, dict):
            self.data["ip_reputation"] = {}
            return 0
        empty = [
            ip
            for ip, record in reps.items()
            if isinstance(record, dict)
            and (
                not str(ip).strip()
                or (
                    not record.get("distinct_bad_endpoints")
                    and int(record.get("total_bad_sessions", 0) or 0) <= 0
                    and int(record.get("total_etw_matched_sessions", 0) or 0) <= 0
                    and float(record.get("ip_reputation_score", 0.0) or 0.0) <= 0
                    and not record.get("last_bad_seen")
                )
            )
        ]
        for ip in empty:
            reps.pop(ip, None)
        return len(empty)

    def _write_compat_lists(self) -> None:
        bans = self.data.get("bans", {})
        banned_lines = [
            f"{subject},{int(record.get('count', 0) or 0)}"
            for subject, record in sorted(bans.items())
            if int(record.get("count", 0) or 0) > 0 or record.get("permanent")
        ]
        perma_lines = [subject for subject, record in sorted(bans.items()) if record.get("permanent")]
        (self.state_dir / "banned-clients.txt").write_text("\n".join(banned_lines) + ("\n" if banned_lines else ""), encoding="utf-8")
        (self.state_dir / "perma-banned-clients.txt").write_text("\n".join(perma_lines) + ("\n" if perma_lines else ""), encoding="utf-8")


def _endpoint_port(endpoint: str) -> int | None:
    _, sep, port = endpoint.rpartition(":")
    return int(port) if sep and port.isdigit() else None


def _endpoint_ip(endpoint: str) -> str:
    host, sep, port = endpoint.rpartition(":")
    if sep and host and port.isdigit():
        return host
    return endpoint
