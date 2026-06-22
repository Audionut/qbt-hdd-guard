from __future__ import annotations

from dataclasses import dataclass, field
from statistics import fmean
from typing import Any

from .models import CONFIDENCE, BanDecision, EtwReadEvent, GuardConfig, PeerSnapshot
from .pathmap import PathMapper
from .state import GuardState


@dataclass
class PeerSession:
    endpoint: str
    ip: str
    torrent_hash: str
    torrent_name: str
    first_seen: float
    last_seen: float
    uploaded_start: int | None = None
    uploaded_last: int = 0
    last_poll_uploaded_delta: int = 0
    last_poll_etw_bytes: int = 0
    last_poll_raw_etw_bytes: int = 0
    last_poll_etw_count: int = 0
    last_poll_confidence_value: int = 0
    speed_samples: list[int] = field(default_factory=list)
    files: tuple[Any, ...] = field(default_factory=tuple)
    productive_polls: int = 0
    consecutive_good_polls: int = 0
    etw_bytes: int = 0
    attributed_etw_bytes: int = 0
    etw_count: int = 0
    etw_paths: set[str] = field(default_factory=set)
    confidence_value: int = 0
    file_peer_count: int = 0
    etw_attribution_methods: set[str] = field(default_factory=set)
    external_read_bytes: int = 0
    external_read_count: int = 0
    external_processes: set[str] = field(default_factory=set)
    torrent_wake_seen: bool = False

    def observe(self, snapshot: PeerSnapshot, now: float, config: GuardConfig) -> None:
        self.last_seen = now
        self.ip = snapshot.ip
        self.torrent_name = snapshot.torrent_name
        self.files = snapshot.files or self.files
        self.speed_samples.append(snapshot.up_speed)
        self.last_poll_uploaded_delta = 0
        self.last_poll_etw_bytes = 0
        self.last_poll_raw_etw_bytes = 0
        self.last_poll_etw_count = 0
        self.last_poll_confidence_value = 0
        if self.uploaded_start is None or snapshot.uploaded < self.uploaded_start:
            self.uploaded_start = snapshot.uploaded
            self.uploaded_last = snapshot.uploaded
        elif snapshot.uploaded >= self.uploaded_last:
            self.last_poll_uploaded_delta = snapshot.uploaded - self.uploaded_last
            self.uploaded_last = snapshot.uploaded

        if snapshot.up_speed >= config.effective_productive_speed:
            self.productive_polls += 1
        if snapshot.up_speed >= int(config.low_speed_threshold * config.good_speed_multiplier):
            self.consecutive_good_polls += 1
        else:
            self.consecutive_good_polls = 0

    def add_etw(
        self,
        event: EtwReadEvent,
        confidence_value: int,
        file_peer_count: int,
        attributed_size: int,
        attribution_method: str,
    ) -> None:
        raw_size = max(0, int(event.size))
        attributed_size = max(0, int(attributed_size))
        self.etw_bytes += raw_size
        self.attributed_etw_bytes += attributed_size
        self.etw_count += 1
        self.etw_paths.add(event.path)
        self.confidence_value = max(self.confidence_value, confidence_value)
        self.file_peer_count = max(self.file_peer_count, file_peer_count)
        self.etw_attribution_methods.add(attribution_method)
        self.last_poll_etw_bytes += attributed_size
        self.last_poll_raw_etw_bytes += raw_size
        self.last_poll_etw_count += 1
        self.last_poll_confidence_value = max(self.last_poll_confidence_value, confidence_value)

    def add_external_read(self, event: EtwReadEvent) -> None:
        self.external_read_bytes += max(0, int(event.size))
        self.external_read_count += 1
        if event.process:
            self.external_processes.add(event.process)

    @property
    def duration(self) -> float:
        return max(0.0, self.last_seen - self.first_seen)

    @property
    def uploaded_delta(self) -> int:
        if self.uploaded_start is None:
            return 0
        return max(0, self.uploaded_last - self.uploaded_start)

    @property
    def average_speed(self) -> float:
        return fmean(self.speed_samples) if self.speed_samples else 0.0

    @property
    def max_speed(self) -> int:
        return max(self.speed_samples) if self.speed_samples else 0

    def low_speed_ratio(self, threshold: int) -> float:
        if not self.speed_samples:
            return 0.0
        return sum(1 for speed in self.speed_samples if speed < threshold) / len(self.speed_samples)

    def is_productive(self, config: GuardConfig) -> bool:
        return self.average_speed >= config.effective_productive_speed or self.consecutive_good_polls >= config.good_polls


class DecisionEngine:
    def __init__(self, config: GuardConfig, state: GuardState) -> None:
        self.config = config
        self.state = state
        self.sessions: dict[tuple[str, str], PeerSession] = {}
        self.burst_history: dict[str, list[tuple[float, int, int, int]]] = {}
        self.connected_burst_history: dict[str, list[dict[str, Any]]] = {}
        self.connected_poll_history: dict[str, list[tuple[float, bool]]] = {}
        activity_history = state.activity_wake_history()
        self.ip_activity_wake_history = _coerce_activity_wake_history(activity_history.get("by_ip", {}))
        self.endpoint_activity_wake_history = _coerce_activity_wake_history(activity_history.get("by_endpoint", {}))
        self.previous_active_torrents: set[str] | None = None
        self.woken_torrents: set[str] = set()
        self.near_misses_last: list[dict[str, Any]] = []

    def observe_active_torrents(self, torrent_hashes: set[str]) -> None:
        if self.previous_active_torrents is None:
            self.woken_torrents = set()
        else:
            self.woken_torrents = set(torrent_hashes) - self.previous_active_torrents
        self.previous_active_torrents = set(torrent_hashes)

    def observe(self, snapshots: list[PeerSnapshot], now: float) -> None:
        observed_endpoints: set[str] = set()
        for snapshot in snapshots:
            key = (snapshot.endpoint, snapshot.torrent_hash)
            session = self.sessions.get(key)
            if session is None:
                session = PeerSession(
                    endpoint=snapshot.endpoint,
                    ip=snapshot.ip,
                    torrent_hash=snapshot.torrent_hash,
                    torrent_name=snapshot.torrent_name,
                    first_seen=now,
                    last_seen=now,
                )
                self.sessions[key] = session
            session.observe(snapshot, now, self.config)
            if snapshot.torrent_hash in self.woken_torrents:
                session.torrent_wake_seen = True
            if snapshot.endpoint in self.state.data.get("reputation", {}):
                self.state.decay_reputation(
                    snapshot.endpoint,
                    now,
                    self.config.reputation_decay_hours * 60 * 60,
                )
                self.state.data["reputation"][snapshot.endpoint]["last_seen"] = now
            observed_endpoints.add(snapshot.endpoint)
        for endpoint in observed_endpoints:
            self._record_connected_poll(endpoint, now)

    def close_missing(self, seen: set[tuple[str, str]], now: float) -> None:
        for key, session in list(self.sessions.items()):
            if key in seen:
                continue
            self._record_closed_session(session, now)
            self.sessions.pop(key, None)

    def ingest_etw(self, event: EtwReadEvent, mapper: PathMapper) -> int:
        candidates = [
            session
            for session in self.sessions.values()
            if mapper.path_matches_session(event.path, session.torrent_hash, session.files)
        ]
        if not candidates:
            return 0
        if not event.is_qbt:
            for session in candidates:
                session.add_external_read(event)
            return 0
        confidence = CONFIDENCE["high"] if len(candidates) == 1 else CONFIDENCE["medium"]
        attributed_sizes, attribution_method = _attribute_etw_bytes(event, candidates)
        for session, attributed_size in zip(candidates, attributed_sizes):
            session.add_etw(event, confidence, len(candidates), attributed_size, attribution_method)
        return len(candidates)

    def decisions(self, now: float) -> list[BanDecision]:
        self.near_misses_last = []
        decisions: list[BanDecision] = []
        self._record_connected_bursts(now)
        decisions.extend(self._active_decisions(now))
        decisions.extend(self._burst_decisions(now))
        decisions.extend(self._connected_burst_decisions(now))
        decisions.extend(self._endpoint_activity_wake_churn_decisions(now))
        decisions.extend(self._ip_activity_wake_churn_decisions(now))
        decisions.extend(self._ip_escalation_decisions(now))
        return _dedupe_decisions(decisions)

    def record_ban_outcome(self, endpoint: str, ip: str, now: float) -> None:
        rep = self.state.get_reputation(endpoint)
        rep["last_ban_time"] = now
        ip_rep = self.state.ip_reputation(ip)
        _append_unique(ip_rep["distinct_endpoints"], endpoint)
        _append_unique(ip_rep["distinct_bad_endpoints"], endpoint)
        ip_rep["last_bad_seen"] = now
        self.state.save()

    def _active_decisions(self, now: float) -> list[BanDecision]:
        grouped: dict[str, list[PeerSession]] = {}
        for session in self.sessions.values():
            grouped.setdefault(session.endpoint, []).append(session)

        out: list[BanDecision] = []
        for endpoint, sessions in grouped.items():
            if self.state.is_permanent(endpoint) or self.state.is_banned_this_cycle(endpoint):
                continue
            decision = self._evaluate_endpoint(endpoint, sessions, now)
            if decision:
                out.append(decision)
        return out

    def _evaluate_endpoint(self, endpoint: str, sessions: list[PeerSession], now: float) -> BanDecision | None:
        ip = sessions[0].ip
        total_payload = sum(session.uploaded_delta for session in sessions)
        total_etw_bytes_raw = sum(session.etw_bytes for session in sessions)
        total_etw_bytes = sum(session.attributed_etw_bytes for session in sessions)
        total_etw_count = sum(session.etw_count for session in sessions)
        file_peer_count = max((session.file_peer_count for session in sessions), default=0)
        attribution_methods = tuple(sorted({method for session in sessions for method in session.etw_attribution_methods}))
        external_read_bytes = sum(session.external_read_bytes for session in sessions)
        external_read_count = sum(session.external_read_count for session in sessions)
        external_processes = tuple(sorted({process for session in sessions for process in session.external_processes}))
        confidence_value = max((session.confidence_value for session in sessions), default=0)
        confidence = _confidence_name(confidence_value)
        active_count = len(sessions)
        avg_speed = fmean([sample for session in sessions for sample in session.speed_samples] or [0])
        max_speed = max((session.max_speed for session in sessions), default=0)
        etw_upload_ratio = _etw_upload_ratio(total_etw_bytes, total_payload)
        low_sessions = [
            session
            for session in sessions
            if session.duration >= self.config.threshold_time
            and session.average_speed < self.config.low_speed_threshold
            and session.low_speed_ratio(self.config.low_speed_threshold) >= self.config.low_speed_ratio
        ]
        extreme_low_sessions = [
            session
            for session in sessions
            if session.duration >= self.config.extreme_low_speed_time
            and session.average_speed < self.config.extreme_low_speed_threshold
            and session.low_speed_ratio(self.config.extreme_low_speed_threshold) >= self.config.low_speed_ratio
        ]
        productive_sessions = [session for session in sessions if session.is_productive(self.config)]

        components: list[str] = []
        score = 0
        if confidence_value >= CONFIDENCE["high"]:
            score += 4
            components.append("+4 high-confidence ETW match")
        elif confidence_value >= CONFIDENCE["medium"]:
            score += 2
            components.append("+2 medium-confidence ETW match")

        if low_sessions:
            score += 3
            components.append("+3 average speed below threshold for full window")
        if extreme_low_sessions:
            score += 5
            components.append("+5 extreme low speed for long window")

        if total_payload >= self.config.min_payload:
            score += 3
            components.append("+3 non-trivial uploaded payload")

        hdd_ratio_ok = (
            etw_upload_ratio is not None
            and total_etw_bytes > 0
            and etw_upload_ratio >= self.config.min_etw_upload_ratio
        )
        if low_sessions and etw_upload_ratio is not None and total_etw_bytes > 0:
            if etw_upload_ratio >= 100:
                score += 6
                components.append(f"+6 extreme ETW/upload ratio {etw_upload_ratio:.2f}x")
            elif etw_upload_ratio >= 25:
                score += 4
                components.append(f"+4 very high ETW/upload ratio {etw_upload_ratio:.2f}x")
            elif etw_upload_ratio >= 10:
                score += 2
                components.append(f"+2 high ETW/upload ratio {etw_upload_ratio:.2f}x")
            elif etw_upload_ratio >= self.config.min_etw_upload_ratio:
                score += 1
                components.append(f"+1 ETW/upload ratio {etw_upload_ratio:.2f}x")
            elif etw_upload_ratio < 2:
                score -= 4
                components.append(f"-4 weak ETW/upload ratio {etw_upload_ratio:.2f}x")

        if active_count >= 2:
            if len(low_sessions) >= 2:
                score += 2
                components.append("+2 low-speed on 2+ torrents")
            if len({session.torrent_hash for session in sessions if session.etw_count}) >= 2:
                score += 3
                components.append("+3 ETW reads on 2+ torrents")
            if avg_speed < self.config.low_speed_threshold:
                score += 3
                components.append("+3 aggregate speed below threshold on 2+ torrents")

        rep = self.state.reputation(endpoint)
        rep_score = float(rep.get("reputation_score", 0.0) or 0.0)
        if rep_score >= self.config.reputation_threshold:
            score += 2
            components.append("+2 long-term reputation threshold")
        if self.config.allow_speed_only_bans and int(rep.get("bad_session_count", 0) or 0) >= self.config.reputation_bad_sessions:
            score += 3
            components.append("+3 repeated bad speed-only sessions")

        long_term_low_speed = (
            self.config.allow_speed_only_bans
            and bool(low_sessions)
            and confidence_value >= self.config.minimum_confidence_value
            and avg_speed < self.config.low_speed_threshold
            and float(rep.get("total_low_speed_seconds", 0.0) or 0.0) >= self.config.long_term_low_speed_time
            and int(rep.get("etw_matched_session_count", 0) or 0) >= self.config.long_term_low_speed_min_etw_sessions
            and int(rep.get("total_etw_read_bytes", 0) or 0) >= self.config.long_term_low_speed_min_etw_bytes
            and int(rep.get("total_uploaded", 0) or 0) >= self.config.long_term_low_speed_min_uploaded
            and float(rep.get("last_medium_evidence", 0.0) or 0.0) > 0
            and not productive_sessions
        )
        if long_term_low_speed:
            score += 4
            components.append("+4 long-term low-speed HDD evidence")

        if total_payload < self.config.min_payload and not long_term_low_speed:
            score -= 5
            components.append("-5 uploaded payload below minimum")
        if self.config.etw_required and confidence_value < self.config.minimum_confidence_value:
            score -= 4
            components.append("-4 required ETW confidence missing")
        if productive_sessions and total_etw_bytes == 0:
            score -= 3
            components.append("-3 productive without ETW churn")

        slow_hdd_tier_blocks = _slow_hdd_tier_blocks(
            average_speed=avg_speed,
            etw_upload_ratio=etw_upload_ratio,
            total_payload=total_payload,
            rep=rep,
            reputation_threshold=self.config.reputation_threshold,
            active_count=active_count,
            file_peer_count=file_peer_count,
        )
        safety_blocks = self._safety_blocks(
            endpoint,
            total_payload,
            confidence_value,
            productive_sessions,
            total_etw_bytes,
            etw_upload_ratio=etw_upload_ratio,
            require_hdd_ratio=bool(
                low_sessions
                and confidence_value >= self.config.minimum_confidence_value
                and not long_term_low_speed
            ),
            allow_low_payload=long_term_low_speed,
        )

        direct = (
            score >= self.config.score_threshold
            and bool(low_sessions)
            and hdd_ratio_ok
            and not slow_hdd_tier_blocks
        )
        extreme_low_speed = (
            self.config.allow_speed_only_bans
            and active_count == 1
            and bool(extreme_low_sessions)
            and total_payload >= self.config.extreme_low_speed_min_payload
            and not productive_sessions
        )
        delayed = (
            rep_score >= self.config.reputation_threshold
            and int(rep.get("bad_session_count", 0) or 0) >= self.config.reputation_bad_sessions
            and float(rep.get("last_medium_evidence", 0.0) or 0.0) > 0
            and not productive_sessions
        )
        speed_only = (
            self.config.allow_speed_only_bans
            and confidence_value < self.config.minimum_confidence_value
            and score >= self.config.speed_only_score_threshold
            and (int(rep.get("bad_session_count", 0) or 0) >= self.config.reputation_bad_sessions or active_count >= 2)
        )

        unmet_criteria = self._unmet_criteria(
            active_count=active_count,
            confidence_value=confidence_value,
            direct=direct,
            delayed=delayed,
            endpoint=endpoint,
            extreme_low_sessions=extreme_low_sessions,
            extreme_low_speed=extreme_low_speed,
            low_sessions=low_sessions,
            long_term_low_speed=long_term_low_speed,
            productive_sessions=productive_sessions,
            rep=rep,
            rep_score=rep_score,
            score=score,
            speed_only=speed_only,
            total_payload=total_payload,
            etw_upload_ratio=etw_upload_ratio,
            hdd_ratio_ok=hdd_ratio_ok,
            slow_hdd_tier_blocks=slow_hdd_tier_blocks,
        )

        if safety_blocks or not (direct or delayed or speed_only or extreme_low_speed or long_term_low_speed):
            self._record_near_miss(
                endpoint=endpoint,
                ip=ip,
                sessions=sessions,
                score=score,
                confidence=confidence,
                confidence_value=confidence_value,
                average_speed=avg_speed,
                max_speed=max_speed,
                total_payload=total_payload,
                total_etw_bytes=total_etw_bytes,
                total_etw_bytes_raw=total_etw_bytes_raw,
                total_etw_count=total_etw_count,
                etw_upload_ratio=etw_upload_ratio,
                active_count=active_count,
                file_peer_count=file_peer_count,
                attribution_methods=attribution_methods,
                external_read_bytes=external_read_bytes,
                external_read_count=external_read_count,
                external_processes=external_processes,
                components=components,
                safety_blocks=safety_blocks,
                unmet_criteria=unmet_criteria,
                rep=rep,
                rep_score=rep_score,
                low_sessions=low_sessions,
                extreme_low_sessions=extreme_low_sessions,
            )
            return None

        reason = "slow-hdd-churn"
        if delayed:
            reason = "long-term-reputation"
        if speed_only:
            reason = "speed-only-fallback"
        if extreme_low_speed:
            reason = "extreme-low-speed"
        if long_term_low_speed:
            reason = "long-term-low-speed"

        return BanDecision(
            subject=endpoint,
            endpoint=endpoint,
            ip=ip,
            torrent_hashes=tuple(sorted({session.torrent_hash for session in sessions})),
            reason=reason,
            score=score,
            confidence=confidence,
            average_speed=avg_speed,
            max_speed=max_speed,
            uploaded_delta=total_payload,
            etw_bytes=total_etw_bytes,
            raw_etw_bytes=total_etw_bytes_raw,
            etw_count=total_etw_count,
            active_torrent_count=active_count,
            score_components=tuple(components),
            file_peer_count=file_peer_count,
            etw_attribution_methods=attribution_methods,
            external_read_bytes=external_read_bytes,
            external_read_count=external_read_count,
            external_process_count=len(external_processes),
            external_processes=external_processes,
            etw_upload_ratio=etw_upload_ratio,
        )

    def _burst_decisions(self, now: float) -> list[BanDecision]:
        out: list[BanDecision] = []
        for endpoint, events in list(self.burst_history.items()):
            events = [event for event in events if now - event[0] <= self.config.burst_window]
            self.burst_history[endpoint] = events
            if len(events) < self.config.burst_count:
                continue
            total_payload = sum(event[1] for event in events)
            if total_payload < self.config.burst_min_total_payload:
                continue
            confidence_value = max(event[2] for event in events)
            if confidence_value < self.config.minimum_confidence_value:
                continue
            if self.state.is_permanent(endpoint) or self.state.is_banned_this_cycle(endpoint):
                continue
            rep = self.state.reputation(endpoint)
            out.append(
                BanDecision(
                    subject=endpoint,
                    endpoint=endpoint,
                    ip=str(rep.get("ip", "")),
                    torrent_hashes=(),
                    reason="burst-reconnect-hdd-churn",
                    score=8,
                    confidence=_confidence_name(confidence_value),
                    average_speed=0.0,
                    max_speed=0,
                    uploaded_delta=total_payload,
                    etw_bytes=sum(event[3] for event in events),
                    raw_etw_bytes=sum(event[3] for event in events),
                    etw_count=len(events),
                    active_torrent_count=0,
                    score_components=("+3 burst sessions with payload and ETW match", "+2 repeated burst sessions"),
                )
            )
        return out

    def _connected_burst_decisions(self, now: float) -> list[BanDecision]:
        out: list[BanDecision] = []
        for endpoint, events in list(self.connected_burst_history.items()):
            events = [event for event in events if now - float(event.get("ts", 0.0) or 0.0) <= self.config.connected_burst_window]
            self.connected_burst_history[endpoint] = events
            if len(events) < self.config.connected_burst_count:
                continue
            total_payload = sum(int(event.get("payload", 0) or 0) for event in events)
            if total_payload < self.config.connected_burst_min_total_payload:
                continue
            confidence_value = max(int(event.get("confidence", 0) or 0) for event in events)
            if confidence_value < self.config.minimum_confidence_value:
                continue
            duty_ratio = self._connected_burst_duty_ratio(endpoint, now)
            if duty_ratio > self.config.connected_burst_max_duty_ratio:
                continue
            average_speed = fmean([float(event.get("average_speed", 0.0) or 0.0) for event in events])
            max_speed = max(int(event.get("max_speed", 0) or 0) for event in events)
            if average_speed >= self.config.effective_connected_burst_max_average_speed:
                continue
            if self.state.is_permanent(endpoint) or self.state.is_banned_this_cycle(endpoint):
                continue
            rep = self.state.get_reputation(endpoint)
            productive_count = int(rep.get("recent_productive_session_count", 0) or 0)
            if productive_count:
                continue
            ip = str(rep.get("ip", "") or _last_event_value(events, "ip"))
            torrent_hashes = sorted({str(event.get("torrent_hash", "")) for event in events if event.get("torrent_hash")})
            out.append(
                BanDecision(
                    subject=endpoint,
                    endpoint=endpoint,
                    ip=ip,
                    torrent_hashes=tuple(torrent_hashes),
                    reason="connected-burst-hdd-churn",
                    score=8,
                    confidence=_confidence_name(confidence_value),
                    average_speed=average_speed,
                    max_speed=max_speed,
                    uploaded_delta=total_payload,
                    etw_bytes=sum(int(event.get("etw_bytes", 0) or 0) for event in events),
                    raw_etw_bytes=sum(int(event.get("raw_etw_bytes", event.get("etw_bytes", 0)) or 0) for event in events),
                    etw_count=len(events),
                    active_torrent_count=len(torrent_hashes),
                    score_components=(
                        "+3 connected burst payload with ETW match",
                        f"connected burst duty ratio={duty_ratio:.2f}",
                    ),
                )
            )
        return out

    def _record_connected_bursts(self, now: float) -> None:
        recorded: set[tuple[str, str]] = set()
        changed = False
        for session in self.sessions.values():
            key = (session.endpoint, session.torrent_hash)
            if key in recorded:
                continue
            recorded.add(key)
            if session.last_poll_uploaded_delta < self.config.connected_burst_min_payload:
                continue
            if session.last_poll_confidence_value < self.config.minimum_confidence_value:
                continue
            self.connected_burst_history.setdefault(session.endpoint, []).append(
                {
                    "ts": now,
                    "payload": session.last_poll_uploaded_delta,
                    "confidence": session.last_poll_confidence_value,
                        "etw_bytes": session.last_poll_etw_bytes,
                        "raw_etw_bytes": session.last_poll_raw_etw_bytes,
                        "ip": session.ip,
                        "torrent_hash": session.torrent_hash,
                        "average_speed": session.average_speed,
                        "max_speed": session.max_speed,
                    }
                )
            self._mark_connected_burst_poll(session.endpoint, now)
            rep = self.state.reputation(session.endpoint)
            rep["connected_burst_session_count"] = int(rep.get("connected_burst_session_count", 0) or 0) + 1
            rep["reputation_score"] = float(rep.get("reputation_score", 0.0) or 0.0) + 1
            changed = True
        if changed:
            self.state.save()

    def _record_connected_poll(self, endpoint: str, now: float) -> None:
        history = [
            item
            for item in self.connected_poll_history.get(endpoint, [])
            if now - item[0] <= self.config.connected_burst_window
        ]
        if not history or history[-1][0] != now:
            history.append((now, False))
        self.connected_poll_history[endpoint] = history

    def _mark_connected_burst_poll(self, endpoint: str, now: float) -> None:
        history = self.connected_poll_history.setdefault(endpoint, [])
        for index in range(len(history) - 1, -1, -1):
            if history[index][0] == now:
                history[index] = (history[index][0], True)
                return
        history.append((now, True))

    def _connected_burst_duty_ratio(self, endpoint: str, now: float) -> float:
        history = [
            item
            for item in self.connected_poll_history.get(endpoint, [])
            if now - item[0] <= self.config.connected_burst_window
        ]
        self.connected_poll_history[endpoint] = history
        if not history:
            return 0.0
        return sum(1 for _, is_burst in history if is_burst) / len(history)

    def _ip_escalation_decisions(self, now: float) -> list[BanDecision]:
        if self.config.bare_ip_bad_endpoint_count <= 0:
            return []
        out: list[BanDecision] = []
        for ip, rep in self.state.data.get("ip_reputation", {}).items():
            bad = list(rep.get("distinct_bad_endpoints", []))
            if len(bad) < self.config.bare_ip_bad_endpoint_count:
                continue
            if self.state.is_permanent(ip):
                continue
            out.append(
                BanDecision(
                    subject=ip,
                    endpoint="",
                    ip=ip,
                    torrent_hashes=(),
                    reason="bare-ip-escalation",
                    score=8,
                    confidence="medium",
                    average_speed=0.0,
                    max_speed=0,
                    uploaded_delta=int(rep.get("total_uploaded", 0) or 0),
                    etw_bytes=int(rep.get("total_etw_read_bytes", 0) or 0),
                    etw_count=int(rep.get("total_etw_matched_sessions", 0) or 0),
                    active_torrent_count=0,
                    score_components=(f"bare IP has {len(bad)} distinct bad endpoints",),
                )
            )
        return out

    def _record_closed_session(self, session: PeerSession, now: float) -> None:
        low = (
            session.duration >= self.config.threshold_time
            and session.average_speed < self.config.low_speed_threshold
            and session.low_speed_ratio(self.config.low_speed_threshold) >= self.config.low_speed_ratio
        )
        productive = session.is_productive(self.config)
        confidence_ok = session.confidence_value >= self.config.minimum_confidence_value
        bad = low and session.uploaded_delta >= self.config.min_payload and (
            confidence_ok or self.config.allow_speed_only_bans
        )

        if (
            session.duration <= self.config.short_session_max
            and session.uploaded_delta >= self.config.burst_min_payload
            and confidence_ok
        ):
            burst = True
        else:
            burst = False

        activity_wake = self._record_ip_activity_wake_event(session, now)

        if not (
            productive
            or session.etw_count
            or bad
            or burst
            or activity_wake
            or session.uploaded_delta >= self.config.min_payload
        ):
            return

        rep = self.state.reputation(session.endpoint)
        rep["ip"] = session.ip
        rep["session_count"] = int(rep.get("session_count", 0) or 0) + 1
        rep["last_seen"] = now
        rep["total_uploaded"] = int(rep.get("total_uploaded", 0) or 0) + session.uploaded_delta
        rep["total_etw_read_bytes"] = int(rep.get("total_etw_read_bytes", 0) or 0) + session.attributed_etw_bytes
        rep["total_connected_seconds"] = float(rep.get("total_connected_seconds", 0.0) or 0.0) + session.duration

        if low:
            rep["low_speed_session_count"] = int(rep.get("low_speed_session_count", 0) or 0) + 1
            rep["total_low_speed_seconds"] = float(rep.get("total_low_speed_seconds", 0.0) or 0.0) + session.duration
            rep["reputation_score"] = float(rep.get("reputation_score", 0.0) or 0.0) + 2
        if session.etw_count:
            rep["etw_matched_session_count"] = int(rep.get("etw_matched_session_count", 0) or 0) + 1
            rep["reputation_score"] = float(rep.get("reputation_score", 0.0) or 0.0) + 2
            if session.confidence_value >= CONFIDENCE["medium"]:
                rep["last_medium_evidence"] = now
        if bad:
            rep["bad_session_count"] = int(rep.get("bad_session_count", 0) or 0) + 1
            rep["reputation_score"] = float(rep.get("reputation_score", 0.0) or 0.0) + 3
            self._record_bad_ip(session)
        if productive:
            rep["recent_productive_session_count"] = int(rep.get("recent_productive_session_count", 0) or 0) + 1
            rep["last_productive_seen"] = now
            rep["reputation_score"] = max(0.0, float(rep.get("reputation_score", 0.0) or 0.0) - 2)

        if burst:
            rep["burst_session_count"] = int(rep.get("burst_session_count", 0) or 0) + 1
            rep["reputation_score"] = float(rep.get("reputation_score", 0.0) or 0.0) + 3
            self.burst_history.setdefault(session.endpoint, []).append(
                (now, session.uploaded_delta, session.confidence_value, session.attributed_etw_bytes)
            )
            self._record_bad_ip(session)

        if activity_wake:
            rep["activity_wake_churn_session_count"] = int(rep.get("activity_wake_churn_session_count", 0) or 0) + 1
            rep["reputation_score"] = float(rep.get("reputation_score", 0.0) or 0.0) + 1

        self.state.save()

    def _record_ip_activity_wake_event(self, session: PeerSession, now: float) -> bool:
        if self.config.activity_wake_churn_after <= 0 and self.config.ip_activity_wake_churn_after <= 0:
            return False
        if session.duration > self.config.ip_activity_wake_churn_max_session_time:
            return False
        if session.uploaded_delta > self.config.ip_activity_wake_churn_max_single_payload:
            return False
        peer_etw_evidence = _session_has_activity_wake_etw_evidence(session)
        payload_evidence = session.uploaded_delta > 0
        speed_evidence = session.max_speed >= self.config.ip_activity_wake_churn_min_speed
        tiny_burst = payload_evidence and speed_evidence
        torrent_wake = session.torrent_wake_seen and (
            payload_evidence or peer_etw_evidence or speed_evidence
        )
        if not (tiny_burst or torrent_wake):
            return False
        activity_evidence = []
        if payload_evidence:
            activity_evidence.append("uploaded-delta")
        if peer_etw_evidence:
            activity_evidence.append("etw-attribution")
        if speed_evidence:
            activity_evidence.append("speed-sample")
        history = [
            item
            for item in self.ip_activity_wake_history.get(session.ip, [])
            if now - float(item.get("ts", 0.0) or 0.0) <= self.config.ip_activity_wake_churn_window
        ]
        endpoint_history = [
            item
            for item in self.endpoint_activity_wake_history.get(session.endpoint, [])
            if now - float(item.get("ts", 0.0) or 0.0) <= self.config.ip_activity_wake_churn_window
        ]
        wake_weight, weight_components = _activity_wake_session_weight(
            session=session,
            endpoint_history=endpoint_history,
            ip_history=history,
            peer_etw_evidence=peer_etw_evidence,
            payload_evidence=payload_evidence,
            speed_evidence=speed_evidence,
            torrent_wake=torrent_wake,
        )
        if wake_weight <= 0:
            return False
        event = {
            "ts": now,
            "start": session.first_seen,
            "end": session.last_seen,
            "duration": session.duration,
            "endpoint": session.endpoint,
            "ip": session.ip,
            "port": _endpoint_port(session.endpoint),
            "torrent_hash": session.torrent_hash,
            "payload": session.uploaded_delta,
            "average_speed": session.average_speed,
            "max_speed": session.max_speed,
            "etw_bytes": session.attributed_etw_bytes,
            "raw_etw_bytes": session.etw_bytes,
            "etw_count": session.etw_count,
            "etw_evidence": peer_etw_evidence,
            "activity_evidence": activity_evidence,
            "activity_weight_components": weight_components,
            "raw_torrent_wake": session.torrent_wake_seen,
            "torrent_wake": torrent_wake,
            "tiny_burst": tiny_burst,
            "wake_weight": wake_weight,
            "lone_peer_etw": peer_etw_evidence and session.file_peer_count <= 1,
            "file_peer_count": session.file_peer_count,
            "etw_attribution_methods": sorted(session.etw_attribution_methods),
        }
        history.append(event)
        self.ip_activity_wake_history[session.ip] = history
        endpoint_history.append(event)
        self.endpoint_activity_wake_history[session.endpoint] = endpoint_history
        self._persist_activity_wake_history()
        return True

    def _endpoint_activity_wake_churn_decisions(self, now: float) -> list[BanDecision]:
        if self.config.activity_wake_churn_after <= 0:
            return []
        out: list[BanDecision] = []
        for endpoint, events in list(self.endpoint_activity_wake_history.items()):
            previous_len = len(events)
            events = [
                event
                for event in events
                if now - float(event.get("ts", 0.0) or 0.0) <= self.config.ip_activity_wake_churn_window
            ]
            self.endpoint_activity_wake_history[endpoint] = events
            if len(events) != previous_len:
                self._persist_activity_wake_history()
            event_weight = _activity_wake_events_weight(events)
            if event_weight < self.config.activity_wake_churn_after:
                continue
            total_payload = sum(int(event.get("payload", 0) or 0) for event in events)
            if total_payload > self.config.ip_activity_wake_churn_max_total_payload:
                continue
            if self.state.is_permanent(endpoint) or self.state.is_banned_this_cycle(endpoint):
                continue
            ip = str(_last_event_value(events, "ip") or _endpoint_ip(endpoint))
            rep = self.state.get_reputation(endpoint)
            if int(rep.get("recent_productive_session_count", 0) or 0) > 0:
                continue
            out.append(
                self._activity_wake_decision(
                    subject=endpoint,
                    endpoint=endpoint,
                    ip=ip,
                    reason="activity-wake-churn",
                    events=events,
                    score_components_prefix=f"endpoint activity-wake churn weight={event_weight}",
                )
            )
        return out

    def _ip_activity_wake_churn_decisions(self, now: float) -> list[BanDecision]:
        if self.config.ip_activity_wake_churn_after <= 0:
            return []
        out: list[BanDecision] = []
        for ip, events in list(self.ip_activity_wake_history.items()):
            previous_len = len(events)
            events = [
                event
                for event in events
                if now - float(event.get("ts", 0.0) or 0.0) <= self.config.ip_activity_wake_churn_window
            ]
            self.ip_activity_wake_history[ip] = events
            if len(events) != previous_len:
                self._persist_activity_wake_history()
            event_weight = _activity_wake_events_weight(events)
            ports = sorted({int(event.get("port", 0) or 0) for event in events if int(event.get("port", 0) or 0) > 0})
            total_payload = sum(int(event.get("payload", 0) or 0) for event in events)
            all_distinct_ports = len(ports) == len(events) and len(ports) > 0
            all_distinct_shortcut = (
                self.config.ip_activity_wake_churn_all_distinct_ports_over > 0
                and all_distinct_ports
                and len(ports) > self.config.ip_activity_wake_churn_all_distinct_ports_over
            )
            if (
                (event_weight < self.config.ip_activity_wake_churn_after and not all_distinct_shortcut)
                or len(ports) < self.config.ip_activity_wake_churn_distinct_ports
                or total_payload > self.config.ip_activity_wake_churn_max_total_payload
            ):
                self._record_ip_activity_wake_near_miss(
                    ip,
                    events,
                    event_weight,
                    ports,
                    total_payload,
                    all_distinct_ports=all_distinct_ports,
                    all_distinct_shortcut=all_distinct_shortcut,
                )
                continue
            if self.state.is_permanent(ip) or self.state.is_banned_this_cycle(ip):
                continue
            ip_rep = self.state.get_ip_reputation(ip)
            if int(ip_rep.get("recent_productive_session_count", 0) or 0) > 0:
                continue
            out.append(
                self._activity_wake_decision(
                    subject=ip,
                    endpoint="",
                    ip=ip,
                    reason="ip-activity-wake-churn",
                    events=events,
                    score_components_prefix=f"IP activity-wake churn weight={event_weight}",
                    extra_score_components=(
                        f"distinct ports={len(ports)}",
                    ),
                    extra_details={
                        "ip_churn_event_count": len(events),
                        "ip_churn_weighted_event_count": event_weight,
                        "ip_churn_distinct_ports": len(ports),
                        "ip_churn_total_payload": total_payload,
                        "ip_churn_window_seconds": self.config.ip_activity_wake_churn_window,
                        "ip_churn_all_distinct_ports": all_distinct_ports,
                        "ip_churn_all_distinct_ports_over": self.config.ip_activity_wake_churn_all_distinct_ports_over,
                        "ip_churn_all_distinct_shortcut": all_distinct_shortcut,
                    },
                )
            )
        return out

    def _record_ip_activity_wake_near_miss(
        self,
        ip: str,
        events: list[dict[str, Any]],
        event_weight: int,
        ports: list[int],
        total_payload: int,
        all_distinct_ports: bool,
        all_distinct_shortcut: bool,
    ) -> None:
        zero_upload_etw_events = _activity_wake_zero_upload_etw_events(events)
        if zero_upload_etw_events <= 0:
            return
        near_threshold = max(1, self.config.ip_activity_wake_churn_after - 10)
        if event_weight < near_threshold and len(ports) < self.config.ip_activity_wake_churn_distinct_ports:
            return

        stats = _activity_wake_event_stats(events)
        unmet: list[str] = []
        if event_weight < self.config.ip_activity_wake_churn_after:
            unmet.append(
                "IP activity-wake weighted event count below threshold "
                f"({event_weight} < {self.config.ip_activity_wake_churn_after})"
            )
        if len(ports) < self.config.ip_activity_wake_churn_distinct_ports:
            unmet.append(
                "IP activity-wake distinct ports below threshold "
                f"({len(ports)} < {self.config.ip_activity_wake_churn_distinct_ports})"
            )
        if self.config.ip_activity_wake_churn_all_distinct_ports_over > 0 and not all_distinct_shortcut:
            if not all_distinct_ports:
                unmet.append("IP activity-wake all-distinct-port shortcut requires every counted event to use a distinct port")
            elif len(ports) <= self.config.ip_activity_wake_churn_all_distinct_ports_over:
                unmet.append(
                    "IP activity-wake all-distinct-port count below shortcut threshold "
                    f"({len(ports)} <= {self.config.ip_activity_wake_churn_all_distinct_ports_over})"
                )
        if total_payload > self.config.ip_activity_wake_churn_max_total_payload:
            unmet.append(
                "IP activity-wake payload above tiny-payload cap "
                f"({total_payload} > {self.config.ip_activity_wake_churn_max_total_payload})"
            )

        self.near_misses_last.append(
            {
                "subject": ip,
                "endpoint": "",
                "ip": ip,
                "reason": "ip-activity-wake-churn-near-miss",
                "torrent_hashes": stats["torrent_hashes"],
                "score": event_weight,
                "near_score_threshold": self.config.ip_activity_wake_churn_after,
                "confidence": "medium",
                "confidence_value": CONFIDENCE["medium"],
                "average_speed_bps": stats["average_speed"],
                "max_speed_bps": stats["max_speed"],
                "uploaded_delta_bytes": total_payload,
                "etw_read_bytes": stats["etw_bytes"],
                "etw_read_bytes_attributed": stats["etw_bytes"],
                "etw_read_bytes_raw": stats["raw_etw_bytes"],
                "etw_upload_ratio": None,
                "etw_read_count": stats["etw_count"],
                "active_torrent_count": len(stats["torrent_hashes"]),
                "file_peer_count": 0,
                "score_components": [
                    f"IP activity-wake weighted events={event_weight}",
                    f"zero-upload ETW events={zero_upload_etw_events}",
                    f"distinct ports={len(ports)}",
                ],
                "safety_blocks": [],
                "unmet_criteria": unmet,
                "decision_details": {
                    "activity_wake_event_count": len(events),
                    "activity_wake_confidence": _activity_wake_confidence(events),
                    "activity_wake_weighted_event_count": event_weight,
                    "activity_wake_weight_components": _activity_wake_weight_components(events),
                    "activity_wake_zero_upload_etw_events": zero_upload_etw_events,
                    "activity_wake_total_payload": total_payload,
                    "activity_wake_etw_read_bytes": stats["etw_bytes"],
                    "activity_wake_lone_peer_etw_events": sum(1 for event in events if event.get("lone_peer_etw")),
                    "activity_wake_shared_etw_events": sum(
                        1
                        for event in events
                        if int(event.get("etw_bytes", 0) or 0) > 0 and not event.get("lone_peer_etw")
                    ),
                    "ip_churn_distinct_ports": len(ports),
                    "ip_churn_ports": ports,
                    "ip_churn_endpoints": stats["endpoints"],
                    "ip_churn_torrent_hashes": stats["torrent_hashes"],
                    "ip_churn_window_seconds": self.config.ip_activity_wake_churn_window,
                    "ip_churn_all_distinct_ports": all_distinct_ports,
                    "ip_churn_all_distinct_ports_over": self.config.ip_activity_wake_churn_all_distinct_ports_over,
                    "ip_churn_all_distinct_shortcut": all_distinct_shortcut,
                },
            }
        )

    def _persist_activity_wake_history(self) -> None:
        self.state.set_activity_wake_history(
            by_endpoint=self.endpoint_activity_wake_history,
            by_ip=self.ip_activity_wake_history,
        )

    def _activity_wake_decision(
        self,
        *,
        subject: str,
        endpoint: str,
        ip: str,
        reason: str,
        events: list[dict[str, Any]],
        score_components_prefix: str,
        extra_score_components: tuple[str, ...] = (),
        extra_details: dict[str, Any] | None = None,
    ) -> BanDecision:
        endpoints = sorted({str(event.get("endpoint", "")) for event in events if event.get("endpoint")})
        torrent_hashes = sorted({str(event.get("torrent_hash", "")) for event in events if event.get("torrent_hash")})
        total_payload = sum(int(event.get("payload", 0) or 0) for event in events)
        max_speed = max(int(event.get("max_speed", 0) or 0) for event in events)
        avg_speed = fmean([float(event.get("average_speed", 0.0) or 0.0) for event in events])
        etw_bytes = sum(int(event.get("etw_bytes", 0) or 0) for event in events)
        raw_etw_bytes = sum(int(event.get("raw_etw_bytes", 0) or 0) for event in events)
        details = {
            "activity_wake_event_count": len(events),
            "activity_wake_confidence": _activity_wake_confidence(events),
            "activity_wake_weighted_event_count": _activity_wake_events_weight(events),
            "activity_wake_weight_components": _activity_wake_weight_components(events),
            "activity_wake_total_payload": total_payload,
            "activity_wake_window_seconds": self.config.ip_activity_wake_churn_window,
            "activity_wake_max_speed": max_speed,
            "activity_wake_endpoints": endpoints,
            "activity_wake_torrent_hashes": torrent_hashes,
            "activity_wake_torrent_wake_events": sum(1 for event in events if event.get("torrent_wake")),
            "activity_wake_tiny_burst_events": sum(1 for event in events if event.get("tiny_burst")),
            "activity_wake_peer_evidence_events": sum(1 for event in events if event.get("activity_evidence")),
            "activity_wake_lone_peer_etw_events": sum(1 for event in events if event.get("lone_peer_etw")),
            "activity_wake_shared_etw_events": sum(
                1
                for event in events
                if int(event.get("etw_bytes", 0) or 0) > 0 and not event.get("lone_peer_etw")
            ),
            "activity_wake_zero_upload_etw_events": sum(
                1
                for event in events
                if int(event.get("payload", 0) or 0) <= 0 and _activity_wake_event_has_etw_evidence(event)
            ),
        }
        if extra_details:
            details.update(extra_details)
        components = (
            score_components_prefix,
            f"total payload={total_payload}",
            f"max speed={max_speed}B/s",
            *extra_score_components,
        )
        return BanDecision(
            subject=subject,
            endpoint=endpoint,
            ip=ip,
            torrent_hashes=tuple(torrent_hashes),
            reason=reason,
            score=8,
            confidence="low",
            average_speed=avg_speed,
            max_speed=max_speed,
            uploaded_delta=total_payload,
            etw_bytes=etw_bytes,
            raw_etw_bytes=raw_etw_bytes,
            etw_count=sum(int(event.get("etw_count", 0) or 0) for event in events),
            active_torrent_count=len(torrent_hashes),
            score_components=components,
            details=details,
        )

    def _record_bad_ip(self, session: PeerSession) -> None:
        ip_rep = self.state.ip_reputation(session.ip)
        _append_unique(ip_rep["distinct_endpoints"], session.endpoint)
        _append_unique(ip_rep["distinct_bad_endpoints"], session.endpoint)
        ip_rep["total_bad_sessions"] = int(ip_rep.get("total_bad_sessions", 0) or 0) + 1
        ip_rep["total_uploaded"] = int(ip_rep.get("total_uploaded", 0) or 0) + session.uploaded_delta
        ip_rep["total_etw_read_bytes"] = int(ip_rep.get("total_etw_read_bytes", 0) or 0) + session.attributed_etw_bytes
        if session.etw_count:
            ip_rep["total_etw_matched_sessions"] = int(ip_rep.get("total_etw_matched_sessions", 0) or 0) + 1
        ip_rep["last_bad_seen"] = session.last_seen
        ip_rep["ip_reputation_score"] = float(ip_rep.get("ip_reputation_score", 0.0) or 0.0) + 1

    def _safety_blocks(
        self,
        endpoint: str,
        total_payload: int,
        confidence_value: int,
        productive_sessions: list[PeerSession],
        total_etw_bytes: int,
        *,
        etw_upload_ratio: float | None = None,
        require_hdd_ratio: bool = False,
        allow_low_payload: bool = False,
    ) -> list[str]:
        blocks: list[str] = []
        if total_payload < self.config.min_payload and not allow_low_payload:
            blocks.append(f"uploaded payload below --min-payload ({total_payload} < {self.config.min_payload})")
        if self.config.etw_required and confidence_value < self.config.minimum_confidence_value:
            blocks.append(
                f"ETW confidence below --required-confidence ({_confidence_name(confidence_value)} < {self.config.required_confidence})"
            )
        if not self.config.etw_required and confidence_value < self.config.minimum_confidence_value:
            if not self.config.allow_speed_only_bans:
                blocks.append("ETW confidence missing and speed-only bans disabled")
        if productive_sessions and total_etw_bytes == 0:
            blocks.append("peer was productive and no ETW churn followed")
        if (
            require_hdd_ratio
            and self.config.min_etw_upload_ratio > 0
            and total_etw_bytes > 0
            and (
                etw_upload_ratio is None
                or etw_upload_ratio < self.config.min_etw_upload_ratio
            )
        ):
            ratio_text = "none" if etw_upload_ratio is None else f"{etw_upload_ratio:.2f}x"
            blocks.append(
                "ETW/upload ratio below --min-etw-upload-ratio "
                f"({ratio_text} < {self.config.min_etw_upload_ratio:.2f}x)"
            )
        if self.state.is_permanent(endpoint):
            blocks.append("subject is already permanent")
        return blocks

    def _unmet_criteria(
        self,
        *,
        active_count: int,
        confidence_value: int,
        direct: bool,
        delayed: bool,
        endpoint: str,
        extreme_low_sessions: list[PeerSession],
        extreme_low_speed: bool,
        low_sessions: list[PeerSession],
        long_term_low_speed: bool,
        productive_sessions: list[PeerSession],
        rep: dict[str, Any],
        rep_score: float,
        score: int,
        speed_only: bool,
        total_payload: int,
        etw_upload_ratio: float | None,
        hdd_ratio_ok: bool,
        slow_hdd_tier_blocks: list[str],
    ) -> list[str]:
        unmet: list[str] = []
        if not direct:
            if score < self.config.score_threshold:
                unmet.append(f"slow-hdd score below threshold ({score} < {self.config.score_threshold})")
            if not low_sessions:
                unmet.append("no session met normal low-speed duration/ratio")
            if low_sessions and not hdd_ratio_ok:
                if etw_upload_ratio is None:
                    unmet.append("no ETW/upload ratio available for slow HDD-churn rule")
                else:
                    unmet.append(
                        "ETW/upload ratio below threshold "
                        f"({etw_upload_ratio:.2f}x < {self.config.min_etw_upload_ratio:.2f}x)"
                    )
            unmet.extend(slow_hdd_tier_blocks)
        if not speed_only:
            if not self.config.allow_speed_only_bans:
                unmet.append("speed-only bans disabled")
            if confidence_value >= self.config.minimum_confidence_value:
                unmet.append("speed-only fallback skipped because ETW confidence was available")
            if score < self.config.speed_only_score_threshold:
                unmet.append(f"speed-only score below threshold ({score} < {self.config.speed_only_score_threshold})")
            if int(rep.get("bad_session_count", 0) or 0) < self.config.reputation_bad_sessions and active_count < 2:
                unmet.append("speed-only needs repeated bad sessions or 2+ active torrents")
        if not extreme_low_speed:
            if active_count != 1:
                unmet.append("extreme-low-speed requires exactly one active torrent")
            if not extreme_low_sessions:
                unmet.append("no session met extreme low-speed duration/ratio")
            if total_payload < self.config.extreme_low_speed_min_payload:
                unmet.append(
                    f"extreme-low payload below threshold ({total_payload} < {self.config.extreme_low_speed_min_payload})"
                )
            if productive_sessions:
                unmet.append("extreme-low blocked by productive behavior")
        if not delayed:
            if rep_score < self.config.reputation_threshold:
                unmet.append(f"reputation score below threshold ({rep_score:.2f} < {self.config.reputation_threshold})")
            if int(rep.get("bad_session_count", 0) or 0) < self.config.reputation_bad_sessions:
                unmet.append(
                    f"bad session count below threshold ({int(rep.get('bad_session_count', 0) or 0)} < {self.config.reputation_bad_sessions})"
                )
            if not float(rep.get("last_medium_evidence", 0.0) or 0.0):
                unmet.append("no recent medium/high ETW evidence in reputation")
        if not long_term_low_speed:
            if not low_sessions:
                unmet.append("long-term low-speed needs current low-speed threshold window")
            if confidence_value < self.config.minimum_confidence_value:
                unmet.append(
                    "long-term low-speed needs current ETW confidence "
                    f"({_confidence_name(confidence_value)} < {self.config.required_confidence})"
                )
            if float(rep.get("total_low_speed_seconds", 0.0) or 0.0) < self.config.long_term_low_speed_time:
                unmet.append(
                    "long-term low-speed time below threshold "
                    f"({float(rep.get('total_low_speed_seconds', 0.0) or 0.0):.1f} < {self.config.long_term_low_speed_time})"
                )
            if int(rep.get("etw_matched_session_count", 0) or 0) < self.config.long_term_low_speed_min_etw_sessions:
                unmet.append(
                    "long-term ETW session count below threshold "
                    f"({int(rep.get('etw_matched_session_count', 0) or 0)} < {self.config.long_term_low_speed_min_etw_sessions})"
                )
            if int(rep.get("total_etw_read_bytes", 0) or 0) < self.config.long_term_low_speed_min_etw_bytes:
                unmet.append(
                    "long-term ETW bytes below threshold "
                    f"({int(rep.get('total_etw_read_bytes', 0) or 0)} < {self.config.long_term_low_speed_min_etw_bytes})"
                )
            if int(rep.get("total_uploaded", 0) or 0) < self.config.long_term_low_speed_min_uploaded:
                unmet.append(
                    "long-term uploaded bytes below threshold "
                    f"({int(rep.get('total_uploaded', 0) or 0)} < {self.config.long_term_low_speed_min_uploaded})"
                )
        return unmet

    def _record_near_miss(
        self,
        *,
        endpoint: str,
        ip: str,
        sessions: list[PeerSession],
        score: int,
        confidence: str,
        confidence_value: int,
        average_speed: float,
        max_speed: int,
        total_payload: int,
        total_etw_bytes: int,
        total_etw_bytes_raw: int,
        total_etw_count: int,
        etw_upload_ratio: float | None,
        active_count: int,
        file_peer_count: int,
        attribution_methods: tuple[str, ...],
        external_read_bytes: int,
        external_read_count: int,
        external_processes: tuple[str, ...],
        components: list[str],
        safety_blocks: list[str],
        unmet_criteria: list[str],
        rep: dict[str, Any],
        rep_score: float,
        low_sessions: list[PeerSession],
        extreme_low_sessions: list[PeerSession],
    ) -> None:
        near_score = min(self.config.score_threshold, self.config.speed_only_score_threshold) - 2
        meaningful_signal = (
            low_sessions
            or extreme_low_sessions
            or (
                active_count >= 2
                and average_speed < self.config.low_speed_threshold
                and total_payload >= self.config.burst_min_payload
            )
            or (
                confidence_value >= self.config.minimum_confidence_value
                and total_payload >= self.config.connected_burst_min_payload
                and average_speed < self.config.low_speed_threshold
            )
        )
        if not meaningful_signal:
            return
        self.near_misses_last.append(
            {
                "subject": endpoint,
                "endpoint": endpoint,
                "ip": ip,
                "torrent_hashes": sorted({session.torrent_hash for session in sessions}),
                "score": score,
                "near_score_threshold": near_score,
                "confidence": confidence,
                "confidence_value": confidence_value,
                "average_speed_bps": average_speed,
                "max_speed_bps": max_speed,
                "uploaded_delta_bytes": total_payload,
                "etw_read_bytes": total_etw_bytes,
                "etw_read_bytes_attributed": total_etw_bytes,
                "etw_read_bytes_raw": total_etw_bytes_raw,
                "etw_upload_ratio": etw_upload_ratio,
                "etw_attribution_methods": list(attribution_methods),
                "etw_read_count": total_etw_count,
                "active_torrent_count": active_count,
                "file_peer_count": file_peer_count,
                "external_read_bytes": external_read_bytes,
                "external_read_count": external_read_count,
                "external_process_count": len(external_processes),
                "external_processes": list(external_processes),
                "score_components": list(components),
                "safety_blocks": list(safety_blocks),
                "unmet_criteria": list(dict.fromkeys(unmet_criteria)),
                "low_session_count": len(low_sessions),
                "extreme_low_session_count": len(extreme_low_sessions),
                "reputation_score": rep_score,
                "bad_session_count": int(rep.get("bad_session_count", 0) or 0),
                "etw_matched_session_count": int(rep.get("etw_matched_session_count", 0) or 0),
            }
        )


def _confidence_name(value: int) -> str:
    for name, number in sorted(CONFIDENCE.items(), key=lambda item: item[1], reverse=True):
        if value >= number:
            return name
    return "none"


def _append_unique(values: list[str], value: str) -> None:
    if value and value not in values:
        values.append(value)


def _last_event_value(events: list[dict[str, Any]], key: str) -> str:
    for event in reversed(events):
        value = str(event.get(key, "") or "")
        if value:
            return value
    return ""


def _endpoint_port(endpoint: str) -> int | None:
    _, sep, port = endpoint.rpartition(":")
    if sep and port.isdigit():
        return int(port)
    return None


def _endpoint_ip(endpoint: str) -> str:
    host, sep, port = endpoint.rpartition(":")
    if sep and host and port.isdigit():
        return host
    return endpoint


def _coerce_activity_wake_history(value: object) -> dict[str, list[dict[str, Any]]]:
    if not isinstance(value, dict):
        return {}
    out: dict[str, list[dict[str, Any]]] = {}
    for key, raw_events in value.items():
        if not isinstance(raw_events, list):
            continue
        events = [event for event in raw_events if isinstance(event, dict)]
        if events:
            out[str(key)] = events
    return out


def _activity_wake_session_weight(
    *,
    session: PeerSession,
    endpoint_history: list[dict[str, Any]],
    ip_history: list[dict[str, Any]],
    peer_etw_evidence: bool,
    payload_evidence: bool,
    speed_evidence: bool,
    torrent_wake: bool,
) -> tuple[int, list[str]]:
    components: list[str] = []
    lone_peer_etw = peer_etw_evidence and session.file_peer_count <= 1
    if payload_evidence and lone_peer_etw:
        weight = 5
        components.append("+5 qB payload + lone-peer ETW")
    elif payload_evidence and peer_etw_evidence:
        weight = 4
        components.append("+4 qB payload + ETW")
    elif peer_etw_evidence:
        weight = 3
        components.append("+3 ETW correlation")
    elif payload_evidence:
        weight = 2
        components.append("+2 qB payload")
    elif speed_evidence:
        weight = 1
        components.append("+1 speed-backed wake")
    else:
        return 0, []

    if endpoint_history:
        weight += 1
        components.append("+1 prior endpoint activity")

    other_ip_events = [
        event
        for event in ip_history
        if str(event.get("endpoint", "")) != session.endpoint and int(event.get("wake_weight", 0) or 0) > 0
    ]
    if other_ip_events:
        weight += 1
        components.append("+1 prior IP activity on another endpoint")

    ports = {
        int(event.get("port", 0) or 0)
        for event in ip_history
        if int(event.get("port", 0) or 0) > 0 and int(event.get("wake_weight", 0) or 0) > 0
    }
    current_port = _endpoint_port(session.endpoint)
    if current_port:
        ports.add(current_port)
    if len(ports) >= 2:
        weight += 1
        components.append("+1 rotating ports")

    if torrent_wake:
        weight += 1
        components.append("+1 torrent active-transition")

    if session.attributed_etw_bytes > 0 and not peer_etw_evidence:
        weight -= 1
        components.append("-1 shared/equal ETW attribution")

    if not payload_evidence and not speed_evidence:
        weight -= 2
        components.append("-2 no qB payload or speed evidence")

    return max(0, weight), components


def _activity_wake_events_weight(events: list[dict[str, Any]]) -> int:
    return sum(int(event.get("wake_weight", 1) or 1) for event in events)


def _activity_wake_weight_components(events: list[dict[str, Any]]) -> list[str]:
    components: list[str] = []
    for event in events:
        for component in event.get("activity_weight_components", []) or []:
            if isinstance(component, str) and component not in components:
                components.append(component)
    return components


def _activity_wake_zero_upload_etw_events(events: list[dict[str, Any]]) -> int:
    return sum(
        1
        for event in events
        if int(event.get("payload", 0) or 0) <= 0 and _activity_wake_event_has_etw_evidence(event)
    )


def _activity_wake_confidence(events: list[dict[str, Any]]) -> str:
    if any(_activity_wake_event_has_etw_evidence(event) for event in events):
        return "high"
    if any(int(event.get("payload", 0) or 0) > 0 for event in events):
        return "medium"
    return "weak"


def _activity_wake_event_stats(events: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "endpoints": sorted({str(event.get("endpoint", "")) for event in events if event.get("endpoint")}),
        "torrent_hashes": sorted({str(event.get("torrent_hash", "")) for event in events if event.get("torrent_hash")}),
        "average_speed": fmean([float(event.get("average_speed", 0.0) or 0.0) for event in events]) if events else 0.0,
        "max_speed": max((int(event.get("max_speed", 0) or 0) for event in events), default=0),
        "etw_bytes": sum(int(event.get("etw_bytes", 0) or 0) for event in events),
        "raw_etw_bytes": sum(int(event.get("raw_etw_bytes", 0) or 0) for event in events),
        "etw_count": sum(int(event.get("etw_count", 0) or 0) for event in events),
    }


def _activity_wake_event_has_etw_evidence(event: dict[str, Any]) -> bool:
    if int(event.get("etw_bytes", 0) or 0) <= 0:
        return False
    return bool(event.get("etw_evidence", True))


def _session_has_activity_wake_etw_evidence(session: PeerSession) -> bool:
    if session.attributed_etw_bytes <= 0:
        return False
    return any(method != "equal" for method in session.etw_attribution_methods)


def _etw_upload_ratio(etw_bytes: int, uploaded_bytes: int) -> float | None:
    if uploaded_bytes <= 0:
        return None
    return max(0, int(etw_bytes)) / uploaded_bytes


def _slow_hdd_tier_blocks(
    *,
    average_speed: float,
    etw_upload_ratio: float | None,
    total_payload: int,
    rep: dict[str, Any],
    reputation_threshold: float,
    active_count: int,
    file_peer_count: int,
) -> list[str]:
    if average_speed < 4 * 1024:
        return []

    ratio = 0.0 if etw_upload_ratio is None else etw_upload_ratio
    bad_sessions = int(rep.get("bad_session_count", 0) or 0)
    connected_bursts = int(rep.get("connected_burst_session_count", 0) or 0)
    rep_score = float(rep.get("reputation_score", 0.0) or 0.0)

    if average_speed < 8 * 1024:
        has_extra_evidence = (
            rep_score >= reputation_threshold
            or bad_sessions >= 2
            or connected_bursts >= 5
            or active_count >= 2
        )
        blocks: list[str] = []
        if ratio < 8:
            blocks.append(f"4-8 KiB/s slow-HDD tier requires ETW/upload ratio >= 8.00x ({ratio:.2f}x)")
        if not has_extra_evidence:
            blocks.append(
                "4-8 KiB/s slow-HDD tier needs repeated reputation, connected bursts, or 2+ active torrents"
            )
        return blocks

    has_strong_extra_evidence = (
        bad_sessions >= 2
        or connected_bursts >= 8
        or active_count >= 2
        or file_peer_count <= 1
    )
    blocks = []
    if ratio < 15:
        blocks.append(f"8+ KiB/s slow-HDD tier requires ETW/upload ratio >= 15.00x ({ratio:.2f}x)")
    if total_payload < 4 * 1024 * 1024:
        blocks.append(f"8+ KiB/s slow-HDD tier requires uploaded payload >= 4 MiB ({total_payload} bytes)")
    if not has_strong_extra_evidence:
        blocks.append("8+ KiB/s slow-HDD tier needs repeated bad sessions, burst history, 2+ torrents, or lone-peer ETW")
    if file_peer_count > 1 and active_count == 1 and bad_sessions < 2:
        blocks.append("8+ KiB/s slow-HDD tier blocks shared-file single-torrent first bad session")
    return blocks


def _attribute_etw_bytes(event: EtwReadEvent, candidates: list[PeerSession]) -> tuple[list[int], str]:
    size = max(0, int(event.size))
    if not candidates:
        return [], "none"
    if len(candidates) == 1:
        return [size], "single"

    weights = [max(0, int(session.last_poll_uploaded_delta)) for session in candidates]
    total_weight = sum(weights)
    if total_weight <= 0:
        base = size // len(candidates)
        values = [base for _ in candidates]
        values[-1] += size - sum(values)
        return values, "equal"

    attributed: list[int] = []
    remaining = size
    remaining_weight = total_weight
    for weight in weights[:-1]:
        value = int(size * weight / total_weight)
        attributed.append(value)
        remaining -= value
        remaining_weight -= weight
    attributed.append(max(0, remaining))
    return attributed, "uploaded-delta"


def _dedupe_decisions(decisions: list[BanDecision]) -> list[BanDecision]:
    seen: set[str] = set()
    out: list[BanDecision] = []
    for decision in decisions:
        if decision.subject in seen:
            continue
        seen.add(decision.subject)
        out.append(decision)
    return out
