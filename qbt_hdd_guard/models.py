from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


CONFIDENCE = {"none": 0, "low": 1, "medium": 2, "high": 3}


@dataclass(frozen=True)
class ConnectionConfig:
    host: str = "localhost"
    port: int = 8080
    username: str | None = None
    password: str | None = None
    timeout: float = 30.0
    verify_webui_certificate: bool = True


@dataclass(frozen=True)
class GuardConfig:
    poll_interval: float = 5.0
    low_speed_threshold: int = 16 * 1024
    threshold_time: float = 180.0
    low_speed_ratio: float = 0.8
    extreme_low_speed_threshold: int = 4 * 1024
    extreme_low_speed_time: float = 900.0
    extreme_low_speed_min_payload: int = 1024 * 1024
    productive_speed: int | None = None
    good_speed_multiplier: float = 4.0
    good_polls: int = 3
    min_payload: int = 1024 * 1024
    min_etw_upload_ratio: float = 5.0
    required_confidence: str = "medium"
    allow_speed_only_bans: bool = True
    speed_only_score_threshold: int = 10
    score_threshold: int = 8
    reputation_threshold: int = 12
    reputation_bad_sessions: int = 3
    reputation_decay_hours: float = 24.0
    stale_reputation_days: float = 30.0
    auto_unban_interval: float = 7 * 24 * 60 * 60
    permanent_after: int = 2
    permanent_expire_after: float = 120 * 24 * 60 * 60
    bare_ip_bad_endpoint_count: int = 0
    short_session_max: float = 10.0
    burst_min_payload: int = 128 * 1024
    burst_count: int = 5
    burst_window: float = 60 * 60
    burst_min_total_payload: int = 2 * 1024 * 1024
    connected_burst_min_payload: int = 128 * 1024
    connected_burst_count: int = 5
    connected_burst_window: float = 60 * 60
    connected_burst_min_total_payload: int = 2 * 1024 * 1024
    connected_burst_max_duty_ratio: float = 0.25
    connected_burst_max_average_speed: int | None = None
    activity_wake_churn_after: int = 10
    ip_activity_wake_churn_after: int = 35
    ip_activity_wake_churn_window: float = 2 * 60 * 60
    ip_activity_wake_churn_max_session_time: float = 90.0
    ip_activity_wake_churn_max_single_payload: int = 64 * 1024
    ip_activity_wake_churn_max_total_payload: int = 256 * 1024
    ip_activity_wake_churn_min_speed: int = 10 * 1024
    ip_activity_wake_churn_distinct_ports: int = 3
    ip_activity_wake_churn_all_distinct_ports_over: int = 15
    terminal_piece_progress: float = 0.999
    terminal_piece_ban_after: int = 3
    terminal_piece_window: float = 2 * 60 * 60
    terminal_piece_min_payload: int = 1024 * 1024
    terminal_piece_excess_ratio: float = 2.0
    slow_burn_window: float = 24 * 60 * 60
    slow_burn_min_etw_session_bytes: int = 1024 * 1024
    slow_burn_min_etw_upload_ratio: float = 10.0
    slow_burn_productive_credit_ratio: float = 1.0
    long_term_low_speed_time: float = 900.0
    long_term_low_speed_min_etw_sessions: int = 3
    long_term_low_speed_min_etw_bytes: int = 256 * 1024 * 1024
    long_term_low_speed_min_uploaded: int = 4 * 1024 * 1024
    etw_enabled: bool = False
    etw_helper: Path | None = None
    etw_required: bool = False
    etw_restart: bool = True
    etw_read_buffer: float = 300.0
    near_miss_log_interval: float = 300.0
    near_miss_startup_grace: float = 60.0
    dry_run: bool = False
    state_dir: Path | None = None

    @property
    def effective_productive_speed(self) -> int:
        return self.low_speed_threshold if self.productive_speed is None else self.productive_speed

    @property
    def effective_connected_burst_max_average_speed(self) -> int:
        return (
            self.low_speed_threshold
            if self.connected_burst_max_average_speed is None
            else self.connected_burst_max_average_speed
        )

    @property
    def minimum_confidence_value(self) -> int:
        return CONFIDENCE[self.required_confidence]


@dataclass(frozen=True)
class TorrentInfo:
    hash: str
    name: str
    content_path: str
    save_path: str
    state: str
    amount_left: int
    size: int = 0


@dataclass(frozen=True)
class PeerSnapshot:
    torrent_hash: str
    torrent_name: str
    endpoint: str
    ip: str
    port: int | None
    up_speed: int
    uploaded: int
    client: str
    torrent_size: int = 0
    progress: float | None = None
    downloaded: int | None = None
    relevance: float | None = None
    files: tuple[Any, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class EtwReadEvent:
    ts: float
    path: str
    size: int
    process: str = "qbittorrent.exe"
    op: str = "Read"
    pid: int | None = None
    offset: int | None = None
    is_qbt: bool = True


@dataclass(frozen=True)
class BanDecision:
    subject: str
    endpoint: str
    ip: str
    torrent_hashes: tuple[str, ...]
    reason: str
    score: int
    confidence: str
    average_speed: float
    max_speed: int
    uploaded_delta: int
    etw_bytes: int
    etw_count: int
    active_torrent_count: int
    score_components: tuple[str, ...]
    raw_etw_bytes: int = 0
    file_peer_count: int = 0
    etw_attribution_methods: tuple[str, ...] = ()
    external_read_bytes: int = 0
    external_read_count: int = 0
    external_process_count: int = 0
    external_processes: tuple[str, ...] = ()
    etw_upload_ratio: float | None = None
    details: dict[str, Any] = field(default_factory=dict)
