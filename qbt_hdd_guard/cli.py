from __future__ import annotations

import argparse
import getpass
import sys
from pathlib import Path

from .logging_utils import configure_logging
from .models import CONFIDENCE, ConnectionConfig, GuardConfig
from .monitor import GuardMonitor
from .qbt import build_client
from .state import GuardState


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logger = configure_logging(args.log_level, ban_only=args.ban_log_only)

    password = args.password
    if args.prompt_password:
        password = getpass.getpass("qBittorrent password: ")

    root_dir = runtime_root()
    state_dir = resolve_from_root(args.state_dir, root_dir) if args.state_dir else root_dir / "state"
    etw_helper = resolve_from_root(args.etw_helper, root_dir) if args.etw_helper else default_etw_helper(root_dir)

    config = GuardConfig(
        poll_interval=args.poll_interval,
        low_speed_threshold=parse_size(args.low_speed_threshold),
        threshold_time=args.threshold_time,
        low_speed_ratio=args.low_speed_ratio,
        extreme_low_speed_threshold=parse_size(args.extreme_low_speed_threshold),
        extreme_low_speed_time=args.extreme_low_speed_time,
        extreme_low_speed_min_payload=parse_size(args.extreme_low_speed_min_payload),
        productive_speed=parse_size(args.productive_speed) if args.productive_speed else None,
        good_speed_multiplier=args.good_speed_multiplier,
        good_polls=args.good_polls,
        min_payload=parse_size(args.min_payload),
        min_etw_upload_ratio=args.min_etw_upload_ratio,
        required_confidence=args.required_confidence,
        allow_speed_only_bans=args.allow_speed_only_bans,
        score_threshold=args.score_threshold,
        reputation_threshold=args.reputation_threshold,
        reputation_bad_sessions=args.reputation_bad_sessions,
        auto_unban_interval=args.auto_unban_interval,
        permanent_after=args.permanent_after,
        permanent_expire_after=args.permanent_expire_after,
        bare_ip_bad_endpoint_count=args.bare_ip_bad_endpoint_count,
        short_session_max=args.short_session_max,
        burst_min_payload=parse_size(args.burst_min_payload),
        burst_count=args.burst_count,
        burst_window=args.burst_window,
        burst_min_total_payload=parse_size(args.burst_min_total_payload),
        connected_burst_min_payload=parse_size(args.connected_burst_min_payload),
        connected_burst_count=args.connected_burst_count,
        connected_burst_window=args.connected_burst_window,
        connected_burst_min_total_payload=parse_size(args.connected_burst_min_total_payload),
        connected_burst_max_duty_ratio=args.connected_burst_max_duty_ratio,
        connected_burst_max_average_speed=(
            parse_size(args.connected_burst_max_average_speed) if args.connected_burst_max_average_speed else None
        ),
        activity_wake_churn_after=args.activity_wake_churn_after,
        ip_activity_wake_churn_after=args.ip_activity_wake_churn_after,
        ip_activity_wake_churn_window=args.ip_activity_wake_churn_window,
        ip_activity_wake_churn_max_session_time=args.ip_activity_wake_churn_max_session_time,
        ip_activity_wake_churn_max_single_payload=parse_size(args.ip_activity_wake_churn_max_single_payload),
        ip_activity_wake_churn_max_total_payload=parse_size(args.ip_activity_wake_churn_max_total_payload),
        ip_activity_wake_churn_min_speed=parse_size(args.ip_activity_wake_churn_min_speed),
        ip_activity_wake_churn_distinct_ports=args.ip_activity_wake_churn_distinct_ports,
        ip_activity_wake_churn_all_distinct_ports_over=args.ip_activity_wake_churn_all_distinct_ports_over,
        terminal_piece_progress=args.terminal_piece_progress,
        terminal_piece_ban_after=args.terminal_piece_ban_after,
        terminal_piece_window=args.terminal_piece_window,
        terminal_piece_min_payload=parse_size(args.terminal_piece_min_payload),
        terminal_piece_excess_ratio=args.terminal_piece_excess_ratio,
        slow_burn_window=args.slow_burn_window,
        slow_burn_min_etw_session_bytes=parse_size(args.slow_burn_min_etw_session_bytes),
        slow_burn_min_etw_upload_ratio=args.slow_burn_min_etw_upload_ratio,
        slow_burn_productive_credit_ratio=args.slow_burn_productive_credit_ratio,
        long_term_low_speed_time=args.long_term_low_speed_time,
        long_term_low_speed_min_etw_sessions=args.long_term_low_speed_min_etw_sessions,
        long_term_low_speed_min_etw_bytes=parse_size(args.long_term_low_speed_min_etw_bytes),
        long_term_low_speed_min_uploaded=parse_size(args.long_term_low_speed_min_uploaded),
        etw_enabled=args.etw,
        etw_helper=etw_helper,
        etw_required=args.etw_required,
        etw_restart=not args.no_etw_restart,
        near_miss_log_interval=args.near_miss_log_interval,
        near_miss_startup_grace=args.near_miss_startup_grace,
        dry_run=args.dry_run,
        state_dir=state_dir,
    )

    client = build_client(
        ConnectionConfig(
            host=args.host,
            port=args.port,
            username=args.username,
            password=password,
            timeout=args.timeout,
            verify_webui_certificate=not args.no_verify_webui_certificate,
        )
    )
    state = GuardState.load(state_dir)
    monitor = GuardMonitor(client, config, state, logger=logger)
    if args.once:
        monitor.poller.wait_until_ready()
        monitor.initialize_auto_unban()
        monitor.run_once()
        return 0
    monitor.run_forever()
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="qBittorrent HDD-churn peer guard")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--username")
    parser.add_argument("--password")
    parser.add_argument("--prompt-password", action="store_true")
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--no-verify-webui-certificate", action="store_true")

    parser.add_argument("--poll-interval", type=float, default=5.0)
    parser.add_argument("--low-speed-threshold", default="16KiB")
    parser.add_argument("--threshold-time", type=float, default=180.0)
    parser.add_argument("--low-speed-ratio", type=float, default=0.8)
    parser.add_argument("--extreme-low-speed-threshold", default="4KiB")
    parser.add_argument("--extreme-low-speed-time", type=float, default=900.0)
    parser.add_argument("--extreme-low-speed-min-payload", default="1MiB")
    parser.add_argument("--productive-speed")
    parser.add_argument("--good-speed-multiplier", type=float, default=4.0)
    parser.add_argument("--good-polls", type=int, default=3)
    parser.add_argument("--min-payload", default="1MiB")
    parser.add_argument("--min-etw-upload-ratio", type=float, default=5.0)
    parser.add_argument("--required-confidence", choices=sorted(CONFIDENCE), default="medium")
    parser.add_argument("--speed-only-bans", dest="allow_speed_only_bans", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--allow-speed-only-bans", dest="allow_speed_only_bans", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--score-threshold", type=int, default=8)
    parser.add_argument("--reputation-threshold", type=int, default=12)
    parser.add_argument("--reputation-bad-sessions", type=int, default=3)

    parser.add_argument("--etw", action="store_true", help="start/read C# ETW helper; admin rights usually required")
    parser.add_argument("--etw-helper")
    parser.add_argument("--require-etw", dest="etw_required", action="store_true", default=False, help="require ETW confidence before banning")
    parser.add_argument("--etw-not-required", dest="etw_required", action="store_false", help=argparse.SUPPRESS)
    parser.add_argument("--no-etw-restart", action="store_true")
    parser.add_argument("--near-miss-log-interval", type=float, default=300.0)
    parser.add_argument("--near-miss-startup-grace", type=float, default=60.0)

    parser.add_argument("--auto-unban-interval", type=float, default=7 * 24 * 60 * 60)
    parser.add_argument("--permanent-after", type=int, default=2)
    parser.add_argument("--permanent-expire-after", type=float, default=120 * 24 * 60 * 60)
    parser.add_argument("--bare-ip-bad-endpoint-count", type=int, default=0)

    parser.add_argument("--short-session-max", type=float, default=10.0)
    parser.add_argument("--burst-min-payload", default="128KiB")
    parser.add_argument("--burst-count", type=int, default=5)
    parser.add_argument("--burst-window", type=float, default=3600.0)
    parser.add_argument("--burst-min-total-payload", default="2MiB")
    parser.add_argument("--connected-burst-min-payload", default="128KiB")
    parser.add_argument("--connected-burst-count", type=int, default=5)
    parser.add_argument("--connected-burst-window", type=float, default=3600.0)
    parser.add_argument("--connected-burst-min-total-payload", default="2MiB")
    parser.add_argument("--connected-burst-max-duty-ratio", type=float, default=0.25)
    parser.add_argument("--connected-burst-max-average-speed")
    parser.add_argument("--activity-wake-churn-after", type=int, default=10)
    parser.add_argument("--ip-activity-wake-churn-after", type=int, default=35)
    parser.add_argument("--ip-activity-wake-churn-window", type=float, default=7200.0)
    parser.add_argument("--ip-activity-wake-churn-max-session-time", type=float, default=90.0)
    parser.add_argument("--ip-activity-wake-churn-max-single-payload", default="64KiB")
    parser.add_argument("--ip-activity-wake-churn-max-total-payload", default="256KiB")
    parser.add_argument("--ip-activity-wake-churn-min-speed", default="10KiB")
    parser.add_argument("--ip-activity-wake-churn-distinct-ports", type=int, default=3)
    parser.add_argument("--ip-activity-wake-churn-all-distinct-ports-over", type=int, default=15)
    parser.add_argument("--terminal-piece-progress", type=float, default=0.999)
    parser.add_argument("--terminal-piece-ban-after", type=int, default=3)
    parser.add_argument("--terminal-piece-window", type=float, default=7200.0)
    parser.add_argument("--terminal-piece-min-payload", default="1MiB")
    parser.add_argument("--terminal-piece-excess-ratio", type=float, default=2.0)
    parser.add_argument("--slow-burn-window", type=float, default=86400.0)
    parser.add_argument("--slow-burn-min-etw-session-bytes", default="1MiB")
    parser.add_argument("--slow-burn-min-etw-upload-ratio", type=float, default=10.0)
    parser.add_argument("--slow-burn-productive-credit-ratio", type=float, default=1.0)
    parser.add_argument("--long-term-low-speed-time", type=float, default=900.0)
    parser.add_argument("--long-term-low-speed-min-etw-sessions", type=int, default=3)
    parser.add_argument("--long-term-low-speed-min-etw-bytes", default="256MiB")
    parser.add_argument("--long-term-low-speed-min-uploaded", default="4MiB")

    parser.add_argument("--state-dir")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--ban-log-only", action="store_true")
    return parser


def parse_size(value: str) -> int:
    raw = value.strip().lower().replace("/s", "")
    units = [
        ("gib", 1024**3),
        ("gb", 1000**3),
        ("mib", 1024**2),
        ("mb", 1000**2),
        ("kib", 1024),
        ("kb", 1000),
        ("b", 1),
    ]
    for suffix, multiplier in units:
        if raw.endswith(suffix):
            return int(float(raw[: -len(suffix)].strip()) * multiplier)
    return int(float(raw))


def runtime_root() -> Path:
    launcher = Path(sys.argv[0]).resolve()
    if launcher.is_file() and (launcher.parent / "qbt_hdd_guard").exists():
        return launcher.parent
    return Path(__file__).resolve().parents[1]


def resolve_from_root(value: str, root_dir: Path) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path.resolve()
    return (root_dir / path).resolve()


def default_etw_helper(root_dir: Path) -> Path:
    candidates = [
        root_dir / "etw-helper" / "bin" / "Release" / "net8.0" / "QbtEtwHelper.exe",
        root_dir / "etw-helper" / "bin" / "Release" / "net8.0" / "publish" / "QbtEtwHelper.exe",
        root_dir / "etw-helper" / "bin" / "Diag" / "QbtEtwHelperDiag.exe",
        root_dir / "QbtEtwHelper.exe",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0]


if __name__ == "__main__":
    raise SystemExit(main())
