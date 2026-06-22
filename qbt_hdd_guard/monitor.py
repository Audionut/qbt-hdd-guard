from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable

import qbittorrentapi

from .engine import DecisionEngine
from .etw import EtwCollector
from .logging_utils import ban
from .models import BanDecision, GuardConfig
from .pathmap import PathMapper
from .qbt import QbtPoller, add_bare_ip_bans, ban_endpoint, set_bare_ip_bans
from .state import GuardState


class GuardMonitor:
    def __init__(
        self,
        client: qbittorrentapi.Client,
        config: GuardConfig,
        state: GuardState,
        *,
        logger: logging.Logger,
        clock: Callable[[], float] = time.time,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        self.client = client
        self.config = config
        self.state = state
        self.logger = logger
        self.clock = clock
        self.sleeper = sleeper
        self.poller = QbtPoller(client)
        self.mapper = PathMapper()
        self.engine = DecisionEngine(config, state)
        self.etw = EtwCollector(config.etw_helper, logger=logger, restart=config.etw_restart) if config.etw_enabled and config.etw_helper else None
        self._near_miss_last_logged: dict[str, float] = {}
        self._started_at = self.clock()

    def run_forever(self) -> None:
        self.logger.info("waiting for qBittorrent Web UI")
        version = self.poller.wait_until_ready()
        self.logger.info("qBittorrent Web UI online: %s", version)
        self.initialize_auto_unban()
        self.expire_permanent_bans()
        self.enforce_permanent_bans()
        if self.etw:
            self.etw.start()
        try:
            while True:
                try:
                    self.run_once()
                except qbittorrentapi.APIError as exc:
                    self.logger.error("qBittorrent API error: %s", exc)
                self.sleeper(self.config.poll_interval)
        except KeyboardInterrupt:
            self.logger.info("shutdown requested")
        finally:
            if self.etw:
                self.etw.stop()

    def run_once(self) -> list[BanDecision]:
        now = self.clock()
        self.state.prune_stale_reputation(now, self.config.stale_reputation_days * 24 * 60 * 60)
        self.expire_permanent_bans()
        self.maybe_auto_unban()

        torrents = self.poller.active_completed_torrents()
        self.engine.observe_active_torrents({torrent.hash for torrent in torrents})
        snapshots = []
        for torrent in torrents:
            try:
                self.mapper.update_torrent(torrent, self.poller.torrent_files(torrent.hash))
                snapshots.extend(self.poller.peer_snapshots(torrent))
            except qbittorrentapi.APIError as exc:
                self.logger.debug("skipping torrent %s after qB API error: %s", torrent.hash, exc)

        seen = {(snapshot.endpoint, snapshot.torrent_hash) for snapshot in snapshots}
        self.engine.observe(snapshots, now)
        etw_received = 0
        etw_matched = 0
        if self.etw:
            self.etw.set_watch_paths(self.mapper.watch_roots())
            events = self.etw.drain()
            etw_received = len(events)
            for event in events:
                etw_matched += self.engine.ingest_etw(event, self.mapper)
        elif self.config.etw_required and not self.config.allow_speed_only_bans:
            self.logger.debug("ETW required and unavailable; ban decisions requiring ETW are paused")

        self.engine.close_missing(seen, now)
        decisions = self.engine.decisions(now)
        for decision in decisions:
            self.apply_decision(decision)
        self._write_near_miss_audits(now)
        self.logger.debug(
            "poll complete: active_torrents=%s peers=%s decisions=%s etw_received=%s etw_matched=%s",
            len(torrents),
            len(snapshots),
            len(decisions),
            etw_received,
            etw_matched,
        )
        return decisions

    def apply_decision(self, decision: BanDecision) -> None:
        now = self.clock()
        if self.state.is_permanent(decision.subject) or self.state.is_banned_this_cycle(decision.subject):
            return

        self._log_decision(decision, dry_run=self.config.dry_run)
        if not self.config.dry_run:
            if decision.subject == decision.ip and decision.ip:
                add_bare_ip_bans(self.client, [decision.ip])
            else:
                ban_endpoint(self.client, decision.subject)

        count, counted, promoted = self.state.record_ban(decision.subject, now, self.config.permanent_after)
        self.engine.record_ban_outcome(decision.endpoint or decision.subject, decision.ip, now)
        self._write_ban_audit(decision, now=now, ban_count=count, counted=counted, promoted=promoted)
        if counted:
            self.logger.info("Client %s ban count: %s", decision.subject, count)
        else:
            self.logger.info("Client %s already counted in current unban cycle: %s", decision.subject, count)

        if promoted:
            if not self.config.dry_run:
                if decision.subject == decision.ip and decision.ip:
                    add_bare_ip_bans(self.client, [decision.subject])
                else:
                    ban_endpoint(self.client, decision.subject)
            ban(self.logger, "Client %s permanently banned", decision.subject)

    def _write_ban_audit(
        self,
        decision: BanDecision,
        *,
        now: float,
        ban_count: int,
        counted: bool,
        promoted: bool,
    ) -> None:
        endpoint_key = decision.endpoint or decision.subject
        record = {
            "timestamp": now,
            "timestamp_iso_utc": _iso_utc(now),
            "dry_run": self.config.dry_run,
            "subject": decision.subject,
            "endpoint": decision.endpoint,
            "ip": decision.ip,
            "torrent_hashes": list(decision.torrent_hashes),
            "reason": decision.reason,
            "score": decision.score,
            "confidence": decision.confidence,
            "average_speed_bps": decision.average_speed,
            "max_speed_bps": decision.max_speed,
            "uploaded_delta_bytes": decision.uploaded_delta,
            "etw_read_bytes": decision.etw_bytes,
            "etw_read_bytes_attributed": decision.etw_bytes,
            "etw_read_bytes_raw": decision.raw_etw_bytes or decision.etw_bytes,
            "etw_upload_ratio": decision.etw_upload_ratio,
            "etw_read_count": decision.etw_count,
            "active_torrent_count": decision.active_torrent_count,
            "file_peer_count": decision.file_peer_count,
            "etw_attribution_methods": list(decision.etw_attribution_methods),
            "external_read_bytes": decision.external_read_bytes,
            "external_read_count": decision.external_read_count,
            "external_process_count": decision.external_process_count,
            "external_processes": list(decision.external_processes),
            "score_components": list(decision.score_components),
            "decision_details": _json_copy(decision.details),
            "ban_count": ban_count,
            "counted_in_unban_cycle": counted,
            "promoted_to_permanent": promoted,
            "unban_cycle": self.state.unban_cycle,
            "permanent": self.state.is_permanent(decision.subject),
            "ban_record": _json_copy(self.state.ban_record(decision.subject)),
            "endpoint_reputation": _json_copy(self.state.data.get("reputation", {}).get(endpoint_key, {})),
            "ip_reputation": _json_copy(self.state.data.get("ip_reputation", {}).get(decision.ip, {})),
        }
        self.state.append_ban_audit(record)
        self.logger.debug("ban audit written: %s", self.state.ban_audit_file)

    def _write_near_miss_audits(self, now: float) -> None:
        if self.config.near_miss_log_interval < 0:
            return
        if self.config.near_miss_startup_grace > 0 and now - self._started_at < self.config.near_miss_startup_grace:
            return
        written = 0
        for near_miss in self.engine.near_misses_last:
            key = self._near_miss_key(near_miss)
            last_logged = self._near_miss_last_logged.get(key)
            if last_logged is not None and now - last_logged < self.config.near_miss_log_interval:
                continue
            self._near_miss_last_logged[key] = now
            record = {
                "timestamp": now,
                "timestamp_iso_utc": _iso_utc(now),
                "dry_run": self.config.dry_run,
                **_json_copy(near_miss),
                "unban_cycle": self.state.unban_cycle,
                "ban_record": _json_copy(self.state.data.get("bans", {}).get(str(near_miss.get("subject", "")), {})),
                "endpoint_reputation": _json_copy(
                    self.state.data.get("reputation", {}).get(str(near_miss.get("endpoint", near_miss.get("subject", ""))), {})
                ),
                "ip_reputation": _json_copy(self.state.data.get("ip_reputation", {}).get(str(near_miss.get("ip", "")), {})),
            }
            self.state.append_near_miss_audit(record)
            written += 1
        if written:
            self.logger.debug("near-miss audits written: %s file=%s", written, self.state.near_miss_audit_file)

    def _near_miss_key(self, near_miss: dict[str, object]) -> str:
        return str(near_miss.get("subject", ""))

    def initialize_auto_unban(self) -> None:
        if self.config.auto_unban_interval <= 0:
            return
        if self.state.last_auto_unban is None:
            self.state.mark_auto_unban(self.clock(), advance_cycle=False)
            self.logger.info("auto-unban timer initialized")

    def maybe_auto_unban(self) -> bool:
        if self.config.auto_unban_interval <= 0 or self.state.last_auto_unban is None:
            return False
        now = self.clock()
        if now - self.state.last_auto_unban < self.config.auto_unban_interval:
            return False
        if not self.config.dry_run:
            set_bare_ip_bans(self.client, [subject for subject in self.state.permanent_subjects() if _is_host_only(subject)])
            self.enforce_permanent_bans()
        self.state.mark_auto_unban(now, advance_cycle=True)
        ban(self.logger, "auto-unbanned normal peers; kept %s permanent bans", len(self.state.permanent_subjects()))
        return True

    def enforce_permanent_bans(self) -> None:
        permanent = sorted(self.state.permanent_subjects())
        if not permanent:
            return
        if self.config.dry_run:
            self.logger.info("(dry-run) would enforce %s permanent bans", len(permanent))
            return
        hosts = [subject for subject in permanent if _is_host_only(subject)]
        endpoints = [subject for subject in permanent if not _is_host_only(subject)]
        if hosts:
            add_bare_ip_bans(self.client, hosts)
        for endpoint in endpoints:
            ban_endpoint(self.client, endpoint)
        self.logger.info("enforced %s permanent bans", len(permanent))

    def expire_permanent_bans(self) -> None:
        expired = self.state.expire_first_generation_permanent(self.clock(), self.config.permanent_expire_after)
        if not expired:
            return
        if not self.config.dry_run:
            set_bare_ip_bans(self.client, [subject for subject in self.state.permanent_subjects() if _is_host_only(subject)])
            self.enforce_permanent_bans()
        ban(self.logger, "expired first-stage permanent bans: %s", expired)

    def _log_decision(self, decision: BanDecision, *, dry_run: bool) -> None:
        prefix = "(dry-run) " if dry_run else ""
        ban(
            self.logger,
            "%sban %s reason=%s ip=%s torrents=%s score=%s confidence=%s avg_speed=%.1fB/s max_speed=%sB/s uploaded=%s etw_bytes=%s raw_etw_bytes=%s etw_ratio=%s etw_count=%s file_peers=%s attribution=%s external_reads=%s external_bytes=%s external_processes=%s active_torrents=%s components=%s",
            prefix,
            decision.subject,
            decision.reason,
            decision.ip,
            ",".join(decision.torrent_hashes) or "-",
            decision.score,
            decision.confidence,
            decision.average_speed,
            decision.max_speed,
            decision.uploaded_delta,
            decision.etw_bytes,
            decision.raw_etw_bytes or decision.etw_bytes,
            "-" if decision.etw_upload_ratio is None else f"{decision.etw_upload_ratio:.2f}x",
            decision.etw_count,
            decision.file_peer_count,
            ",".join(decision.etw_attribution_methods) or "-",
            decision.external_read_count,
            decision.external_read_bytes,
            ",".join(decision.external_processes) or "-",
            decision.active_torrent_count,
            " | ".join(decision.score_components),
        )


def _is_host_only(subject: str) -> bool:
    host, sep, port = subject.rpartition(":")
    return not (sep and host and port.isdigit())


def _iso_utc(timestamp: float) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(timestamp))


def _json_copy(value: object) -> object:
    return json.loads(json.dumps(value, default=str))
