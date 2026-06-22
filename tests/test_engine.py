from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from qbt_hdd_guard.engine import DecisionEngine
from qbt_hdd_guard.models import EtwReadEvent, GuardConfig, PeerSnapshot, TorrentInfo
from qbt_hdd_guard.pathmap import PathMapper
from qbt_hdd_guard.state import GuardState


class EngineTests(unittest.TestCase):
    def make_engine(self, **overrides):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        values = {
            "threshold_time": 10,
            "low_speed_threshold": 2048,
            "min_payload": 1024,
            "required_confidence": "medium",
        }
        values.update(overrides)
        config = GuardConfig(**values)
        state = GuardState.load(Path(tmp.name))
        return DecisionEngine(config, state), config

    def test_medium_etw_slow_payload_bans(self):
        engine, _ = self.make_engine()
        mapper = self.mapper()
        peer = self.peer(uploaded=0)
        engine.observe([peer], 0)
        engine.observe([self.peer(uploaded=2048)], 11)
        self.assertEqual(engine.ingest_etw(EtwReadEvent(ts=11, path=r"\\testshare\Torrents\Movie\file.mkv", size=12288), mapper), 1)
        decisions = engine.decisions(11)
        self.assertEqual(len(decisions), 1)
        self.assertEqual(decisions[0].reason, "slow-hdd-churn")
        self.assertAlmostEqual(decisions[0].etw_upload_ratio, 6.0)

    def test_medium_etw_low_ratio_slow_payload_does_not_ban(self):
        engine, _ = self.make_engine()
        mapper = self.mapper()
        engine.observe([self.peer(uploaded=0)], 0)
        engine.observe([self.peer(uploaded=2048)], 11)
        self.assertEqual(engine.ingest_etw(EtwReadEvent(ts=11, path=r"\\testshare\Torrents\Movie\file.mkv", size=4096), mapper), 1)
        decisions = engine.decisions(11)
        self.assertEqual(decisions, [])
        self.assertIn("ETW/upload ratio", engine.near_misses_last[0]["safety_blocks"][0])

    def test_slow_hdd_churn_above_8k_requires_stronger_extra_evidence(self):
        engine, _ = self.make_engine(low_speed_threshold=16 * 1024, threshold_time=10, min_payload=1024)
        mapper = self.mapper()
        target = "185.228.162.195:55555"
        engine.observe(
            [
                self.peer(uploaded=0, up_speed=10 * 1024, endpoint=target, ip="185.228.162.195"),
                self.peer(uploaded=0, up_speed=20 * 1024, endpoint="5.6.7.8:50002", ip="5.6.7.8"),
                self.peer(uploaded=0, up_speed=20 * 1024, endpoint="9.10.11.12:50003", ip="9.10.11.12"),
            ],
            0,
        )
        engine.observe(
            [
                self.peer(uploaded=6 * 1024 * 1024, up_speed=10 * 1024, endpoint=target, ip="185.228.162.195"),
                self.peer(uploaded=0, up_speed=20 * 1024, endpoint="5.6.7.8:50002", ip="5.6.7.8"),
                self.peer(uploaded=0, up_speed=20 * 1024, endpoint="9.10.11.12:50003", ip="9.10.11.12"),
            ],
            11,
        )
        self.assertEqual(
            engine.ingest_etw(EtwReadEvent(ts=11, path=r"\\testshare\Torrents\Movie\file.mkv", size=48 * 1024 * 1024), mapper),
            3,
        )

        self.assertEqual(engine.decisions(11), [])
        target_near_miss = next(item for item in engine.near_misses_last if item["endpoint"] == target)
        self.assertTrue(
            any(
                item.startswith("8+ KiB/s slow-HDD tier requires ETW/upload ratio")
                for item in target_near_miss["unmet_criteria"]
            )
        )
        self.assertIn(
            "8+ KiB/s slow-HDD tier blocks shared-file single-torrent first bad session",
            target_near_miss["unmet_criteria"],
        )

    def test_shared_file_etw_records_peer_count(self):
        engine, _ = self.make_engine()
        mapper = self.mapper()
        engine.observe([self.peer(uploaded=0), self.peer(endpoint="5.6.7.8:6881", ip="5.6.7.8", uploaded=0)], 0)
        engine.observe([self.peer(uploaded=2048), self.peer(endpoint="5.6.7.8:6881", ip="5.6.7.8", uploaded=2048)], 11)
        self.assertEqual(engine.ingest_etw(EtwReadEvent(ts=11, path=r"\\testshare\Torrents\Movie\file.mkv", size=24576), mapper), 2)
        decisions = engine.decisions(11)
        self.assertEqual({decision.file_peer_count for decision in decisions}, {2})
        self.assertEqual({decision.confidence for decision in decisions}, {"medium"})

    def test_shared_file_etw_is_weighted_by_uploaded_delta(self):
        engine, _ = self.make_engine()
        mapper = self.mapper()
        peer_a = self.peer(uploaded=0)
        peer_b = self.peer(endpoint="5.6.7.8:6881", ip="5.6.7.8", uploaded=0)
        engine.observe([peer_a, peer_b], 0)
        engine.observe(
            [
                self.peer(uploaded=3072),
                self.peer(endpoint="5.6.7.8:6881", ip="5.6.7.8", uploaded=1024),
            ],
            11,
        )
        self.assertEqual(engine.ingest_etw(EtwReadEvent(ts=11, path=r"\\testshare\Torrents\Movie\file.mkv", size=4096), mapper), 2)

        session_a = engine.sessions[("1.2.3.4:6881", "abc")]
        session_b = engine.sessions[("5.6.7.8:6881", "abc")]
        self.assertEqual(session_a.etw_bytes, 4096)
        self.assertEqual(session_b.etw_bytes, 4096)
        self.assertEqual(session_a.attributed_etw_bytes, 3072)
        self.assertEqual(session_b.attributed_etw_bytes, 1024)
        self.assertEqual(session_a.etw_attribution_methods, {"uploaded-delta"})

    def test_external_process_reads_are_reported_without_counting_as_qbt_etw(self):
        engine, _ = self.make_engine()
        mapper = self.mapper()
        engine.observe([self.peer(uploaded=0)], 0)
        engine.observe([self.peer(uploaded=2048)], 11)
        self.assertEqual(
            engine.ingest_etw(
                EtwReadEvent(
                    ts=11,
                    path=r"\\testshare\Torrents\Movie\file.mkv",
                    size=8192,
                    process="MsMpEng.exe",
                    is_qbt=False,
                ),
                mapper,
            ),
            0,
        )
        self.assertEqual(engine.ingest_etw(EtwReadEvent(ts=11, path=r"\\testshare\Torrents\Movie\file.mkv", size=12288), mapper), 1)
        decisions = engine.decisions(11)
        self.assertEqual(len(decisions), 1)
        self.assertEqual(decisions[0].external_read_bytes, 8192)
        self.assertEqual(decisions[0].external_read_count, 1)
        self.assertEqual(decisions[0].external_processes, ("MsMpEng.exe",))

    def test_no_etw_no_ban_by_default(self):
        engine, _ = self.make_engine(allow_speed_only_bans=False, etw_required=True)
        engine.observe([self.peer(uploaded=0)], 0)
        engine.observe([self.peer(uploaded=4096)], 11)
        self.assertEqual(engine.decisions(11), [])

    def test_near_miss_records_failed_gates(self):
        engine, _ = self.make_engine(min_payload=4096)
        mapper = self.mapper()
        engine.observe([self.peer(uploaded=0)], 0)
        engine.observe([self.peer(uploaded=2048)], 11)
        self.assertEqual(engine.ingest_etw(EtwReadEvent(ts=11, path=r"\\testshare\Torrents\Movie\file.mkv", size=4096), mapper), 1)
        self.assertEqual(engine.decisions(11), [])
        self.assertEqual(len(engine.near_misses_last), 1)
        self.assertEqual(engine.near_misses_last[0]["subject"], "1.2.3.4:6881")
        self.assertTrue(engine.near_misses_last[0]["safety_blocks"])

    def test_high_throughput_etw_payload_is_not_near_miss_noise(self):
        engine, _ = self.make_engine(min_payload=1024)
        mapper = self.mapper()
        engine.observe([self.peer(uploaded=0, up_speed=9000)], 0)
        engine.observe([self.peer(uploaded=2048, up_speed=9000)], 11)
        self.assertEqual(engine.ingest_etw(EtwReadEvent(ts=11, path=r"\\testshare\Torrents\Movie\file.mkv", size=4096), mapper), 1)

        self.assertEqual(engine.decisions(11), [])
        self.assertEqual(engine.near_misses_last, [])

    def test_ordinary_early_peer_is_not_near_miss(self):
        engine, _ = self.make_engine(min_payload=4096)
        engine.observe([self.peer(uploaded=0)], 0)
        engine.observe([self.peer(uploaded=512)], 5)
        self.assertEqual(engine.decisions(5), [])
        self.assertEqual(engine.near_misses_last, [])
        self.assertEqual(engine.state.data["ip_reputation"], {})

    def test_zero_payload_disconnect_does_not_persist_reputation(self):
        engine, _ = self.make_engine()
        engine.observe([self.peer(uploaded=0)], 0)
        engine.close_missing(set(), 5)
        self.assertEqual(engine.state.data["reputation"], {})

    def test_repeated_speed_only_bad_sessions_can_ban_by_default(self):
        engine, _ = self.make_engine()
        for i in range(3):
            start = i * 20
            engine.observe([self.peer(uploaded=0)], start)
            engine.observe([self.peer(uploaded=4096)], start + 11)
            engine.close_missing(set(), start + 12)
        engine.observe([self.peer(uploaded=0)], 100)
        engine.observe([self.peer(uploaded=4096)], 111)
        decisions = engine.decisions(111)
        self.assertEqual(len(decisions), 1)
        self.assertEqual(decisions[0].reason, "speed-only-fallback")

    def test_productive_peer_not_banned_without_churn(self):
        engine, _ = self.make_engine(allow_speed_only_bans=True, etw_required=False)
        engine.observe([self.peer(uploaded=0, up_speed=9000)], 0)
        engine.observe([self.peer(uploaded=9000, up_speed=9000)], 11)
        self.assertEqual(engine.decisions(11), [])

    def test_single_torrent_extreme_low_speed_can_ban_after_long_window(self):
        engine, _ = self.make_engine(
            threshold_time=9999,
            productive_speed=16 * 1024,
            extreme_low_speed_threshold=4 * 1024,
            extreme_low_speed_time=900,
            extreme_low_speed_min_payload=1024,
        )
        engine.observe([self.peer(uploaded=0, up_speed=3 * 1024)], 0)
        engine.observe([self.peer(uploaded=2048, up_speed=3 * 1024)], 901)
        decisions = engine.decisions(901)
        self.assertEqual(len(decisions), 1)
        self.assertEqual(decisions[0].reason, "extreme-low-speed")

    def test_single_torrent_extreme_low_speed_respects_payload_floor(self):
        engine, _ = self.make_engine(
            threshold_time=9999,
            productive_speed=16 * 1024,
            extreme_low_speed_threshold=4 * 1024,
            extreme_low_speed_time=900,
            extreme_low_speed_min_payload=4096,
        )
        engine.observe([self.peer(uploaded=0, up_speed=3 * 1024)], 0)
        engine.observe([self.peer(uploaded=2048, up_speed=3 * 1024)], 901)
        self.assertEqual(engine.decisions(901), [])

    def test_burst_reconnect_after_repeated_etw_payload_sessions(self):
        engine, _ = self.make_engine(short_session_max=10, burst_count=2, burst_min_payload=1024, burst_min_total_payload=2048)
        mapper = self.mapper()
        for i in range(2):
            start = i * 20
            engine.observe([self.peer(uploaded=0)], start)
            engine.observe([self.peer(uploaded=2048)], start + 4)
            self.assertEqual(engine.ingest_etw(EtwReadEvent(ts=start + 4, path=r"\\testshare\Torrents\Movie\file.mkv", size=4096), mapper), 1)
            engine.close_missing(set(), start + 5)
        decisions = engine.decisions(45)
        self.assertEqual(len(decisions), 1)
        self.assertEqual(decisions[0].reason, "burst-reconnect-hdd-churn")

    def test_connected_burst_churn_bans_sporadic_active_peer(self):
        engine, _ = self.make_engine(
            threshold_time=999,
            connected_burst_count=2,
            connected_burst_min_payload=1024,
            connected_burst_min_total_payload=2048,
            connected_burst_max_duty_ratio=0.5,
        )
        mapper = self.mapper()
        uploaded = 0
        for poll in range(6):
            if poll in (1, 5):
                uploaded += 2048
            engine.observe([self.peer(uploaded=uploaded)], poll * 5)
            if poll in (1, 5):
                self.assertEqual(
                    engine.ingest_etw(EtwReadEvent(ts=poll * 5, path=r"\\testshare\Torrents\Movie\file.mkv", size=4096), mapper),
                    1,
                )
            decisions = engine.decisions(poll * 5)
        self.assertEqual(len(decisions), 1)
        self.assertEqual(decisions[0].reason, "connected-burst-hdd-churn")
        self.assertEqual(decisions[0].ip, "1.2.3.4")
        self.assertEqual(decisions[0].torrent_hashes, ("abc",))
        self.assertEqual(decisions[0].active_torrent_count, 1)

    def test_connected_burst_churn_ignores_high_duty_peer(self):
        engine, _ = self.make_engine(
            threshold_time=999,
            connected_burst_count=2,
            connected_burst_min_payload=1024,
            connected_burst_min_total_payload=2048,
            connected_burst_max_duty_ratio=0.5,
        )
        mapper = self.mapper()
        uploaded = 0
        decisions = []
        for poll in range(3):
            uploaded += 2048
            engine.observe([self.peer(uploaded=uploaded)], poll * 5)
            self.assertEqual(
                engine.ingest_etw(EtwReadEvent(ts=poll * 5, path=r"\\testshare\Torrents\Movie\file.mkv", size=4096), mapper),
                1,
            )
            decisions = engine.decisions(poll * 5)
        self.assertEqual(decisions, [])

    def test_connected_burst_churn_ignores_high_average_speed_peer(self):
        engine, _ = self.make_engine(
            threshold_time=999,
            connected_burst_count=2,
            connected_burst_min_payload=1024,
            connected_burst_min_total_payload=2048,
            connected_burst_max_duty_ratio=0.5,
            connected_burst_max_average_speed=2048,
        )
        mapper = self.mapper()
        uploaded = 0
        decisions = []
        for poll in range(6):
            if poll in (1, 5):
                uploaded += 2048
            engine.observe([self.peer(uploaded=uploaded, up_speed=4096)], poll * 5)
            if poll in (1, 5):
                self.assertEqual(
                    engine.ingest_etw(EtwReadEvent(ts=poll * 5, path=r"\\testshare\Torrents\Movie\file.mkv", size=4096), mapper),
                    1,
                )
            decisions = engine.decisions(poll * 5)
        self.assertEqual(decisions, [])

    def test_connected_burst_churn_skips_already_banned_this_cycle(self):
        engine, _ = self.make_engine(
            threshold_time=999,
            connected_burst_count=2,
            connected_burst_min_payload=1024,
            connected_burst_min_total_payload=2048,
            connected_burst_max_duty_ratio=0.5,
        )
        engine.state.record_ban("1.2.3.4:6881", 1.0, permanent_after=2)
        mapper = self.mapper()
        uploaded = 0
        for poll in range(6):
            if poll in (1, 5):
                uploaded += 2048
            engine.observe([self.peer(uploaded=uploaded)], poll * 5)
            if poll in (1, 5):
                engine.ingest_etw(EtwReadEvent(ts=poll * 5, path=r"\\testshare\Torrents\Movie\file.mkv", size=4096), mapper)
            decisions = engine.decisions(poll * 5)
        self.assertEqual(decisions, [])

    def test_connected_burst_churn_skips_productive_reputation(self):
        engine, _ = self.make_engine(
            threshold_time=999,
            connected_burst_count=2,
            connected_burst_min_payload=1024,
            connected_burst_min_total_payload=2048,
            connected_burst_max_duty_ratio=0.5,
        )
        engine.state.reputation("1.2.3.4:6881")["recent_productive_session_count"] = 1
        mapper = self.mapper()
        uploaded = 0
        for poll in range(6):
            if poll in (1, 5):
                uploaded += 2048
            engine.observe([self.peer(uploaded=uploaded)], poll * 5)
            if poll in (1, 5):
                engine.ingest_etw(EtwReadEvent(ts=poll * 5, path=r"\\testshare\Torrents\Movie\file.mkv", size=4096), mapper)
            decisions = engine.decisions(poll * 5)
        self.assertEqual(decisions, [])

    def test_long_term_low_speed_bans_despite_small_current_payload(self):
        engine, _ = self.make_engine(
            min_payload=1024 * 1024,
            long_term_low_speed_time=900,
            long_term_low_speed_min_etw_sessions=100,
            long_term_low_speed_min_etw_bytes=256 * 1024 * 1024,
            long_term_low_speed_min_uploaded=1024 * 1024,
        )
        rep = engine.state.reputation("1.2.3.4:6881")
        rep.update(
            {
                "total_low_speed_seconds": 1051.0,
                "etw_matched_session_count": 445,
                "total_etw_read_bytes": 353_959_936,
                "total_uploaded": 1_671_168,
                "last_medium_evidence": 100.0,
            }
        )
        mapper = self.mapper()
        engine.observe([self.peer(uploaded=0, up_speed=1200)], 0)
        engine.observe([self.peer(uploaded=393216, up_speed=1300)], 11)
        self.assertEqual(engine.ingest_etw(EtwReadEvent(ts=11, path=r"\\testshare\Torrents\Movie\file.mkv", size=4096), mapper), 1)
        decisions = engine.decisions(11)
        self.assertEqual(len(decisions), 1)
        self.assertEqual(decisions[0].reason, "long-term-low-speed")

    def test_long_term_low_speed_does_not_ban_on_startup_without_current_window_or_etw(self):
        engine, _ = self.make_engine(
            min_payload=1024 * 1024,
            long_term_low_speed_time=900,
            long_term_low_speed_min_etw_sessions=100,
            long_term_low_speed_min_etw_bytes=256 * 1024 * 1024,
            long_term_low_speed_min_uploaded=1024 * 1024,
        )
        rep = engine.state.reputation("1.2.3.4:6881")
        rep.update(
            {
                "reputation_score": 904.0,
                "total_low_speed_seconds": 1051.0,
                "etw_matched_session_count": 445,
                "total_etw_read_bytes": 353_959_936,
                "total_uploaded": 1_671_168,
                "last_medium_evidence": 100.0,
            }
        )
        engine.observe([self.peer(uploaded=0, up_speed=964)], 0)
        self.assertEqual(engine.decisions(0), [])

    def test_tiny_current_payload_with_reputation_is_not_near_miss_noise(self):
        engine, _ = self.make_engine(min_payload=1024 * 1024)
        rep = engine.state.reputation("1.2.3.4:6881")
        rep.update(
            {
                "reputation_score": 904.0,
                "total_low_speed_seconds": 1051.0,
                "etw_matched_session_count": 445,
                "total_etw_read_bytes": 353_959_936,
                "total_uploaded": 1_671_168,
                "last_medium_evidence": 100.0,
            }
        )
        engine.observe([self.peer(uploaded=0, up_speed=5000)], 0)
        engine.observe([self.peer(uploaded=32768, up_speed=5000)], 11)
        self.assertEqual(engine.decisions(11), [])
        self.assertEqual(engine.near_misses_last, [])

    def test_ip_activity_wake_churn_bans_same_ip_rotating_ports(self):
        engine, _ = self.make_engine(
            low_speed_threshold=16 * 1024,
            activity_wake_churn_after=99,
            ip_activity_wake_churn_after=3,
            ip_activity_wake_churn_window=3600,
            ip_activity_wake_churn_max_session_time=90,
            ip_activity_wake_churn_max_single_payload=64 * 1024,
            ip_activity_wake_churn_max_total_payload=256 * 1024,
            ip_activity_wake_churn_min_speed=10 * 1024,
            ip_activity_wake_churn_distinct_ports=3,
        )
        decisions = []
        for index, endpoint in enumerate(["1.2.3.4:50001", "1.2.3.4:50002", "1.2.3.4:50003"]):
            start = index * 30
            engine.observe([self.peer(uploaded=0, up_speed=0, endpoint=endpoint)], start)
            engine.observe([self.peer(uploaded=32 * 1024, up_speed=15 * 1024, endpoint=endpoint)], start + 5)
            engine.close_missing(set(), start + 6)
            decisions = engine.decisions(start + 6)

        self.assertEqual(len(decisions), 1)
        self.assertEqual(decisions[0].subject, "1.2.3.4")
        self.assertEqual(decisions[0].reason, "ip-activity-wake-churn")
        self.assertEqual(decisions[0].uploaded_delta, 96 * 1024)
        self.assertEqual(decisions[0].details["ip_churn_distinct_ports"], 3)

    def test_activity_wake_churn_bans_same_endpoint(self):
        engine, _ = self.make_engine(
            low_speed_threshold=16 * 1024,
            activity_wake_churn_after=3,
            ip_activity_wake_churn_after=3,
            ip_activity_wake_churn_window=3600,
            ip_activity_wake_churn_max_session_time=90,
            ip_activity_wake_churn_max_single_payload=64 * 1024,
            ip_activity_wake_churn_max_total_payload=256 * 1024,
            ip_activity_wake_churn_min_speed=10 * 1024,
            ip_activity_wake_churn_distinct_ports=3,
        )
        decisions = []
        for index in range(3):
            start = index * 30
            engine.observe([self.peer(uploaded=0, up_speed=0, endpoint="1.2.3.4:50001")], start)
            engine.observe([self.peer(uploaded=32 * 1024, up_speed=15 * 1024, endpoint="1.2.3.4:50001")], start + 5)
            engine.close_missing(set(), start + 6)
            decisions = engine.decisions(start + 6)

        self.assertEqual(len(decisions), 1)
        self.assertEqual(decisions[0].subject, "1.2.3.4:50001")
        self.assertEqual(decisions[0].reason, "activity-wake-churn")

    def test_activity_wake_churn_counts_tiny_fast_bursts(self):
        engine, _ = self.make_engine(
            low_speed_threshold=16 * 1024,
            activity_wake_churn_after=2,
            ip_activity_wake_churn_after=2,
            ip_activity_wake_churn_window=3600,
            ip_activity_wake_churn_max_session_time=90,
            ip_activity_wake_churn_max_single_payload=64 * 1024,
            ip_activity_wake_churn_max_total_payload=256 * 1024,
            ip_activity_wake_churn_min_speed=10 * 1024,
            ip_activity_wake_churn_distinct_ports=3,
        )
        decisions = []
        for index in range(2):
            start = index * 30
            engine.observe([self.peer(uploaded=0, up_speed=0, endpoint="1.2.3.4:50001")], start)
            engine.observe([self.peer(uploaded=16 * 1024, up_speed=30 * 1024, endpoint="1.2.3.4:50001")], start + 5)
            engine.close_missing(set(), start + 6)
            decisions = engine.decisions(start + 6)

        self.assertEqual(len(decisions), 1)
        self.assertEqual(decisions[0].reason, "activity-wake-churn")
        self.assertEqual(decisions[0].details["activity_wake_tiny_burst_events"], 2)

    def test_activity_wake_churn_counts_torrent_active_transition_with_speed_evidence(self):
        engine, _ = self.make_engine(
            low_speed_threshold=16 * 1024,
            activity_wake_churn_after=2,
            ip_activity_wake_churn_after=2,
            ip_activity_wake_churn_window=3600,
            ip_activity_wake_churn_max_session_time=90,
            ip_activity_wake_churn_max_single_payload=64 * 1024,
            ip_activity_wake_churn_max_total_payload=256 * 1024,
            ip_activity_wake_churn_min_speed=10 * 1024,
            ip_activity_wake_churn_distinct_ports=3,
        )
        decisions = []
        for index in range(2):
            start = index * 30
            engine.observe_active_torrents(set())
            engine.observe([], start)
            engine.observe_active_torrents({"abc"})
            engine.observe([self.peer(uploaded=0, up_speed=12 * 1024, endpoint="1.2.3.4:50001")], start + 5)
            engine.close_missing(set(), start + 6)
            decisions = engine.decisions(start + 6)

        self.assertEqual(len(decisions), 1)
        self.assertEqual(decisions[0].reason, "activity-wake-churn")
        self.assertEqual(decisions[0].uploaded_delta, 0)
        self.assertEqual(decisions[0].details["activity_wake_torrent_wake_events"], 2)

    def test_activity_wake_churn_does_not_count_passive_torrent_active_transition(self):
        engine, _ = self.make_engine(
            low_speed_threshold=16 * 1024,
            activity_wake_churn_after=1,
            ip_activity_wake_churn_after=99,
            ip_activity_wake_churn_window=3600,
            ip_activity_wake_churn_max_session_time=90,
            ip_activity_wake_churn_max_single_payload=64 * 1024,
            ip_activity_wake_churn_max_total_payload=256 * 1024,
            ip_activity_wake_churn_min_speed=10 * 1024,
            ip_activity_wake_churn_distinct_ports=3,
        )

        engine.observe_active_torrents(set())
        engine.observe([], 0)
        engine.observe_active_torrents({"abc"})
        engine.observe([self.peer(uploaded=0, up_speed=0, endpoint="1.2.3.4:50001")], 5)
        engine.close_missing(set(), 6)

        self.assertEqual(engine.decisions(6), [])

    def test_activity_wake_churn_does_not_count_equal_split_etw_as_peer_evidence(self):
        engine, _ = self.make_engine(
            low_speed_threshold=16 * 1024,
            activity_wake_churn_after=1,
            ip_activity_wake_churn_after=99,
            ip_activity_wake_churn_window=3600,
            ip_activity_wake_churn_max_session_time=90,
            ip_activity_wake_churn_max_single_payload=64 * 1024,
            ip_activity_wake_churn_max_total_payload=256 * 1024,
            ip_activity_wake_churn_min_speed=10 * 1024,
            ip_activity_wake_churn_distinct_ports=3,
        )
        mapper = self.mapper()

        engine.observe_active_torrents(set())
        engine.observe([], 0)
        engine.observe_active_torrents({"abc"})
        engine.observe(
            [
                self.peer(uploaded=0, up_speed=0, endpoint="1.2.3.4:50001"),
                self.peer(uploaded=0, up_speed=0, endpoint="5.6.7.8:50002", ip="5.6.7.8"),
            ],
            5,
        )
        self.assertEqual(engine.ingest_etw(EtwReadEvent(ts=5, path=r"\\testshare\Torrents\Movie\file.mkv", size=4096), mapper), 2)
        engine.close_missing(set(), 6)

        self.assertEqual(engine.decisions(6), [])

    def test_activity_wake_churn_weights_qbt_payload_and_lone_etw_highest(self):
        engine, _ = self.make_engine(
            low_speed_threshold=16 * 1024,
            activity_wake_churn_after=6,
            ip_activity_wake_churn_after=99,
            ip_activity_wake_churn_window=3600,
            ip_activity_wake_churn_max_session_time=90,
            ip_activity_wake_churn_max_single_payload=64 * 1024,
            ip_activity_wake_churn_max_total_payload=256 * 1024,
            ip_activity_wake_churn_min_speed=10 * 1024,
            ip_activity_wake_churn_distinct_ports=3,
        )
        mapper = self.mapper()

        engine.observe_active_torrents(set())
        engine.observe([], 0)
        engine.observe_active_torrents({"abc"})
        engine.observe([self.peer(uploaded=0, up_speed=0, endpoint="1.2.3.4:50001")], 0)
        engine.observe([self.peer(uploaded=16 * 1024, up_speed=30 * 1024, endpoint="1.2.3.4:50001")], 5)
        self.assertEqual(engine.ingest_etw(EtwReadEvent(ts=5, path=r"\\testshare\Torrents\Movie\file.mkv", size=4096), mapper), 1)
        engine.close_missing(set(), 6)
        decisions = engine.decisions(6)

        self.assertEqual(len(decisions), 1)
        self.assertEqual(decisions[0].reason, "activity-wake-churn")
        self.assertEqual(decisions[0].details["activity_wake_weighted_event_count"], 6)
        self.assertEqual(decisions[0].details["activity_wake_lone_peer_etw_events"], 1)
        self.assertIn("+5 qB payload + lone-peer ETW", decisions[0].details["activity_wake_weight_components"])

    def test_activity_wake_churn_weights_zero_upload_etw_highly(self):
        engine, _ = self.make_engine(
            low_speed_threshold=16 * 1024,
            activity_wake_churn_after=7,
            ip_activity_wake_churn_after=99,
            ip_activity_wake_churn_window=3600,
            ip_activity_wake_churn_max_session_time=90,
            ip_activity_wake_churn_max_single_payload=64 * 1024,
            ip_activity_wake_churn_max_total_payload=256 * 1024,
            ip_activity_wake_churn_min_speed=10 * 1024,
            ip_activity_wake_churn_distinct_ports=3,
        )
        mapper = self.mapper()

        engine.observe_active_torrents(set())
        engine.observe([], 0)
        engine.observe_active_torrents({"abc"})
        engine.observe([self.peer(uploaded=0, up_speed=12 * 1024, endpoint="1.2.3.4:50001")], 5)
        self.assertEqual(engine.ingest_etw(EtwReadEvent(ts=5, path=r"\\testshare\Torrents\Movie\file.mkv", size=1024), mapper), 1)
        engine.close_missing(set(), 6)
        self.assertEqual(engine.decisions(6), [])

        engine.observe_active_torrents(set())
        engine.observe([], 30)
        engine.observe_active_torrents({"abc"})
        engine.observe([self.peer(uploaded=0, up_speed=12 * 1024, endpoint="1.2.3.4:50001")], 35)
        engine.close_missing(set(), 36)
        decisions = engine.decisions(36)

        self.assertEqual(len(decisions), 1)
        self.assertEqual(decisions[0].reason, "activity-wake-churn")
        self.assertEqual(decisions[0].details["activity_wake_event_count"], 2)
        self.assertEqual(decisions[0].details["activity_wake_confidence"], "high")
        self.assertEqual(decisions[0].details["activity_wake_weighted_event_count"], 7)
        self.assertEqual(decisions[0].details["activity_wake_zero_upload_etw_events"], 1)

    def test_activity_wake_history_persists_across_engine_restart(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        config = GuardConfig(
            threshold_time=10,
            low_speed_threshold=16 * 1024,
            min_payload=1024,
            required_confidence="medium",
            activity_wake_churn_after=5,
            ip_activity_wake_churn_after=99,
            ip_activity_wake_churn_window=3600,
            ip_activity_wake_churn_max_session_time=90,
            ip_activity_wake_churn_max_single_payload=64 * 1024,
            ip_activity_wake_churn_max_total_payload=256 * 1024,
            ip_activity_wake_churn_min_speed=10 * 1024,
            ip_activity_wake_churn_distinct_ports=3,
        )
        state = GuardState.load(Path(tmp.name))
        engine = DecisionEngine(config, state)
        engine.observe_active_torrents(set())
        engine.observe([], 0)
        engine.observe_active_torrents({"abc"})
        engine.observe([self.peer(uploaded=0, up_speed=12 * 1024, endpoint="1.2.3.4:50001")], 5)
        engine.close_missing(set(), 6)
        self.assertEqual(engine.decisions(6), [])

        reloaded_state = GuardState.load(Path(tmp.name))
        reloaded_engine = DecisionEngine(config, reloaded_state)
        reloaded_engine.observe_active_torrents(set())
        reloaded_engine.observe([], 30)
        reloaded_engine.observe_active_torrents({"abc"})
        reloaded_engine.observe([self.peer(uploaded=0, up_speed=12 * 1024, endpoint="1.2.3.4:50001")], 35)
        reloaded_engine.close_missing(set(), 36)
        decisions = reloaded_engine.decisions(36)

        self.assertEqual(len(decisions), 1)
        self.assertEqual(decisions[0].reason, "activity-wake-churn")
        self.assertEqual(decisions[0].details["activity_wake_event_count"], 2)

    def test_activity_wake_history_is_seeded_from_existing_reputation_counts(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        state = GuardState.load(Path(tmp.name))
        rep = state.reputation("1.2.3.4:50001")
        rep.update(
            {
                "activity_wake_churn_session_count": 1,
                "last_seen": 100.0,
                "total_uploaded": 0,
                "total_etw_read_bytes": 1024,
            }
        )
        state.save()

        reloaded = GuardState.load(Path(tmp.name))
        history = reloaded.activity_wake_history()
        events = history["by_endpoint"]["1.2.3.4:50001"]
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["wake_weight"], 2)

    def test_ip_activity_wake_churn_requires_three_distinct_ports(self):
        engine, _ = self.make_engine(
            low_speed_threshold=16 * 1024,
            activity_wake_churn_after=99,
            ip_activity_wake_churn_after=3,
            ip_activity_wake_churn_window=3600,
            ip_activity_wake_churn_max_session_time=90,
            ip_activity_wake_churn_max_single_payload=64 * 1024,
            ip_activity_wake_churn_max_total_payload=256 * 1024,
            ip_activity_wake_churn_min_speed=10 * 1024,
            ip_activity_wake_churn_distinct_ports=3,
        )
        decisions = []
        for index, endpoint in enumerate(["1.2.3.4:50001", "1.2.3.4:50002", "1.2.3.4:50001"]):
            start = index * 30
            engine.observe([self.peer(uploaded=0, up_speed=0, endpoint=endpoint)], start)
            engine.observe([self.peer(uploaded=32 * 1024, up_speed=15 * 1024, endpoint=endpoint)], start + 5)
            engine.close_missing(set(), start + 6)
            decisions = engine.decisions(start + 6)

        self.assertEqual(decisions, [])

    def test_ip_activity_wake_churn_bans_when_all_ports_are_distinct_over_shortcut(self):
        engine, _ = self.make_engine(
            low_speed_threshold=16 * 1024,
            activity_wake_churn_after=99,
            ip_activity_wake_churn_after=99,
            ip_activity_wake_churn_all_distinct_ports_over=3,
            ip_activity_wake_churn_window=3600,
            ip_activity_wake_churn_max_session_time=90,
            ip_activity_wake_churn_max_single_payload=64 * 1024,
            ip_activity_wake_churn_max_total_payload=256 * 1024,
            ip_activity_wake_churn_min_speed=10 * 1024,
            ip_activity_wake_churn_distinct_ports=3,
        )
        decisions = []
        for index, endpoint in enumerate(["1.2.3.4:50001", "1.2.3.4:50002", "1.2.3.4:50003", "1.2.3.4:50004"]):
            start = index * 30
            engine.observe_active_torrents(set())
            engine.observe([], start)
            engine.observe_active_torrents({"abc"})
            engine.observe([self.peer(uploaded=0, up_speed=12 * 1024, endpoint=endpoint)], start + 5)
            engine.close_missing(set(), start + 6)
            decisions = engine.decisions(start + 6)

        self.assertEqual(len(decisions), 1)
        self.assertEqual(decisions[0].subject, "1.2.3.4")
        self.assertEqual(decisions[0].reason, "ip-activity-wake-churn")
        self.assertTrue(decisions[0].details["ip_churn_all_distinct_shortcut"])

    def test_ip_activity_wake_churn_all_distinct_shortcut_rejects_reused_port(self):
        engine, _ = self.make_engine(
            low_speed_threshold=16 * 1024,
            activity_wake_churn_after=99,
            ip_activity_wake_churn_after=99,
            ip_activity_wake_churn_all_distinct_ports_over=3,
            ip_activity_wake_churn_window=3600,
            ip_activity_wake_churn_max_session_time=90,
            ip_activity_wake_churn_max_single_payload=64 * 1024,
            ip_activity_wake_churn_max_total_payload=256 * 1024,
            ip_activity_wake_churn_min_speed=10 * 1024,
            ip_activity_wake_churn_distinct_ports=3,
        )
        decisions = []
        for index, endpoint in enumerate(["1.2.3.4:50001", "1.2.3.4:50002", "1.2.3.4:50003", "1.2.3.4:50001"]):
            start = index * 30
            engine.observe_active_torrents(set())
            engine.observe([], start)
            engine.observe_active_torrents({"abc"})
            engine.observe([self.peer(uploaded=0, up_speed=12 * 1024, endpoint=endpoint)], start + 5)
            engine.close_missing(set(), start + 6)
            decisions = engine.decisions(start + 6)

        self.assertEqual(decisions, [])

    def test_ip_activity_wake_zero_upload_etw_near_miss_is_recorded(self):
        engine, _ = self.make_engine(
            low_speed_threshold=16 * 1024,
            activity_wake_churn_after=99,
            ip_activity_wake_churn_after=20,
            ip_activity_wake_churn_window=3600,
            ip_activity_wake_churn_max_session_time=90,
            ip_activity_wake_churn_max_single_payload=64 * 1024,
            ip_activity_wake_churn_max_total_payload=256 * 1024,
            ip_activity_wake_churn_min_speed=10 * 1024,
            ip_activity_wake_churn_distinct_ports=3,
        )
        mapper = self.mapper()
        decisions = []
        for index, endpoint in enumerate(["1.2.3.4:50001", "1.2.3.4:50002", "1.2.3.4:50003"]):
            start = index * 30
            engine.observe_active_torrents(set())
            engine.observe([], start)
            engine.observe_active_torrents({"abc"})
            engine.observe([self.peer(uploaded=0, up_speed=12 * 1024, endpoint=endpoint)], start + 5)
            if index == 0:
                self.assertEqual(
                    engine.ingest_etw(EtwReadEvent(ts=start + 5, path=r"\\testshare\Torrents\Movie\file.mkv", size=1024), mapper),
                    1,
                )
            engine.close_missing(set(), start + 6)
            decisions = engine.decisions(start + 6)

        self.assertEqual(decisions, [])
        near_misses = [item for item in engine.near_misses_last if item.get("reason") == "ip-activity-wake-churn-near-miss"]
        self.assertEqual(len(near_misses), 1)
        self.assertEqual(near_misses[0]["subject"], "1.2.3.4")
        self.assertEqual(near_misses[0]["decision_details"]["activity_wake_weighted_event_count"], 12)
        self.assertEqual(near_misses[0]["decision_details"]["activity_wake_confidence"], "high")
        self.assertEqual(near_misses[0]["decision_details"]["activity_wake_zero_upload_etw_events"], 1)

    def mapper(self) -> PathMapper:
        mapper = PathMapper()
        mapper.update_torrent(
            TorrentInfo(
                hash="abc",
                name="Movie",
                content_path=r"\\testshare\Torrents\Movie",
                save_path=r"\\testshare\Torrents",
                state="uploading",
                amount_left=0,
            ),
            [{"index": 0, "name": r"Movie\file.mkv"}],
        )
        return mapper

    def peer(
        self,
        *,
        uploaded: int,
        up_speed: int = 100,
        endpoint: str = "1.2.3.4:6881",
        ip: str = "1.2.3.4",
    ) -> PeerSnapshot:
        return PeerSnapshot(
            torrent_hash="abc",
            torrent_name="Movie",
            endpoint=endpoint,
            ip=ip,
            port=6881,
            up_speed=up_speed,
            uploaded=uploaded,
            client="test",
            files=("0",),
        )


if __name__ == "__main__":
    unittest.main()
