from __future__ import annotations

import json
import logging
import tempfile
import unittest
from pathlib import Path

from qbt_hdd_guard.models import BanDecision, GuardConfig
from qbt_hdd_guard.monitor import GuardMonitor
from qbt_hdd_guard.state import GuardState


class MonitorAuditTests(unittest.TestCase):
    def test_permanent_check_does_not_create_zero_count_ban_record(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        state = GuardState.load(Path(tmp.name))

        self.assertFalse(state.is_permanent("1.2.3.4:6881"))
        state.save()

        self.assertEqual(state.data["bans"], {})
        self.assertEqual((Path(tmp.name) / "banned-clients.txt").read_text(encoding="utf-8"), "")

    def test_current_cycle_ban_check_does_not_create_record(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        state = GuardState.load(Path(tmp.name))

        self.assertFalse(state.is_banned_this_cycle("1.2.3.4:6881"))

        self.assertEqual(state.data["bans"], {})

    def test_banned_clients_compat_file_omits_zero_count_records(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        state = GuardState.load(Path(tmp.name))
        state.data["bans"]["1.2.3.4:6881"] = {}
        state.data["bans"]["5.6.7.8:6881"] = {"count": 2}

        state.save()

        self.assertEqual((Path(tmp.name) / "banned-clients.txt").read_text(encoding="utf-8"), "5.6.7.8:6881,2\n")

    def test_load_prunes_empty_ban_records_from_existing_state(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        state_path = Path(tmp.name) / "hdd-guard-state.json"
        state_path.write_text(
            json.dumps(
                {
                    "version": 1,
                    "bans": {
                        "1.2.3.4:6881": {},
                        "5.6.7.8:6881": {"count": 1},
                    },
                }
            ),
            encoding="utf-8",
        )

        state = GuardState.load(Path(tmp.name))

        self.assertNotIn("1.2.3.4:6881", state.data["bans"])
        self.assertIn("5.6.7.8:6881", state.data["bans"])

    def test_load_prunes_empty_ip_reputation_from_existing_state(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        state_path = Path(tmp.name) / "hdd-guard-state.json"
        state_path.write_text(
            json.dumps(
                {
                    "version": 1,
                    "ip_reputation": {
                        "1.2.3.4": {
                            "distinct_bad_endpoints": [],
                            "distinct_endpoints": ["1.2.3.4:6881"],
                            "ip_reputation_score": 0.0,
                            "last_bad_seen": None,
                            "total_bad_sessions": 0,
                            "total_etw_matched_sessions": 0,
                        },
                        "": {
                            "distinct_bad_endpoints": ["1.2.3.4:6881"],
                            "distinct_endpoints": ["1.2.3.4:6881"],
                            "ip_reputation_score": 0.0,
                            "last_bad_seen": 100.0,
                            "total_bad_sessions": 0,
                            "total_etw_matched_sessions": 0,
                        },
                        "5.6.7.8": {
                            "distinct_bad_endpoints": ["5.6.7.8:6881"],
                            "distinct_endpoints": ["5.6.7.8:6881"],
                            "ip_reputation_score": 1.0,
                            "last_bad_seen": 100.0,
                            "total_bad_sessions": 1,
                            "total_etw_matched_sessions": 1,
                        },
                    },
                }
            ),
            encoding="utf-8",
        )

        state = GuardState.load(Path(tmp.name))

        self.assertNotIn("1.2.3.4", state.data["ip_reputation"])
        self.assertNotIn("", state.data["ip_reputation"])
        self.assertIn("5.6.7.8", state.data["ip_reputation"])

    def test_load_prunes_zero_payload_reputation_from_existing_state(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        state_path = Path(tmp.name) / "hdd-guard-state.json"
        state_path.write_text(
            json.dumps(
                {
                    "version": 1,
                    "reputation": {
                        "1.2.3.4:6881": {
                            "session_count": 1,
                            "low_speed_session_count": 1,
                            "reputation_score": 2.0,
                            "total_uploaded": 0,
                            "total_etw_read_bytes": 0,
                        },
                        "5.6.7.8:6881": {
                            "session_count": 1,
                            "bad_session_count": 1,
                            "reputation_score": 5.0,
                            "total_uploaded": 1048576,
                            "total_etw_read_bytes": 0,
                        },
                    },
                }
            ),
            encoding="utf-8",
        )

        state = GuardState.load(Path(tmp.name))

        self.assertNotIn("1.2.3.4:6881", state.data["reputation"])
        self.assertIn("5.6.7.8:6881", state.data["reputation"])

    def test_apply_decision_writes_ban_audit_jsonl(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        state = GuardState.load(Path(tmp.name))
        config = GuardConfig(dry_run=True, state_dir=Path(tmp.name))
        monitor = GuardMonitor(
            client=object(),
            config=config,
            state=state,
            logger=logging.getLogger("test-ban-audit"),
            clock=lambda: 1234.0,
        )
        decision = BanDecision(
            subject="1.2.3.4:6881",
            endpoint="1.2.3.4:6881",
            ip="1.2.3.4",
            torrent_hashes=("abc",),
            reason="extreme-low-speed",
            score=8,
            confidence="none",
            average_speed=3072.0,
            max_speed=4096,
            uploaded_delta=1048576,
            etw_bytes=0,
            etw_count=0,
            active_torrent_count=1,
            score_components=("+5 extreme low speed for long window",),
        )

        monitor.apply_decision(decision)

        lines = state.ban_audit_file.read_text(encoding="utf-8").splitlines()
        self.assertEqual(len(lines), 1)
        record = json.loads(lines[0])
        self.assertEqual(record["subject"], "1.2.3.4:6881")
        self.assertEqual(record["reason"], "extreme-low-speed")
        self.assertEqual(record["ban_count"], 1)
        self.assertTrue(record["counted_in_unban_cycle"])
        self.assertFalse(record["promoted_to_permanent"])
        self.assertEqual(record["score_components"], ["+5 extreme low speed for long window"])
        self.assertEqual(record["ban_record"]["count"], 1)
        self.assertIn("endpoint_reputation", record)

    def test_near_miss_audit_is_written_and_throttled(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        state = GuardState.load(Path(tmp.name))
        config = GuardConfig(dry_run=True, near_miss_log_interval=300, near_miss_startup_grace=0, state_dir=Path(tmp.name))
        monitor = GuardMonitor(
            client=object(),
            config=config,
            state=state,
            logger=logging.getLogger("test-near-miss-audit"),
            clock=lambda: 1000.0,
        )
        monitor.engine.near_misses_last = [
            {
                "subject": "1.2.3.4:6881",
                "endpoint": "1.2.3.4:6881",
                "ip": "1.2.3.4",
                "reason": "near-miss",
                "score": 7,
                "safety_blocks": ["uploaded payload below --min-payload (512 < 1024)"],
                "unmet_criteria": ["speed-only score below threshold (7 < 10)"],
            }
        ]

        monitor._write_near_miss_audits(1000.0)
        monitor.engine.near_misses_last = [
            {
                "subject": "1.2.3.4:6881",
                "endpoint": "1.2.3.4:6881",
                "ip": "1.2.3.4",
                "reason": "near-miss",
                "score": 8,
                "safety_blocks": ["uploaded payload below --min-payload (900 < 1024)"],
                "unmet_criteria": ["speed-only score below threshold (8 < 10)"],
            }
        ]
        monitor._write_near_miss_audits(1100.0)

        lines = state.near_miss_audit_file.read_text(encoding="utf-8").splitlines()
        self.assertEqual(len(lines), 1)
        record = json.loads(lines[0])
        self.assertEqual(record["subject"], "1.2.3.4:6881")
        self.assertEqual(record["score"], 7)
        self.assertEqual(record["safety_blocks"], ["uploaded payload below --min-payload (512 < 1024)"])

    def test_near_miss_audit_respects_startup_grace(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        state = GuardState.load(Path(tmp.name))
        config = GuardConfig(
            dry_run=True,
            near_miss_log_interval=300,
            near_miss_startup_grace=60,
            state_dir=Path(tmp.name),
        )
        monitor = GuardMonitor(
            client=object(),
            config=config,
            state=state,
            logger=logging.getLogger("test-near-miss-startup-grace"),
            clock=lambda: 1000.0,
        )
        monitor.engine.near_misses_last = [
            {
                "subject": "1.2.3.4:6881",
                "endpoint": "1.2.3.4:6881",
                "ip": "1.2.3.4",
                "score": 7,
                "safety_blocks": [],
                "unmet_criteria": ["slow-hdd score below threshold (7 < 8)"],
            }
        ]

        monitor._write_near_miss_audits(1030.0)
        self.assertFalse(state.near_miss_audit_file.exists())

        monitor._write_near_miss_audits(1061.0)
        lines = state.near_miss_audit_file.read_text(encoding="utf-8").splitlines()
        self.assertEqual(len(lines), 1)


if __name__ == "__main__":
    unittest.main()
