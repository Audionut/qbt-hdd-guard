from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from qbt_hdd_guard.cli import build_parser, default_etw_helper, resolve_from_root


class CliPathTests(unittest.TestCase):
    def test_relative_paths_resolve_from_runtime_root(self):
        root = Path(tempfile.mkdtemp())
        self.assertEqual(resolve_from_root("state", root), root / "state")

    def test_absolute_paths_stay_absolute(self):
        root = Path(tempfile.mkdtemp())
        absolute = root / "custom" / "state"
        self.assertEqual(resolve_from_root(str(absolute), Path("C:/other")), absolute)

    def test_default_etw_helper_prefers_release_build_output(self):
        root = Path(tempfile.mkdtemp())
        helper = root / "etw-helper" / "bin" / "Release" / "net8.0" / "QbtEtwHelper.exe"
        helper.parent.mkdir(parents=True)
        helper.write_text("", encoding="utf-8")
        self.assertEqual(default_etw_helper(root), helper)

    def test_speed_only_is_enabled_by_default(self):
        args = build_parser().parse_args([])
        self.assertTrue(args.allow_speed_only_bans)
        self.assertFalse(args.etw_required)

    def test_speed_only_and_etw_can_be_made_strict(self):
        args = build_parser().parse_args(["--no-speed-only-bans", "--require-etw"])
        self.assertFalse(args.allow_speed_only_bans)
        self.assertTrue(args.etw_required)


if __name__ == "__main__":
    unittest.main()
