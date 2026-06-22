from __future__ import annotations

import logging
import unittest
from pathlib import Path

from qbt_hdd_guard.etw import EtwCollector


class EtwLogTests(unittest.TestCase):
    def test_helper_status_is_not_warning(self):
        logger = logging.getLogger("test-helper-status")
        logger.handlers.clear()
        logger.setLevel(logging.DEBUG)
        collector = EtwCollector(Path("helper.exe"), logger=logger)
        with self.assertLogs(logger, level="DEBUG") as logs:
            collector._log_stderr("status raw_reads=1")
            collector._log_stderr("shutdown requested")
            collector._log_stderr("real error")

        self.assertIn("DEBUG:test-helper-status:ETW helper: status raw_reads=1", logs.output)
        self.assertIn("INFO:test-helper-status:ETW helper: shutdown requested", logs.output)
        self.assertIn("WARNING:test-helper-status:ETW helper: real error", logs.output)


if __name__ == "__main__":
    unittest.main()
