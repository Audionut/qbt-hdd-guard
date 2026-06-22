from __future__ import annotations

import json
import logging
import queue
import subprocess
import threading
import time
from pathlib import Path

from .models import EtwReadEvent


class EtwCollector:
    def __init__(self, helper: Path, *, logger: logging.Logger, restart: bool = True) -> None:
        self.helper = helper
        self.logger = logger
        self.restart = restart
        self._events: queue.Queue[EtwReadEvent] = queue.Queue()
        self._process: subprocess.Popen[str] | None = None
        self._thread: threading.Thread | None = None
        self._last_start = 0.0
        self._watch_paths: set[str] = set()

    def start(self) -> None:
        if not self.helper.exists():
            self.logger.error("ETW helper not found: %s", self.helper)
            return
        if self._process and self._process.poll() is None:
            return
        now = time.time()
        if now - self._last_start < 5:
            return
        self._last_start = now
        self._process = subprocess.Popen(
            [str(self.helper)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            creationflags=subprocess.CREATE_NO_WINDOW,
        )
        self._thread = threading.Thread(target=self._read_stdout, daemon=True)
        self._thread.start()
        threading.Thread(target=self._read_stderr, daemon=True).start()
        self._sync_watch_paths()
        self.logger.info("ETW helper started: %s", self.helper)

    def set_watch_paths(self, paths: set[str]) -> None:
        normalized = {str(path).strip() for path in paths if str(path).strip()}
        if normalized == self._watch_paths:
            return
        self._watch_paths = normalized
        self._sync_watch_paths()

    def drain(self) -> list[EtwReadEvent]:
        if self.restart and (self._process is None or self._process.poll() is not None):
            if self._process is not None:
                self.logger.warning("ETW helper exited with code %s", self._process.poll())
            self.start()
        events: list[EtwReadEvent] = []
        while True:
            try:
                events.append(self._events.get_nowait())
            except queue.Empty:
                return events

    def stop(self) -> None:
        if not self._process or self._process.poll() is not None:
            return

        try:
            if self._process.stdin:
                self._process.stdin.write("quit\n")
                self._process.stdin.flush()
        except OSError:
            pass

        try:
            self._process.wait(timeout=5)
            self.logger.info("ETW helper stopped")
            return
        except subprocess.TimeoutExpired:
            self.logger.warning("ETW helper did not stop after quit request; terminating")

        self._process.terminate()
        try:
            self._process.wait(timeout=5)
            self.logger.info("ETW helper terminated")
            return
        except subprocess.TimeoutExpired:
            self.logger.warning("ETW helper did not terminate; killing")
            self._process.kill()
            self._process.wait(timeout=5)
            self.logger.info("ETW helper killed")

    def _sync_watch_paths(self) -> None:
        if not self._process or self._process.poll() is not None or not self._process.stdin:
            return
        try:
            self._process.stdin.write("clear-watch\n")
            for path in sorted(self._watch_paths):
                self._process.stdin.write(f"watch\t{path}\n")
            self._process.stdin.flush()
        except OSError:
            pass

    def _read_stdout(self) -> None:
        assert self._process is not None
        assert self._process.stdout is not None
        for line in self._process.stdout:
            try:
                data = json.loads(line)
                path = str(data.get("path", "") or "")
                if not path:
                    continue
                self._events.put(
                    EtwReadEvent(
                        ts=float(data.get("ts", time.time()) or time.time()),
                        path=path,
                        size=int(data.get("size", 0) or 0),
                        process=str(data.get("process", "qbittorrent.exe") or "qbittorrent.exe"),
                        op=str(data.get("op", "Read") or "Read"),
                        pid=_optional_int(data.get("pid")),
                        offset=_optional_int(data.get("offset")),
                        is_qbt=bool(data.get("is_qbt", True)),
                    )
                )
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                self.logger.debug("ignored bad ETW event: %s", exc)

    def _read_stderr(self) -> None:
        assert self._process is not None
        assert self._process.stderr is not None
        for line in self._process.stderr:
            self._log_stderr(line.rstrip())

    def _log_stderr(self, message: str) -> None:
        if message.startswith("status "):
            self.logger.debug("ETW helper: %s", message)
        elif message == "shutdown requested":
            self.logger.info("ETW helper: %s", message)
        else:
            self.logger.warning("ETW helper: %s", message)


def _optional_int(value: object) -> int | None:
    if value in (None, ""):
        return None
    return int(value)
