from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from .models import TorrentInfo


class PathMapper:
    def __init__(self) -> None:
        self._files_by_torrent: dict[str, dict[str, set[str]]] = {}
        self._index_by_torrent: dict[str, dict[str, set[str]]] = {}
        self._watch_roots_by_torrent: dict[str, set[str]] = {}

    def update_torrent(self, torrent: TorrentInfo, files: list[Any]) -> None:
        absolute: set[str] = set()
        relative: set[str] = set()
        index_map: dict[str, set[str]] = {}
        watch_roots = {_norm_path(value) for value in (torrent.content_path, torrent.save_path) if value}
        for idx, item in enumerate(files):
            name = str(_field(item, "name", "") or "")
            if not name:
                continue
            rel = _norm_relative(name)
            relative.add(rel)
            candidates = _path_candidates(torrent, name)
            absolute.update(candidates)
            file_index = str(_field(item, "index", idx))
            index_map[file_index] = {rel, *candidates}
        self._files_by_torrent[torrent.hash] = {"absolute": absolute, "relative": relative}
        self._index_by_torrent[torrent.hash] = index_map
        self._watch_roots_by_torrent[torrent.hash] = watch_roots

    def watch_roots(self) -> set[str]:
        return {root for roots in self._watch_roots_by_torrent.values() for root in roots}

    def path_matches_session(self, etw_path: str, torrent_hash: str, peer_files: tuple[Any, ...]) -> bool:
        normalized_path = _norm_path(etw_path)
        torrent_files = self._files_by_torrent.get(torrent_hash)
        if not torrent_files:
            return False

        peer_file_set = self._normalize_peer_files(torrent_hash, peer_files)
        if peer_file_set:
            return any(_path_matches(normalized_path, value) for value in peer_file_set)

        return any(_path_matches(normalized_path, value) for value in torrent_files["absolute"]) or any(
            normalized_path.endswith(value) for value in torrent_files["relative"]
        )

    def _normalize_peer_files(self, torrent_hash: str, peer_files: tuple[Any, ...]) -> set[str]:
        values: set[str] = set()
        index_map = self._index_by_torrent.get(torrent_hash, {})
        for value in _flatten_peer_files(peer_files):
            raw = str(value).strip()
            if not raw:
                continue
            if raw in index_map:
                values.update(index_map[raw])
                continue
            values.add(_norm_relative(raw))
            values.add(_norm_path(raw))
        return values


def _path_candidates(torrent: TorrentInfo, name: str) -> set[str]:
    candidates: set[str] = set()
    rel = name.replace("/", os.sep).replace("\\", os.sep)
    if torrent.save_path:
        candidates.add(_norm_path(str(Path(torrent.save_path) / rel)))
    if torrent.content_path:
        content = Path(torrent.content_path)
        if content.name.lower() == Path(rel).name.lower():
            candidates.add(_norm_path(str(content)))
        candidates.add(_norm_path(str(content / rel)))
    return candidates


def _flatten_peer_files(values: tuple[Any, ...]) -> list[Any]:
    out: list[Any] = []
    for value in values:
        if isinstance(value, (list, tuple, set)):
            out.extend(_flatten_peer_files(tuple(value)))
        elif isinstance(value, str):
            parts = [part.strip() for part in value.replace("|", ",").split(",")]
            out.extend(part for part in parts if part)
        else:
            out.append(value)
    return out


def _path_matches(path: str, candidate: str) -> bool:
    candidate = candidate.strip()
    if not candidate:
        return False
    return path == candidate or path.endswith("\\" + candidate) or path.endswith("/" + candidate)


def _norm_path(value: str) -> str:
    return os.path.normcase(os.path.normpath(value.replace("/", os.sep).replace("\\", os.sep)))


def _norm_relative(value: str) -> str:
    return os.path.normcase(os.path.normpath(value.replace("/", os.sep).replace("\\", os.sep))).lstrip("\\/")


def _field(value: Any, name: str, default: Any = None) -> Any:
    if isinstance(value, dict):
        return value.get(name, default)
    return getattr(value, name, default)
