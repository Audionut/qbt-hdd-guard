from __future__ import annotations

from typing import Any

import qbittorrentapi

from .models import ConnectionConfig, PeerSnapshot, TorrentInfo


def build_client(config: ConnectionConfig) -> qbittorrentapi.Client:
    return qbittorrentapi.Client(
        host=config.host,
        port=config.port,
        username=config.username,
        password=config.password,
        VERIFY_WEBUI_CERTIFICATE=config.verify_webui_certificate,
        REQUESTS_ARGS={"timeout": config.timeout},
    )


class QbtPoller:
    def __init__(self, client: qbittorrentapi.Client) -> None:
        self.client = client

    def wait_until_ready(self) -> str:
        return str(self.client.app_version())

    def active_completed_torrents(self) -> list[TorrentInfo]:
        torrents = self.client.torrents_info(status_filter="active")
        out: list[TorrentInfo] = []
        for torrent in torrents:
            amount_left = int(_field(torrent, "amount_left", 0) or 0)
            torrent_hash = str(_field(torrent, "hash", "") or "")
            if amount_left != 0 or not torrent_hash:
                continue
            out.append(
                TorrentInfo(
                    hash=torrent_hash,
                    name=str(_field(torrent, "name", torrent_hash) or torrent_hash),
                    content_path=str(_field(torrent, "content_path", "") or ""),
                    save_path=str(_field(torrent, "save_path", "") or ""),
                    state=str(_field(torrent, "state", "") or ""),
                    amount_left=amount_left,
                )
            )
        return out

    def torrent_files(self, torrent_hash: str) -> list[Any]:
        return list(self.client.torrents_files(torrent_hash=torrent_hash))

    def peer_snapshots(self, torrent: TorrentInfo) -> list[PeerSnapshot]:
        data = self.client.sync_torrent_peers(torrent_hash=torrent.hash)
        peers = _field(data, "peers", {}) or {}
        if not isinstance(peers, dict):
            return []
        snapshots: list[PeerSnapshot] = []
        for endpoint, info in peers.items():
            endpoint_s = str(endpoint)
            ip, port = split_endpoint(endpoint_s, info)
            snapshots.append(
                PeerSnapshot(
                    torrent_hash=torrent.hash,
                    torrent_name=torrent.name,
                    endpoint=endpoint_s,
                    ip=ip,
                    port=port,
                    up_speed=int(_field(info, "up_speed", 0) or 0),
                    uploaded=int(_field(info, "uploaded", 0) or 0),
                    client=str(_field(info, "client", _field(info, "peer_id_client", "")) or ""),
                    files=tuple(_coerce_files(_field(info, "files", ()))),
                )
            )
        return snapshots


def ban_endpoint(client: qbittorrentapi.Client, endpoint: str) -> None:
    client.transfer_ban_peers(peers=endpoint)


def set_bare_ip_bans(client: qbittorrentapi.Client, ips: list[str]) -> None:
    client.app_set_preferences(prefs={"banned_IPs": "\n".join(sorted(set(ips)))})


def add_bare_ip_bans(client: qbittorrentapi.Client, ips: list[str]) -> None:
    prefs = client.app_preferences()
    current = _split_banned_ips(str(_field(prefs, "banned_IPs", "") or ""))
    current.update(ips)
    client.app_set_preferences(prefs={"banned_IPs": "\n".join(sorted(current))})


def split_endpoint(endpoint: str, info: Any | None = None) -> tuple[str, int | None]:
    ip = str(_field(info, "ip", "") or "")
    port_value = _field(info, "port", None)
    if ip:
        return ip, int(port_value) if port_value not in (None, "") else _port_from_endpoint(endpoint)
    if endpoint.startswith("[") and "]:" in endpoint:
        host, _, port = endpoint[1:].partition("]:")
        return host, int(port) if port.isdigit() else None
    host, sep, port = endpoint.rpartition(":")
    if sep and port.isdigit():
        return host, int(port)
    return endpoint, None


def _coerce_files(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return list(value)
    return [value]


def _port_from_endpoint(endpoint: str) -> int | None:
    _, sep, port = endpoint.rpartition(":")
    return int(port) if sep and port.isdigit() else None


def _split_banned_ips(value: str) -> set[str]:
    return {part.strip() for line in value.splitlines() for part in line.split(",") if part.strip()}


def _field(value: Any, name: str, default: Any = None) -> Any:
    if isinstance(value, dict):
        return value.get(name, default)
    return getattr(value, name, default)
