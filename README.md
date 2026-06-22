# qbt-hdd-guard

Windows-native qBittorrent peer guard focused on HDD protection.

It polls only active completed torrents, tracks peers by `IP:port`, correlates qB peer/file telemetry with optional Windows ETW file-read events, then bans peers that show repeated low-value HDD churn. It is not meant to ban every slow peer.

For the default `--etw` behavior and ban rules, see [DEFAULT_ETW_OVERVIEW.md](DEFAULT_ETW_OVERVIEW.md).

## Clone

From PowerShell:

```powershell
git clone <repo-url>
cd qbit-peer-hhd-churn-checker
python -m pip install -r requirements.txt
```

The repo is self-contained for normal script use:

```text
run_hdd_guard.py        direct runner
qbt_hdd_guard\          Python package
etw-helper\             ETW helper source plus prebuilt helper bundle
state\                  runtime state/log folder, created on first run and ignored by Git
tests\                  unit tests
```

The program resolves relative paths from the repo/script folder. That means the default `state\` folder and default ETW helper lookup continue to work if the whole repo is moved.

## Install

From PowerShell:

```powershell
cd path\to\qbit-peer-hhd-churn-checker
python -m pip install -r requirements.txt
```

No venv required.

Python requirements:

```text
Python >= 3.10
qbittorrent-api == 2026.6.0
qBittorrent Web UI enabled and reachable
```

Install `requirements.txt` with the same Python executable used to run the script. The only Python package dependency is `qbittorrent-api`; the ETW helper is separate and requires .NET 8 runtime only when `--etw` is used.

## Run Directly From Script

Conservative ETW-backed mode, dry-run first:

```powershell
cd path\to\qbit-peer-hhd-churn-checker
python .\run_hdd_guard.py --host localhost --port 8080 --username admin --password "YOUR_PASSWORD" --etw --dry-run
```

Actual bans:

```powershell
python .\run_hdd_guard.py --host localhost --port 8080 --username admin --password "YOUR_PASSWORD" --etw
```

Without ETW, conservative speed-only fallback bans are enabled by default:

```powershell
python .\run_hdd_guard.py --host localhost --port 8080 --username admin --password "YOUR_PASSWORD"
```

Strict ETW-only mode:

```powershell
python .\run_hdd_guard.py --host localhost --port 8080 --username admin --password "YOUR_PASSWORD" --etw --require-etw --no-speed-only-bans
```

## How It Works

Each poll asks qBittorrent Web UI for active completed torrents, then fetches peers only for those active torrents. The guard tracks sessions by peer endpoint (`IP:port`) and bare IP. It looks for patterns that are likely to create HDD churn without useful upload, including:

```text
slow HDD-churn with ETW/read amplification
extreme long low-speed sessions
short burst reconnects
connected sporadic bursts
activity-wake churn from rotating ports
bare-IP escalation when many ports behave badly
```

With `--etw`, the script starts the bundled ETW helper from the default repo location and watches Windows file-read events for qBittorrent. The Python process maps those reads back to active torrent files and weighs evidence against qB peer upload/speed data. Without `--etw`, only conservative speed/reputation fallback paths are available.

## qBittorrent Polling

`--poll-interval` controls how often the script asks qBittorrent for active completed torrents and then peer lists for those active torrents. It does not scan every torrent in qBittorrent.

Default:

```text
--poll-interval 5
```

Lower values, for example `2` or `3` seconds:

```text
catch shorter connect/disconnect behavior
give better timing for burst/activity-wake detection
improve ETW-to-peer attribution because upload deltas are fresher
increase qB Web UI API calls, Python CPU work, and debug log volume
can make noisy peers accumulate evidence faster
```

Higher values, for example `10` or `15` seconds:

```text
reduce qB Web UI/API overhead
reduce log volume
can miss peers that connect and disconnect between polls
make short-burst/activity-wake behavior harder to prove
delay bans because fewer observations are collected per hour
make ETW attribution less precise because more peer changes can happen between samples
```

Thresholds such as `--threshold-time` are wall-clock durations, not poll counts. Changing `--poll-interval` changes how many samples exist inside those durations. Disconnect/burst/activity-wake rules are the most sensitive to polling interval.

## Logs And State

Runtime files are written to `.\state\` by default:

```text
hdd-guard-state.json       persistent ban/reputation/activity state
banned-clients.txt         current non-permanent bans copied from state
perma-banned-clients.txt   current permanent bans copied from state
ban-audit.jsonl            detailed JSON record for each ban decision
near-miss-audit.jsonl      detailed JSON record for close/non-ban decisions
```

`ban-audit.jsonl` is the main file to inspect after a ban. It includes reason, score components, qB speed/upload data, ETW read data, torrent hashes, file peer count, external read processes, reputation snapshots, and whether a ban was promoted to permanent.

`near-miss-audit.jsonl` explains why a peer did not ban: safety blocks, unmet criteria, current score, thresholds, and activity-wake details. It is intentionally useful for tuning.

`hdd-guard-state.json` is persistent. Restarting the script does not reset reputation, activity-wake windows, ban counts, or permanent bans.

## ETW Helper

ETW means Event Tracing for Windows. With `--etw`, this project starts a small helper process that subscribes to Windows file-read events. The helper reports reads made by `qbittorrent.exe`, plus selected non-qB reads under active torrent roots, back to the Python script.

This matters because qBittorrent's peer list can show upload speed and bytes sent, but it does not directly say how much disk reading a peer caused. ETW adds the missing disk side of the picture:

```text
qB peer data: who received bytes, current speed, torrent, peer endpoint
ETW data: which torrent files qBittorrent read, how many bytes, when
combined: whether a peer is getting useful upload for the HDD reads it appears to cause
```

The guard maps ETW file paths back to active torrent files. If one peer is the only match, the read evidence is strong. If several peers match the same file, reads are weighted by qB uploaded-delta where possible; otherwise they are split equally and treated more cautiously. This is why ETW-backed bans can distinguish a merely slow peer from a peer that is slow while driving disproportionate random disk reads.

The repo includes a prebuilt helper bundle at the default lookup location:

```text
.\etw-helper\bin\Release\net8.0\QbtEtwHelper.exe
```

Compatibility notes:

```text
Windows only
tested/packaged from Windows 11 x64
requires .NET 8 runtime to run the prebuilt helper
includes TraceEvent native assets for amd64, x86, and arm64
requires Administrator rights for live ETW tracing
```

Install the .NET 8 runtime from Microsoft if `--etw` cannot start the bundled helper: <https://dotnet.microsoft.com/download/dotnet/8.0>

The prebuilt helper is not intended for Linux/macOS. It is not hard-coded to one local path. If the helper fails on another Windows install or CPU architecture, rebuild it from source with the .NET 8 SDK on that machine.

Using only `--etw` normally uses the existing bundled helper. No `--etw-helper` argument is required:

```powershell
python .\run_hdd_guard.py --host localhost --port 8080 --username admin --password "YOUR_PASSWORD" --etw
```

Run PowerShell or `cmd.exe` as Administrator for live ETW tracing. The helper is framework-dependent, so keep the whole `etw-helper\bin\Release\net8.0\` folder together; the `.dll`, `.deps.json`, `.runtimeconfig.json`, and architecture subfolders are part of the runnable bundle.

You can explicitly point at the bundled helper:

```powershell
python .\run_hdd_guard.py --etw --etw-helper ".\etw-helper\bin\Release\net8.0\QbtEtwHelper.exe"
```

## Build ETW Helper From Source

Optional. Requires the .NET 8 SDK, not just the runtime. Run PowerShell as Administrator for live ETW tracing.

Install the SDK from Microsoft if you want to compile locally: <https://dotnet.microsoft.com/download/dotnet/8.0>

```powershell
cd path\to\qbit-peer-hhd-churn-checker\etw-helper
dotnet publish -c Release
```

Build output is written to the same default helper location:

```text
.\etw-helper\bin\Release\net8.0\QbtEtwHelper.exe
.\etw-helper\bin\Release\net8.0\publish\QbtEtwHelper.exe
.\etw-helper\bin\Diag\QbtEtwHelperDiag.exe
```

The script resolves helper paths relative to the project folder, not the current PowerShell directory.

The diagnostic helper writes status to the console through Python every 15 seconds:

```text
ETW helper: status raw_reads=... qbt_reads=... qbt_named_reads=... qbt_pids=...
```

If `raw_reads` grows but `qbt_reads` stays `0`, ETW is active but qBittorrent is not being matched. If `qbt_reads` grows but `qbt_named_reads` stays `0`, reads are seen but path resolution is missing. If all remain `0`, the session is not receiving file-read events.

## Important Defaults

```text
poll interval: 5 seconds
low speed threshold: 16 KiB/s
threshold window: 180 seconds
min payload: 1 MiB
required confidence: medium
auto-unban: 7 days
permanent after: 2 counted bans across separate unban cycles
first-stage permanent expiry: 120 days
bare-IP bans: disabled
speed-only bans: enabled
```

## Unban And Permanent Ban Cycle

Normal bans are temporary. The script keeps its own ban count in `hdd-guard-state.json` and periodically clears normal bans so peers can be tested again.

Relevant args:

```text
--auto-unban-interval 604800
--permanent-after 2
--permanent-expire-after 10368000
```

How it works by default:

```text
1. Peer qualifies for a ban.
2. Ban count becomes 1 for the current unban cycle.
3. After 7 days, normal bans are auto-unbanned and the unban cycle advances.
4. If the same peer/IP qualifies again in a later cycle, ban count becomes 2.
5. With --permanent-after 2, that second counted ban promotes it to permanent.
6. Permanent bans are re-enforced and are kept across normal auto-unbans.
```

Repeated ban decisions in the same unban cycle do not increase the permanent-ban count. The peer/IP must be unbanned by a later cycle, then qualify again.

First-stage permanent expiry:

```text
--permanent-expire-after 10368000
```

Default is 120 days. A first-stage permanent ban can expire after that time. If the same peer/IP later reaches permanent status again, it becomes second-generation permanent and does not expire under this rule. Set `--permanent-expire-after 0` to disable first-stage permanent expiry.

State files are saved under the project/running-script folder by default:

```text
.\state\hdd-guard-state.json
.\state\banned-clients.txt
.\state\perma-banned-clients.txt
.\state\ban-audit.jsonl
.\state\near-miss-audit.jsonl
```

Use `--state-dir` to move them.

Relative `--state-dir` and `--etw-helper` values are also resolved from the project/running-script folder. Absolute paths still work normally.

`ban-audit.jsonl` gets one JSON record per ban decision. It includes the peer, IP, torrent hashes, ban reason, score, confidence, speed/upload stats, ETW stats, file peer count, external process read stats, score components, unban cycle, ban count, permanent-promotion status, and reputation snapshots.

`near-miss-audit.jsonl` gets throttled JSON records for peers that were close to a ban or had meaningful low-speed evidence but were blocked by a specific gate. Ordinary early peers, tiny-payload reputation-only cases, and high-throughput ETW+payload peers are ignored. It includes the same core metrics plus `safety_blocks` and `unmet_criteria` explaining why no ban happened. Default throttle is one matching near-miss per peer every 300 seconds, after a 60 second startup grace.

ETW attribution fields:

```text
file_peer_count          active peer sessions whose qB file list matched the ETW read path
etw_read_bytes_raw       raw qB read bytes observed for matching file paths
etw_read_bytes_attributed qB read bytes attributed to this peer after shared-file weighting
etw_attribution_methods  single, uploaded-delta, or equal
external_read_bytes      bytes read by non-qB processes under active torrent roots
external_read_count      matching non-qB read event count
external_process_count   distinct non-qB process count
external_processes       process names, for example MsMpEng.exe
```

When more than one active peer matches the same ETW file path, qB read bytes are weighted by each candidate peer's current qB uploaded-delta. If no candidate uploaded bytes in that poll, the read is split equally. Ban scoring uses attributed ETW bytes; raw ETW bytes remain in audits for debugging.

The ETW helper always emits qBittorrent reads. For non-qB processes it only emits reads under currently active torrent roots, which keeps noise lower while still exposing antivirus/indexer/player activity that may make ETW attribution less exact.

## Ban Logic

A normal slow HDD-churn ban needs:

```text
peer connected for --threshold-time
average speed below --low-speed-threshold
at least --min-payload uploaded
ETW read evidence matching qB peer.files/torrent files, unless using speed-only fallback
attributed ETW/read bytes divided by qB uploaded bytes >= --min-etw-upload-ratio
confidence >= --required-confidence when ETW is required
safety gates pass
```

Additional normal slow-HDD gates depend on average speed:

```text
avg < 4 KiB/s:
  normal ETW/upload ratio gate is enough

4-8 KiB/s:
  ETW/upload ratio >= 8x
  plus repeated reputation, connected bursts, or 2+ active torrents

avg >= 8 KiB/s:
  ETW/upload ratio >= 15x
  uploaded >= 4 MiB
  plus repeated bad sessions, connected burst history, 2+ active torrents, or lone-peer ETW
```

For `avg >= 8 KiB/s`, a shared-file, single-torrent, first bad session is near-miss only. This avoids banning plausible peers that happen to request scattered pieces from a large file.

A single-torrent extreme-low-speed ban catches one peer that stays connected for a longer window but remains extremely slow:

```text
average speed below --extreme-low-speed-threshold
connected for --extreme-low-speed-time
uploaded at least --extreme-low-speed-min-payload
speed-only bans enabled
not productive elsewhere
```

Defaults:

```text
--extreme-low-speed-threshold 4KiB
--extreme-low-speed-time 900
--extreme-low-speed-min-payload 1MiB
```

A long-term low-speed HDD ban catches peers that build strong historical evidence but may have a small current-window payload:

```text
current average speed below --low-speed-threshold
historical low-speed time >= --long-term-low-speed-time
historical ETW matched sessions >= --long-term-low-speed-min-etw-sessions
historical ETW read bytes >= --long-term-low-speed-min-etw-bytes
historical uploaded bytes >= --long-term-low-speed-min-uploaded
recent medium/high ETW evidence exists
not currently productive
```

Safety gates block bans when:

```text
payload is tiny/zero
peer is productive elsewhere without HDD churn
ETW/file mapping is missing while ETW is required
evidence is only a one-off short session
ETW/upload ratio is too low for an HDD-churn ban
```

The ETW/upload ratio only gates low-speed HDD-churn bans. It does not block the separate speed-only fallback or extreme-low-speed rule. Default `--min-etw-upload-ratio 5.0` means qB must read at least 5 bytes from disk for every 1 byte uploaded to that peer before the normal HDD-churn rule can ban it. Peers averaging `8 KiB/s` or more require a higher `15x` ratio plus extra evidence.

Burst reconnect bans target peers that repeatedly connect, receive payload, trigger matched reads, then disconnect:

```text
--short-session-max 10
--burst-min-payload 128KiB
--burst-count 5
--burst-window 3600
--burst-min-total-payload 2MiB
```

Connected burst bans target peers that remain connected but only periodically pull payload while causing matched reads:

```text
--connected-burst-min-payload 128KiB
--connected-burst-count 5
--connected-burst-window 3600
--connected-burst-min-total-payload 2MiB
--connected-burst-max-duty-ratio 0.25
--connected-burst-max-average-speed unset, uses --low-speed-threshold
```

Duty ratio is burst polls divided by observed connected polls in the window. A low duty ratio catches sporadic burst churn. The connected-burst average-speed gate defaults to `--low-speed-threshold`, so peers averaging 16 KiB/s or more are skipped with default args.

Activity-wake churn bans target peers that repeatedly wake a torrent active for tiny bursts, while never transferring enough data to be useful:

```text
--activity-wake-churn-after 10
--ip-activity-wake-churn-after 35
--ip-activity-wake-churn-window 7200
--ip-activity-wake-churn-max-session-time 90
--ip-activity-wake-churn-max-single-payload 64KiB
--ip-activity-wake-churn-max-total-payload 256KiB
--ip-activity-wake-churn-min-speed 10KiB
--ip-activity-wake-churn-distinct-ports 3
--ip-activity-wake-churn-all-distinct-ports-over 15
```

The same `IP:port` is banned with reason `activity-wake-churn` after 10 weighted activity-wake points by default. A bare IP is banned with reason `ip-activity-wake-churn` after 35 weighted points across 3+ distinct ports from the same IP. If every counted event for the IP uses a different port, the bare IP can also be banned once the distinct-port count is greater than 15. Set `--activity-wake-churn-after 0` to disable endpoint activity-wake bans. Set `--ip-activity-wake-churn-after 0` to disable bare-IP activity-wake bans.

A counted activity-wake event can be either:

```text
tiny payload burst with max speed >= --ip-activity-wake-churn-min-speed
peer appears on a torrent that just moved into qB's active set and also has peer-local evidence
```

Peer-local evidence means at least one of: qB reported uploaded bytes, qB reported peer speed >= `--ip-activity-wake-churn-min-speed`, or ETW reads were attributed by `single`/`uploaded-delta` matching. Equal-split ETW from a shared file is not enough for activity-wake evidence. This avoids counting harmless peers that were merely connected while another peer woke the torrent.

Activity-wake uses weighted points instead of raw event count:

```text
1  speed-backed torrent wake
2  qB uploaded payload
3  peer-local ETW correlation
4  qB uploaded payload + peer-local ETW
5  qB uploaded payload + lone-peer ETW
+1 prior same endpoint activity inside the window
+1 prior same IP activity on another endpoint inside the window
+1 same IP rotating ports
+1 torrent active-transition
-1 shared/equal ETW attribution
-2 no qB payload or speed evidence
```

`qB uploaded payload + ETW` is weighted highest because it ties network transfer and disk reads to the same torrent window. Lone-peer ETW gets the strongest score. Equal-split ETW from shared files is deliberately discounted.

Activity-wake event history is persisted in `hdd-guard-state.json`, so endpoint/IP activity-wake windows survive script restarts.

If an IP has zero-upload ETW activity-wake evidence but has not reached the bare-IP ban threshold, a near-miss record is written with reason `ip-activity-wake-churn-near-miss`. The record includes weighted event count, threshold, distinct ports, endpoints, torrents, and ETW read bytes.

Activity-wake audit details include `activity_wake_confidence`: `weak` for speed-backed active-transition only, `medium` when qB reports payload without ETW, and `high` when peer-local ETW reads are present.

## Useful Examples

Poll every 5s, treat under 2 KiB/s as low, ban only with ETW evidence:

```powershell
python .\run_hdd_guard.py --password "YOUR_PASSWORD" --etw --poll-interval 5 --low-speed-threshold 2KiB --threshold-time 180
```

Only show actual bans:

```powershell
python .\run_hdd_guard.py --password "YOUR_PASSWORD" --etw --ban-log-only
```

Enable bare-IP escalation after 5 distinct bad `IP:port` endpoints:

```powershell
python .\run_hdd_guard.py --password "YOUR_PASSWORD" --etw --bare-ip-bad-endpoint-count 5
```

## Key Args

| Arg | Default | Meaning |
| --- | ---: | --- |
| `--poll-interval` | `5` | Seconds between qB active-torrent polls. |
| `--low-speed-threshold` | `16KiB` | Low upload speed threshold. Supports `KiB`, `MiB`, `KB`, `MB`. |
| `--threshold-time` | `180` | Connected time before slow HDD-churn rule can fire. |
| `--low-speed-ratio` | `0.8` | Fraction of samples below threshold required. |
| `--extreme-low-speed-threshold` | `4KiB` | Single-torrent extreme low-speed threshold. |
| `--extreme-low-speed-time` | `900` | Connected seconds before extreme low-speed rule can fire. |
| `--extreme-low-speed-min-payload` | `1MiB` | Payload required for extreme low-speed ban. |
| `--min-payload` | `1MiB` | Minimum uploaded bytes before any normal ban. |
| `--min-etw-upload-ratio` | `5.0` | Minimum attributed ETW read bytes divided by uploaded bytes for low-speed HDD-churn bans. |
| `--required-confidence` | `medium` | Required ETW confidence: `none`, `low`, `medium`, `high`. |
| `--etw` | off | Start/read ETW helper. |
| `--etw-helper` | built helper path | Override helper exe path. |
| `--speed-only-bans` / `--no-speed-only-bans` | on | Permit or disable conservative non-ETW fallback bans. |
| `--require-etw` | off | Require ETW confidence before banning. |
| `--near-miss-log-interval` | `300` | Seconds before repeating the same near-miss peer/reason in `near-miss-audit.jsonl`. |
| `--near-miss-startup-grace` | `60` | Seconds after script start before near-miss audit records are written. Bans still evaluate normally. |
| `--auto-unban-interval` | `604800` | Seconds between normal ban clears. `0` disables. |
| `--permanent-after` | `2` | Promote after counted bans in separate unban cycles. `0` disables. |
| `--permanent-expire-after` | `10368000` | First-stage permanent expiry seconds. `0` disables. |
| `--bare-ip-bad-endpoint-count` | `0` | Bare-IP ban after this many distinct bad endpoints. `0` disables. |
| `--short-session-max` | `10` | Max seconds for burst reconnect session. |
| `--burst-min-payload` | `128KiB` | Payload required for one burst session. |
| `--burst-count` | `5` | Burst sessions required in window. |
| `--burst-window` | `3600` | Burst tracking window seconds. |
| `--burst-min-total-payload` | `2MiB` | Total burst payload required before ban. |
| `--connected-burst-min-payload` | `128KiB` | Payload required for one connected burst poll. |
| `--connected-burst-count` | `5` | Connected burst polls required in window. |
| `--connected-burst-window` | `3600` | Connected burst tracking window seconds. |
| `--connected-burst-min-total-payload` | `2MiB` | Total connected burst payload required before ban. |
| `--connected-burst-max-duty-ratio` | `0.25` | Maximum fraction of connected polls allowed to be burst polls. |
| `--connected-burst-max-average-speed` | low-speed threshold | Maximum average speed for connected-burst bans. Omit to use `--low-speed-threshold`. |
| `--activity-wake-churn-after` | `10` | Weighted activity-wake points required before exact `IP:port` activity-wake ban. `0` disables endpoint layer. |
| `--ip-activity-wake-churn-after` | `35` | Weighted activity-wake points required before bare-IP activity-wake ban. `0` disables IP layer. |
| `--ip-activity-wake-churn-window` | `7200` | Window seconds for IP activity-wake churn. |
| `--ip-activity-wake-churn-max-session-time` | `90` | Max seconds for one counted IP activity-wake session. |
| `--ip-activity-wake-churn-max-single-payload` | `64KiB` | Max uploaded bytes for one counted IP activity-wake session. |
| `--ip-activity-wake-churn-max-total-payload` | `256KiB` | Max total uploaded bytes across the IP activity-wake window. |
| `--ip-activity-wake-churn-min-speed` | `10KiB` | Minimum burst speed for one counted payload activity-wake session. Torrent active-transition events can count even if upload delta is missed. |
| `--ip-activity-wake-churn-distinct-ports` | `3` | Distinct ports required before IP activity-wake bare-IP ban. |
| `--ip-activity-wake-churn-all-distinct-ports-over` | `15` | Bare-IP activity-wake shortcut when every counted event uses a different port and the distinct-port count is greater than this value. `0` disables. |
| `--long-term-low-speed-time` | `900` | Historical low-speed seconds required for long-term low-speed bans. |
| `--long-term-low-speed-min-etw-sessions` | `100` | Historical matched ETW sessions required for long-term low-speed bans. |
| `--long-term-low-speed-min-etw-bytes` | `256MiB` | Historical matched read bytes required for long-term low-speed bans. |
| `--long-term-low-speed-min-uploaded` | `1MiB` | Historical uploaded bytes required for long-term low-speed bans. |
| `--state-dir` | `.\state` | State/audit output dir. |
| `--dry-run` | off | Log decisions without banning. |
| `--ban-log-only` | off | Console logs only ban-level events. |

## Disclaimer

This tool is meant to reduce avoidable HDD churn, not punish every slow peer. The auto-unban cycle exists for that reason: normal bans are periodically cleared so peers can get another chance later. Permanent bans only happen after repeated counted bans across separate unban cycles, and first-stage permanent bans can expire after `--permanent-expire-after`.

Peer banning can affect swarm health and may conflict with expectations on some private trackers or communities. Tracker rules and norms differ. Use caution, start with `--dry-run`, inspect `ban-audit.jsonl` and `near-miss-audit.jsonl`, and tune thresholds before allowing real bans.

More conservative examples:

```powershell
# observe only, no actual qB bans
python .\run_hdd_guard.py --host localhost --port 8080 --username admin --password "YOUR_PASSWORD" --etw --dry-run

# disable broad plain-IP escalation paths
python .\run_hdd_guard.py --host localhost --port 8080 --username admin --password "YOUR_PASSWORD" --etw --bare-ip-bad-endpoint-count 0 --ip-activity-wake-churn-after 0

# keep endpoint activity-wake bans, but make bare-IP activity-wake much harder
python .\run_hdd_guard.py --host localhost --port 8080 --username admin --password "YOUR_PASSWORD" --etw --ip-activity-wake-churn-after 60 --ip-activity-wake-churn-all-distinct-ports-over 30

# require stronger slow-HDD evidence
python .\run_hdd_guard.py --host localhost --port 8080 --username admin --password "YOUR_PASSWORD" --etw --min-etw-upload-ratio 10 --no-speed-only-bans
```

The most cautious production shape is usually `--etw --no-speed-only-bans` with higher bare-IP thresholds. Bare-IP bans are broader than `IP:port` bans and should be treated as the highest-risk setting, especially around VPNs, NAT, seedboxes, and shared networks.
