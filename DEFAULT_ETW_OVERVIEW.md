# qB HDD Guard Overview: Defaults With `--etw`

This tool watches active completed qBittorrent torrents and bans peers that appear to create excessive disk reads for poor upload value.

Default run shape:

```powershell
python .\run_hdd_guard.py --host localhost --port 8080 --username "YOUR_USERNAME" --password "YOUR_PASSWORD" --etw
```

## What It Polls

Every 5 seconds, the script asks qBittorrent for active completed torrents, then asks for peers on those active torrents.

It does not scan every torrent in qBittorrent. It only evaluates peers currently visible on active completed torrents.

## What ETW Adds

With `--etw`, the script starts the ETW helper and watches Windows file-read events from `qbittorrent.exe`.

The script maps ETW reads back to active torrent files using qBittorrent torrent/file data. If a file is shared by multiple peers, ETW read bytes are attributed by qB uploaded-delta where possible. If that is not possible, reads are split equally.

The key HDD signal is:

```text
attributed ETW read bytes / qB uploaded bytes
```

Default minimum for normal low-speed HDD-churn bans:

```text
--min-etw-upload-ratio 5.0
```

So a low-speed peer normally needs to cause at least 5 bytes read from disk for every 1 byte uploaded to it.

## Main Default Ban Rules

### Normal Slow HDD-Churn Ban

Default requirements:

```text
connected for at least 180 seconds
average upload speed below 16 KiB/s
at least 80% of samples below 16 KiB/s
uploaded at least 1 MiB
ETW/upload ratio >= 5.0x
ETW confidence >= medium when needed
not protected by safety gates
```

Average-speed tiers add stricter gates:

```text
avg < 4 KiB/s: normal ETW/upload ratio gate is enough
avg 4-8 KiB/s: ETW/upload ratio >= 8x plus repeated/multi-torrent evidence
avg >= 8 KiB/s: ETW/upload ratio >= 15x, uploaded >= 4 MiB, plus stronger extra evidence
```

For `avg >= 8 KiB/s`, shared-file, single-torrent, first bad sessions are near-miss only. This protects peers that are slow but plausibly just receiving scattered pieces.

This is the main HDD-protection rule.

### Extreme Low-Speed Ban

Default requirements:

```text
single active torrent
connected for at least 900 seconds
average upload speed below 4 KiB/s
uploaded at least 1 MiB
not productive elsewhere
```

This can work as a speed-only fallback for peers that stay connected but barely upload.

### Long-Term Low-Speed Ban

This catches peers that build bad history over time.

Default requirements:

```text
current low-speed window exists
historical low-speed time >= 900 seconds
historical ETW matched sessions >= 100
historical ETW read bytes >= 256 MiB
historical uploaded bytes >= 1 MiB
recent medium/high ETW evidence exists
not currently productive
```

### Burst Reconnect Ban

This catches peers that repeatedly connect, receive a little data, trigger ETW reads, then disconnect.

Defaults:

```text
short session <= 10 seconds
burst payload >= 128 KiB
5 burst sessions within 3600 seconds
total burst payload >= 2 MiB
ETW confidence >= medium
```

### Connected Burst Ban

This catches peers that stay connected but only periodically pull data while causing matched reads.

Defaults:

```text
connected burst payload >= 128 KiB
5 burst polls within 3600 seconds
total connected burst payload >= 2 MiB
duty ratio <= 0.25
average speed below 16 KiB/s
ETW confidence >= medium
```

Duty ratio means burst polls divided by observed connected polls in the window.

### Activity-Wake Churn Ban

This catches a peer repeatedly waking a torrent active for tiny bursts, while transferring very little total data.

Defaults:

```text
same IP:port: 10 weighted points within 7200 seconds
same bare IP: 35 weighted points across 3+ ports within 7200 seconds
all-distinct bare IP shortcut: >15 counted distinct ports
session duration <= 90 seconds
single-session payload <= 64 KiB
total window payload <= 256 KiB
burst speed >= 10 KiB/s
distinct ports >= 3 for bare-IP escalation
```

The same `IP:port` is banned as `activity-wake-churn` after 10 weighted activity-wake points. If the same IP reaches 35 weighted points across 3+ distinct ports, the bare IP is banned as `ip-activity-wake-churn`. A rotating-port shortcut also bans the bare IP when every counted event uses a different port and the distinct-port count is greater than 15.

Activity-wake can be counted from either a tiny payload burst or a peer appearing when a torrent just moved into qB's active set with peer-local evidence. Peer-local evidence means qB reported uploaded bytes, qB reported peer speed above the wake floor, or ETW reads were attributed by `single`/`uploaded-delta` matching. Equal-split ETW from a shared file is not enough.

Activity-wake uses weighted points:

```text
1  speed-backed torrent wake
2  qB uploaded payload
3  peer-local ETW correlation
4  qB uploaded payload + peer-local ETW
5  qB uploaded payload + lone-peer ETW
+1 prior same endpoint activity
+1 prior same IP activity on another endpoint
+1 same IP rotating ports
+1 torrent active-transition
-1 shared/equal ETW attribution
-2 no qB payload or speed evidence
```

Activity-wake history is stored in `hdd-guard-state.json`, so restarting the script does not reset the rolling activity-wake window.

If an IP is close to the activity-wake bare-IP threshold and has `uploaded=0` with matched ETW reads, `near-miss-audit.jsonl` gets an `ip-activity-wake-churn-near-miss` record for debugging/tuning.

Activity-wake audit details also include `activity_wake_confidence`: `weak`, `medium`, or `high`; peer-local ETW makes it `high`.

## Productive Peer Safety

The script tries not to ban a peer if it has a valid productive connection.

Examples:

```text
one torrent slow but another torrent productive -> safer
high speed peer with high ETW reads -> not banned by low-speed HDD rule
low-speed peer with weak ETW/upload ratio -> blocked from normal HDD-churn ban
```

The intended order is:

```text
low speed first, HDD waste second
```

## Ban Count And Permanent Bans

Default normal bans are counted by unban cycle.

Defaults:

```text
--auto-unban-interval 604800
--permanent-after 2
--permanent-expire-after 10368000
```

Normal bans are counted by unban cycle. A peer/IP banned once is normally unbanned during the weekly auto-unban. The unban cycle advances at that point. If the same peer/IP qualifies again in a later cycle, it can become permanently banned.

Repeated decisions in the same unban cycle do not increment the permanent-ban count. The peer/IP must survive a normal unban and then qualify again.

Permanent bans are kept across auto-unban and re-enforced. First-stage permanent bans can expire after 120 days by default. If the same peer/IP later reaches permanent status again, it becomes second-generation permanent and stays permanent. Set `--permanent-expire-after 0` to disable first-stage permanent expiry.

## Files Written

Default state folder is next to the script:

```text
.\state
```

Important files:

```text
banned-clients.txt          copy of normal/current banned clients
perma-banned-clients.txt    copy of permanent banned clients
hdd-guard-state.json        main state, ban counts, reputation, timers
ban-audit.jsonl             detailed records for actual bans
near-miss-audit.jsonl       detailed records for close-but-not-banned peers
```

## Console Logs

Default console logs include startup, ETW helper status, poll summaries, near-miss write summaries, and actual bans.

Actual ban lines include:

```text
reason
score
confidence
average speed
uploaded bytes
attributed ETW bytes
raw ETW bytes
ETW/upload ratio
file peer count
attribution method
external read info
score components
```

Use this to show only actual ban-level events:

```powershell
python .\run_hdd_guard.py --host localhost --port 8080 --username "YOUR_USERNAME" --password "YOUR_PASSWORD" --etw --ban-log-only
```

## Practical Meaning

With defaults and `--etw`, the script is conservative. It is not just banning slow peers. It is mainly banning peers that are both slow and associated with disproportionate qBittorrent disk reads.

The most useful signal for HDD protection is a low-speed peer with a high ETW/upload ratio, especially `10x+`.

For repeated tiny bursts from one endpoint, look for `activity-wake-churn`. For the same IP rotating ports, look for `ip-activity-wake-churn`.
