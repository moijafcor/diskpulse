# diskpulse

Inventory-driven SMART health monitoring and failure prediction for distributed drive fleets.

diskpulse monitors drives across any number of machines and datacentres via SSH, tracks SMART attribute trends over time, and alerts **only** when a drive is predicted to fail within a configurable horizon. No hardcoded topology -- all infrastructure is defined in a single YAML inventory file.

## Features

- **Inventory-driven** -- add a datacentre, host, or drive by editing `inventory.yaml`
- **Failure prediction** -- trend-based analysis with configurable horizon (default: 30 days)
- **NVMe + SATA** -- auto-detects drive type and extracts the right attributes
- **Read-only filesystem detection** -- catches kernel `ro` remounts after I/O errors, even when SMART still reports PASSED
- **RAID monitoring** -- parses `/proc/mdstat` for degraded arrays
- **Alert deduplication** -- won't spam you with the same alert every run
- **Pluggable alert channels** -- log file, webhook (Slack/Discord/ntfy), desktop notification, custom script
- **Parallel collection** -- queries multiple hosts concurrently via `ThreadPoolExecutor`
- **Zero external dependencies** -- only `pyyaml` (for inventory parsing); everything else is stdlib

## Requirements

- Python 3.10+
- `pyyaml` (`pip install pyyaml`)
- `smartmontools` installed on every monitored host
- SSH key-based auth to all remote targets (passwordless)
- `sudo smartctl` must work without a password prompt on monitored hosts

## Quick Start

```bash
# Install dependency
pip install pyyaml

# Discover drives on a host (generates inventory YAML)
python3 diskpulse.py --scan localhost
python3 diskpulse.py --scan ubuntu@192.168.1.10

# Create your inventory from the scan output
vim inventory.yaml

# Dry run -- see what it would do without firing alerts
python3 diskpulse.py -c inventory.yaml --dry-run -v

# Status dashboard
python3 diskpulse.py -c inventory.yaml --status

# Normal run (for cron)
python3 diskpulse.py -c inventory.yaml
```

## Inventory File

The inventory defines your entire fleet in a `datacentres > hosts > devices` hierarchy. Only `devices[].path` is required -- everything else has sensible defaults.

```yaml
defaults:
  ssh_user: ubuntu
  ssh_port: 22
  ssh_timeout: 15
  failure_horizon_days: 30    # alert if failure predicted within N days
  alert_dedup_days: 7         # don't re-alert same condition within N days
  history_retention_days: 180  # prune history older than this
  thresholds:
    nvme_temp_warn: 70
    sata_ssd_temp_warn: 60
    sata_hdd_temp_warn: 50
    temp_consecutive_readings: 3  # sustained high temp requires N readings

datacentres:
  home-lab:
    description: "Home LAN"
    hosts:
      server01:
        host: 192.168.1.10
        devices:
          - path: /dev/sda
            mount: /
          - path: /dev/sdb
            mount: /bulk
          - path: /dev/nvme0
            mount: /data
          - path: /dev/sdc
            mount: /mnt/usb-backup
            removable: true          # won't alert when disconnected

      workstation:
        host: localhost              # local machine, no SSH
        devices:
          - path: /dev/nvme0
            mount: /

  dc-east-1:
    description: "Remote datacentre (RAID1)"
    hosts:
      web-prod:
        host: 203.0.113.50
        check_raid: true             # also monitor /proc/mdstat
        devices:
          - path: /dev/nvme0
          - path: /dev/nvme1
```

### Inventory Reference

**Defaults** (can be overridden at datacentre, host, or device level):

| Key | Default | Description |
|-----|---------|-------------|
| `ssh_user` | `ubuntu` | SSH username for remote hosts |
| `ssh_port` | `22` | SSH port |
| `ssh_timeout` | `15` | SSH connection timeout (seconds) |
| `failure_horizon_days` | `30` | Alert if failure predicted within N days |
| `alert_dedup_days` | `7` | Suppress duplicate alerts within N days |
| `history_retention_days` | `180` | Prune history records older than this |
| `log_file` | `state/diskpulse.log` | Path to log file |
| `webhook_url` | *(none)* | HTTP POST endpoint for alerts |
| `notify_send` | `false` | Desktop notification via `notify-send` |
| `alert_script` | *(none)* | Custom script, receives alert JSON on stdin |

**Per-device options:**

| Key | Default | Description |
|-----|---------|-------------|
| `path` | *(required)* | Device path, e.g. `/dev/sda`, `/dev/nvme0n1` |
| `mount` | *(none)* | Mount point -- enables read-only filesystem detection |
| `removable` | `false` | Suppress alerts when device is absent |
| `readonly` | `false` | Mark mount as intentionally read-only (suppresses RO alert) |

**Per-host options:**

| Key | Default | Description |
|-----|---------|-------------|
| `host` | *(required)* | IP/hostname, or `localhost` for local execution |
| `check_raid` | `false` | Parse `/proc/mdstat` for degraded arrays |
| `enabled` | `true` | Set `false` to skip without removing from inventory |
| `tags` | `[]` | List of strings for filtering with `--tags` |

## CLI Reference

```
usage: diskpulse.py [-h] [-c INVENTORY] [--state-dir STATE_DIR] [--dry-run]
                    [--status] [--dc DC] [--host HOST] [--tags TAGS]
                    [--force] [-v] [--max-workers N]
                    [--scan USER@HOST_OR_LOCALHOST]
```

| Flag | Description |
|------|-------------|
| `-c`, `--config` | Path to `inventory.yaml` (required for all modes except `--scan`) |
| `--status` | Print human-readable health dashboard and exit |
| `--dry-run` | Collect and analyze, but don't fire alerts |
| `--scan TARGET` | Discover drives on `localhost` or `user@host` and print inventory YAML |
| `--dc NAME` | Filter by datacentre name |
| `--host NAME` | Filter by hostname |
| `--tags TAG,TAG` | Only run hosts matching any of these tags |
| `--force` | Ignore alert deduplication (re-alert everything) |
| `-v` | Verbose output (repeat for debug: `-vv`) |
| `--max-workers N` | Parallel host queries (default: 5) |
| `--state-dir DIR` | State directory (default: `state/`) |

### Examples

```bash
# Daily cron job
0 6 * * * /usr/bin/python3 /opt/diskpulse/diskpulse.py -c /opt/diskpulse/inventory.yaml

# Check just the home lab
python3 diskpulse.py -c inventory.yaml --status --dc home-lab

# Check a single host
python3 diskpulse.py -c inventory.yaml --status --host server01

# Force re-alert on everything (useful after fixing an issue)
python3 diskpulse.py -c inventory.yaml --force

# Scan a new host and paste the output into your inventory
python3 diskpulse.py --scan ubuntu@192.168.1.42
```

### Status Dashboard

```
SMART Drive Health -- 2026-03-14 06:00:00
===============================================================================

home-lab
-------------------------------------------------------------------------------
  server01 (192.168.1.10)
    /dev/sda   Samsung 870 EVO 500G    9,938h  44C  realloc:0 pending:0       rw  PASSED
    /dev/sdb   Seagate Barracuda 8T      90h  29C  realloc:0 pending:0       rw  PASSED
    /dev/nvme0 WD Black SN770 500G       94h  35C  spare:100% used:0%        rw  PASSED

  workstation (localhost)
    /dev/nvme0 Samsung 990 Pro 2TB       10h  46C  spare:100% used:0%        rw  PASSED

dc-east-1
-------------------------------------------------------------------------------
  web-prod (203.0.113.50)                                       RAID: OK [UU]
    /dev/nvme0 Samsung PM9A3 1.9T   33,305h  36C  spare:100% used:9%        rw  PASSED
    /dev/nvme1 Samsung PM9A3 1.9T   10,320h  39C  spare:100% used:0%        rw  PASSED

6 drives across 3 hosts -- all healthy
```

### Exit Codes

| Code | Meaning |
|------|---------|
| `0` | All drives healthy, no alerts fired |
| `1` | One or more CRITICAL alerts fired |
| `2` | Collection errors (SSH/smartctl failures) but no drive alerts |
| `3` | Configuration or inventory error |

## Failure Prediction

diskpulse doesn't just report current SMART status -- it tracks attribute trends over time and predicts when a drive will fail. Alerts fire only when failure is predicted within the configurable horizon.

### Critical Findings (trigger alerts)

| Condition | Drive Type | What It Means |
|-----------|-----------|---------------|
| `critical_warning != 0` | NVMe | Drive self-reporting a critical issue |
| `media_errors` increasing | NVMe | Active data integrity failures |
| `available_spare <= threshold` | NVMe | Spare blocks exhausted |
| SMART overall health FAILED | SATA | Drive self-reporting failure |
| `current_pending_sector` increasing | SATA | Active sector reallocation failures |
| `reallocated_sector_ct` growing | SATA | Active degradation; 14x higher failure risk (Google, 2007) |
| NVMe endurance projected to 100% within horizon | NVMe | Wear-out imminent based on `percentage_used` growth rate |
| NVMe spare projected below threshold within horizon | NVMe | Spare depletion based on `available_spare` decline rate |
| Filesystem mounted read-only (unexpectedly) | Any | Kernel remounted `ro` after I/O errors -- operationally failed |
| RAID array degraded | Any | `/proc/mdstat` shows `[U_]` or `[_U]` |

### Warning Findings (logged, no alert)

| Condition | What It Means |
|-----------|---------------|
| Sustained high temperature | 3+ consecutive readings above threshold |
| `udma_crc_error_count` growing | SATA cable or controller problem |
| `spin_retry_count > 0` | HDD mechanical stress |
| `command_timeout` growing | Possible controller issue |

### What diskpulse ignores

- **Drive age alone** -- old drives are not inherently failing
- **Single temperature spikes** -- only sustained heat matters
- **Disconnected removable drives** -- by design
- **Unreachable hosts** -- logged as WARNING, does not trigger alerts
- **Healthy endurance trajectories** -- NVMe at 9% used with slow growth is fine

## Alert Channels

Alerts are sent to all configured channels simultaneously.

### Log File (always on)

Every CRITICAL finding is appended to the log file with full SMART dump.

```yaml
defaults:
  log_file: /var/log/diskpulse.log  # default: state/diskpulse.log
```

### Webhook

HTTP POST with JSON payload -- compatible with Slack incoming webhooks, Discord, ntfy.sh, Healthchecks.io, or any endpoint that accepts JSON.

```yaml
defaults:
  webhook_url: "https://hooks.slack.com/services/T.../B.../..."
```

Payload fields: `datacentre`, `host`, `host_ip`, `device`, `model`, `serial`, `mount`, `condition`, `evidence`, `rate`, `projected`, `action`, `severity`, `full_smart`.

### Desktop Notification

Uses `notify-send` (Linux desktop environments with libnotify).

```yaml
defaults:
  notify_send: true
```

### Custom Script

Executes a user-provided script with the full alert as JSON on stdin.

```yaml
defaults:
  alert_script: /opt/diskpulse/handlers/send-email.sh
```

The script receives a JSON object with `reading` and `finding` keys. Example handler:

```bash
#!/bin/bash
# /opt/diskpulse/handlers/send-email.sh
jq -r '.finding.condition' | mail -s "SMART Alert" admin@example.com
```

### Alert Deduplication

diskpulse tracks fired alerts in `state/alerts.json` keyed by `host:device:condition_type`. The same condition won't re-alert within `alert_dedup_days` (default: 7) unless:

- The dedup window has expired
- The severity or evidence has changed (e.g., reallocated sectors grew further)

Use `--force` to bypass deduplication.

## Architecture

```
diskpulse.py          # Single-file script (all logic)
inventory.yaml        # Fleet topology (user-maintained)
state/                # Auto-created state directory
  cache/              # Drive metadata cache (model, serial, type)
    {host}_{device}.json
  history/            # Per-drive SMART readings over time
    {host}_{device}.jsonl
  alerts.json         # Alert deduplication tracker
  diskpulse.log       # Default log file
```

### Data Flow

```
inventory.yaml
      |
      v
  Build host configs (resolve defaults hierarchy)
      |
      v
  Collect readings (parallel per host)
      |-- SSH: sudo smartctl -j -A -H /dev/XXX
      |-- SSH: cat /proc/mounts (for RO detection)
      |-- SSH: cat /proc/mdstat (if check_raid)
      |
      v
  Extract attributes (auto-detect NVMe vs SATA)
      |
      v
  Append to history (JSONL, one file per drive)
      |
      v
  Failure prediction engine
      |-- Compare current vs historical readings
      |-- Project trends (endurance, spare blocks, sector reallocation)
      |-- Generate findings (CRITICAL / WARNING)
      |
      v
  Alert pipeline
      |-- Deduplication check (alerts.json)
      |-- Fire to configured channels (log, webhook, notify-send, script)
```

### Why JSONL History Files

Each drive gets its own append-only `.jsonl` file rather than a single database:

- **Scales** to hundreds of drives without loading everything into memory
- **Append-only** -- no need to parse and rewrite entire files
- **Easy to prune** -- read lines, filter by date, rewrite
- **Easy to inspect** -- `tail -5 state/history/server01_dev-sda.jsonl`
- **No external dependencies** -- no SQLite, no databases

### Configuration Resolution

Settings cascade with most-specific-wins precedence:

```
device > host > datacentre > defaults
```

For example, a host can override `ssh_user` from defaults, and a specific device could override `failure_horizon_days` from its host.

### smartctl Exit Code Handling

smartctl uses a bitmask exit code where bits 0-1 indicate command-level errors (bad arguments, device open failed) and bits 2-7 are SMART status indicators. diskpulse correctly parses JSON output regardless of bits 2-7, only treating bits 0-1 as collection failures. This is critical -- many drives return non-zero exit codes to report SMART status while still producing valid JSON output.

### Read-Only Filesystem Detection

A drive can be "SMART healthy" yet operationally failed. After I/O errors, the Linux kernel remounts the filesystem read-only to prevent data corruption. The drive's internal controller may not have flagged a failure yet, so SMART still reports PASSED.

diskpulse catches this by:
1. Reading `/proc/mounts` once per host
2. Matching each device's configured `mount` against the mount table
3. If the options contain `ro` and the device is not marked `readonly: true` in the inventory, firing a CRITICAL alert

Drives with intentionally read-only mounts (e.g., `/boot/efi`) can be marked with `readonly: true` to suppress false positives.

## Deployment

### Standalone

```bash
git clone https://github.com/moijafcor/diskpulse.git
cd diskpulse
pip install pyyaml

# Scan your hosts and build inventory
python3 diskpulse.py --scan localhost > inventory.yaml
# Edit inventory.yaml to match your infrastructure

# Test
python3 diskpulse.py -c inventory.yaml --status
python3 diskpulse.py -c inventory.yaml --dry-run -v

# Cron (daily at 6am)
crontab -e
# 0 6 * * * /usr/bin/python3 /opt/diskpulse/diskpulse.py -c /opt/diskpulse/inventory.yaml
```

### SSH Setup for Remote Hosts

diskpulse requires passwordless SSH to remote hosts and passwordless sudo for `smartctl`:

```bash
# On the monitoring host: copy SSH key to each remote host
ssh-copy-id ubuntu@192.168.1.10

# On each remote host: allow passwordless smartctl
echo 'ubuntu ALL=(ALL) NOPASSWD: /usr/sbin/smartctl' | sudo tee /etc/sudoers.d/smartctl
```

### Directory Permissions

The state directory needs to be writable by the user running diskpulse:

```bash
mkdir -p state
# If running from cron as a different user:
chown -R diskpulse:diskpulse state/
```

## Monitored SMART Attributes

### NVMe

| Attribute | Source Field | Significance |
|-----------|-------------|-------------|
| `percentage_used` | `nvme_smart_health_information_log` | Drive endurance consumed (0-100%+) |
| `available_spare` | `nvme_smart_health_information_log` | Spare blocks remaining (%) |
| `available_spare_threshold` | `nvme_smart_health_information_log` | Manufacturer's minimum spare (%) |
| `media_errors` | `media_and_data_integrity_errors` | Uncorrectable media errors |
| `critical_warning` | Top-level JSON | Bitmask of critical conditions |
| `temperature` | `nvme_smart_health_information_log` | Current temperature (Celsius) |
| `power_on_hours` | `nvme_smart_health_information_log` | Total operating hours |
| `data_units_written` | `nvme_smart_health_information_log` | Total write volume (512B units) |

### SATA (by attribute ID)

| ID | Name | Significance |
|----|------|-------------|
| 5 | `reallocated_sector_ct` | Bad sectors remapped; growth = active degradation |
| 9 | `power_on_hours` | Total operating hours |
| 10 | `spin_retry_count` | HDD spin-up retries; >0 = mechanical stress |
| 187 | `reported_uncorrect` | Uncorrectable ECC errors |
| 188 | `command_timeout` | Commands that timed out |
| 190 | `airflow_temperature_cel` | Temperature (alternate source) |
| 194 | `temperature_celsius` | Temperature (primary source) |
| 197 | `current_pending_sector` | Sectors waiting for reallocation |
| 198 | `offline_uncorrectable` | Sectors that failed offline scan |
| 199 | `udma_crc_error_count` | SATA interface CRC errors |
| 200 | `multi_zone_error_rate` | Multi-zone error rate |

## Research Basis

The failure prediction heuristics are informed by published research:

- **Google (2007)** -- "Failure Trends in a Large Disk Drive Population": After the first reallocated sector, a drive is **14x more likely to fail within 60 days**. This is why any growth in `reallocated_sector_ct` triggers an immediate CRITICAL finding.

- **Backblaze (ongoing)** -- Quarterly drive stats consistently show that `current_pending_sector`, `reallocated_sector_ct`, and `reported_uncorrect` are the strongest predictors of imminent failure for SATA drives.

- **NVMe spec** -- The `percentage_used` and `available_spare` fields are manufacturer-provided endurance indicators. When `available_spare` drops to or below `available_spare_threshold`, the manufacturer considers the drive at end of life.

## License

[AGPL-3.0](LICENSE)
