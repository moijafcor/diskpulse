#!/usr/bin/env python3
"""
SMART Drive Health Monitor — monitors drive health across machines/datacentres,
estimates time-to-failure, and alerts only when a drive is predicted to fail
within a configurable horizon. Fully inventory-driven; no hardcoded topology.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import subprocess
import sys
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Iterator

try:
    import yaml
except ImportError:
    print("Error: pyyaml is required. Install with: pip install pyyaml", file=sys.stderr)
    sys.exit(3)

# -----------------------------------------------------------------------------
# Constants
# -----------------------------------------------------------------------------

DEFAULT_STATE_DIR = "state"
DEFAULT_LOG_FILE = "state/diskpulse.log"
DEFAULT_MAX_WORKERS = 5
CACHE_REFRESH_DAYS = 30
SATA_ATTR_IDS = {
    5: "reallocated_sector_ct",
    9: "power_on_hours",
    10: "spin_retry_count",
    187: "reported_uncorrect",
    188: "command_timeout",
    190: "airflow_temperature_cel",
    194: "temperature_celsius",
    197: "current_pending_sector",
    198: "offline_uncorrectable",
    199: "udma_crc_error_count",
    200: "multi_zone_error_rate",
}

# smartctl exit code bitmask — bits 0-1 are command-level errors,
# bits 2-7 are SMART status indicators (not failures).
SMARTCTL_CMD_ERROR_MASK = 0x03  # bits 0-1

# Regex to validate device paths (e.g. /dev/sda, /dev/nvme0n1)
DEVICE_PATH_RE = re.compile(r"^/dev/[a-zA-Z0-9_/-]+$")

# -----------------------------------------------------------------------------
# Dataclasses
# -----------------------------------------------------------------------------


@dataclass
class DriveConfig:
    """Per-drive configuration from inventory (with resolved defaults)."""

    path: str
    mount: str | None = None
    removable: bool = False
    readonly: bool = False  # intentionally read-only mount


@dataclass
class HostConfig:
    """Resolved configuration for a single host."""

    hostname: str  # key in inventory, e.g. "cognitive"
    host: str  # IP or hostname or "localhost"
    datacentre: str
    description: str = ""
    ssh_user: str = "ubuntu"
    ssh_port: int = 22
    ssh_timeout: int = 15
    failure_horizon_days: int = 30
    alert_dedup_days: int = 7
    history_retention_days: int = 180
    check_raid: bool = False
    devices: list[DriveConfig] = field(default_factory=list)
    enabled: bool = True
    tags: list[str] = field(default_factory=list)
    # Alert channel overrides (optional)
    log_file: str | None = None
    notify_send: bool = False
    webhook_url: str | None = None
    alert_script: str | None = None
    thresholds: dict[str, Any] = field(default_factory=dict)


@dataclass
class DriveReading:
    """One SMART reading for a drive (attributes + metadata)."""

    host: str
    hostname: str
    device: str
    datacentre: str
    mount: str | None
    ts: str  # ISO format
    raw_json: dict[str, Any]
    drive_type: str  # nvme, sata_ssd, sata_hdd
    model: str = ""
    serial: str = ""
    smart_passed: bool = True
    attrs: dict[str, Any] = field(default_factory=dict)
    # Derived
    filesystem_ro: bool | None = None  # None = not checked, True = ro, False = rw
    raid_ok: bool | None = None  # None = N/A, True/False when check_raid


@dataclass
class Finding:
    """A single health finding (critical / warning / info)."""

    severity: str  # CRITICAL, WARNING, INFO
    condition: str
    evidence: str = ""
    rate: str = ""
    projected: str = ""
    action: str = ""
    condition_type: str = ""  # for dedup key


@dataclass
class Alert:
    """Fully resolved alert to send to channels."""

    finding: Finding
    reading: DriveReading
    host_config: HostConfig


# -----------------------------------------------------------------------------
# Inventory Loading
# -----------------------------------------------------------------------------


def load_inventory(path: str) -> dict[str, Any]:
    """Load and return raw inventory YAML. Exits on error."""
    p = Path(path)
    if not p.exists():
        logging.error("Inventory file not found: %s", path)
        sys.exit(3)
    with open(p, encoding="utf-8") as f:
        try:
            return yaml.safe_load(f) or {}
        except yaml.YAMLError as e:
            logging.error("Invalid YAML in inventory: %s", e)
            sys.exit(3)


def resolve_value(
    defaults: dict[str, Any],
    dc: dict[str, Any] | None,
    host: dict[str, Any] | None,
    device: dict[str, Any] | None,
    key: str,
) -> Any:
    """Resolve a config value: device > host > datacentre > defaults."""
    for source in (device, host, dc):
        if source and key in source:
            return source[key]
    return defaults.get(key)


def build_host_configs(
    inventory: dict[str, Any],
    state_dir: str,
    tags_filter: list[str] | None = None,
    dc_filter: str | None = None,
    host_filter: str | None = None,
) -> list[HostConfig]:
    """Build list of HostConfig from inventory with resolved defaults."""
    defaults = inventory.get("defaults") or {}
    datacentres = inventory.get("datacentres") or {}
    configs: list[HostConfig] = []

    for dc_name, dc_data in datacentres.items():
        if not isinstance(dc_data, dict):
            continue
        if dc_filter and dc_name != dc_filter:
            continue
        if resolve_value(defaults, dc_data, None, None, "enabled") is False:
            continue
        dc_tags = dc_data.get("tags") or []
        hosts = dc_data.get("hosts") or {}
        for hostname, h_data in hosts.items():
            if not isinstance(h_data, dict):
                continue
            if host_filter and hostname != host_filter:
                continue
            if resolve_value(defaults, dc_data, h_data, None, "enabled") is False:
                continue
            h_tags = resolve_value(defaults, dc_data, h_data, None, "tags") or []
            if tags_filter and not any(t in dc_tags for t in tags_filter) and not any(t in h_tags for t in tags_filter):
                continue
            devices_raw = h_data.get("devices") or []
            devices: list[DriveConfig] = []
            for d in devices_raw:
                if not isinstance(d, dict) or "path" not in d:
                    continue
                devices.append(
                    DriveConfig(
                        path=d["path"],
                        mount=d.get("mount"),
                        removable=d.get("removable", False),
                        readonly=d.get("readonly", False),
                    )
                )
            thresholds = defaults.get("thresholds") or {}
            th = resolve_value(defaults, dc_data, h_data, None, "thresholds")
            if isinstance(th, dict):
                thresholds = {**thresholds, **th}
            configs.append(
                HostConfig(
                    hostname=hostname,
                    host=h_data.get("host", "localhost"),
                    datacentre=dc_name,
                    description=h_data.get("description", ""),
                    ssh_user=resolve_value(defaults, dc_data, h_data, None, "ssh_user") or "ubuntu",
                    ssh_port=int(resolve_value(defaults, dc_data, h_data, None, "ssh_port") or 22),
                    ssh_timeout=int(resolve_value(defaults, dc_data, h_data, None, "ssh_timeout") or 15),
                    failure_horizon_days=int(
                        resolve_value(defaults, dc_data, h_data, None, "failure_horizon_days") or 30
                    ),
                    alert_dedup_days=int(
                        resolve_value(defaults, dc_data, h_data, None, "alert_dedup_days") or 7
                    ),
                    history_retention_days=int(
                        resolve_value(defaults, dc_data, h_data, None, "history_retention_days") or 180
                    ),
                    check_raid=h_data.get("check_raid", False),
                    devices=devices,
                    enabled=True,
                    tags=h_tags,
                    log_file=resolve_value(defaults, dc_data, h_data, None, "log_file") or os.path.join(
                        state_dir, "diskpulse.log"
                    ),
                    notify_send=bool(resolve_value(defaults, dc_data, h_data, None, "notify_send")),
                    webhook_url=resolve_value(defaults, dc_data, h_data, None, "webhook_url"),
                    alert_script=resolve_value(defaults, dc_data, h_data, None, "alert_script"),
                    thresholds=thresholds,
                )
            )
    return configs


# -----------------------------------------------------------------------------
# Device-safe names and paths
# -----------------------------------------------------------------------------


def device_safe(device_path: str) -> str:
    """Convert /dev/sda to dev-sda for use in filenames."""
    return device_path.replace("/", "-").lstrip("-") or "unknown"


def cache_path(state_dir: str, hostname: str, device: str) -> str:
    return os.path.join(state_dir, "cache", f"{hostname}_{device_safe(device)}.json")


def history_path(state_dir: str, hostname: str, device: str) -> str:
    return os.path.join(state_dir, "history", f"{hostname}_{device_safe(device)}.jsonl")


# -----------------------------------------------------------------------------
# Data Collection — run smartctl (and optional mdstat/mounts)
# -----------------------------------------------------------------------------


def run_remote(
    host_config: HostConfig,
    command: str,
    log: logging.Logger,
) -> tuple[int, str, str]:
    """Run command locally or via SSH. Returns (returncode, stdout, stderr)."""
    if host_config.host == "localhost":
        try:
            result = subprocess.run(
                command,
                shell=True,
                capture_output=True,
                text=True,
                timeout=host_config.ssh_timeout + 5,
            )
            return result.returncode, result.stdout, result.stderr
        except subprocess.TimeoutExpired:
            return -1, "", "timeout"
        except Exception as e:
            return -1, "", str(e)
    cmd = [
        "ssh",
        "-o",
        f"ConnectTimeout={host_config.ssh_timeout}",
        "-o",
        "StrictHostKeyChecking=accept-new",
        "-o",
        "BatchMode=yes",
        "-p",
        str(host_config.ssh_port),
        f"{host_config.ssh_user}@{host_config.host}",
        command,
    ]
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=host_config.ssh_timeout + 10,
        )
        return result.returncode, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return -1, "", "ssh timeout"
    except Exception as e:
        return -1, "", str(e)


def get_smart_json(host_config: HostConfig, device: str, log: logging.Logger) -> tuple[bool, dict | None]:
    """Run smartctl -j -A -H for device. Returns (success, parsed_json or None).

    smartctl uses a bitmask exit code: bits 0-1 are command-level errors
    (bad args, device open failed), bits 2-7 are SMART status indicators.
    We parse JSON output on any exit code and only treat bits 0-1 as failure.
    """
    cmd = f"sudo smartctl -j -A -H {device}"
    rc, stdout, stderr = run_remote(host_config, cmd, log)
    if rc < 0:
        # Timeout or exception — no output to parse
        return False, None
    if rc & SMARTCTL_CMD_ERROR_MASK:
        # Bits 0-1 set: command-line or device-open error
        log.warning("%s:%s: smartctl command error (exit %d): %s",
                    host_config.hostname, device, rc, stderr.strip())
        return False, None
    try:
        return True, json.loads(stdout)
    except json.JSONDecodeError:
        log.warning("%s:%s: smartctl returned non-JSON output (exit %d)",
                    host_config.hostname, device, rc)
        return False, None


def get_proc_mounts(host_config: HostConfig, log: logging.Logger) -> tuple[bool, str]:
    """Get /proc/mounts content from host."""
    rc, stdout, stderr = run_remote(host_config, "cat /proc/mounts", log)
    return rc == 0, stdout or ""


def get_mdstat(host_config: HostConfig, log: logging.Logger) -> tuple[bool, str]:
    """Get /proc/mdstat content from host."""
    rc, stdout, stderr = run_remote(host_config, "cat /proc/mdstat", log)
    return rc == 0, stdout or ""


def parse_mounts_ro(mounts_content: str) -> dict[str, bool]:
    """Parse /proc/mounts; return dict mount_point -> is_readonly."""
    result: dict[str, bool] = {}
    for line in mounts_content.strip().split("\n"):
        parts = line.split()
        if len(parts) >= 4:
            mount_point = parts[1]
            options = parts[3]
            result[mount_point] = "ro" in options.split(",")
    return result


def parse_mdstat_degraded(mdstat_content: str) -> bool:
    """Return True if any RAID array is degraded (not [UU])."""
    if "[U_]" in mdstat_content or "[_U]" in mdstat_content:
        return True
    return False


def get_drive_info_cached(
    state_dir: str,
    hostname: str,
    device: str,
    host_config: HostConfig,
    log: logging.Logger,
) -> dict[str, Any]:
    """Get drive identity (model, type, etc.) from smartctl -i, with file cache."""
    path = cache_path(state_dir, hostname, device)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if os.path.exists(path):
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            if data.get("_cached_ts"):
                try:
                    cached = datetime.fromisoformat(data["_cached_ts"])
                    if (datetime.now() - cached).days < CACHE_REFRESH_DAYS:
                        return data
                except (ValueError, TypeError):
                    pass
        except (json.JSONDecodeError, OSError):
            pass
    cmd = f"sudo smartctl -j -i {device}"
    rc, stdout, stderr = run_remote(host_config, cmd, log)
    if rc < 0 or (rc & SMARTCTL_CMD_ERROR_MASK):
        return {}
    try:
        raw = json.loads(stdout)
    except json.JSONDecodeError:
        return {}
    model = (raw.get("model_name") or "").strip() or "Unknown"
    serial = raw.get("serial_number", "").strip()
    rotation = raw.get("rotation_rate")
    if "nvme" in device.lower() or raw.get("protocol", "").lower() == "nvme":
        drive_type = "nvme"
    elif rotation == 0:
        drive_type = "sata_ssd"
    else:
        drive_type = "sata_hdd"
    capacity = raw.get("user_capacity", {})
    if isinstance(capacity, dict):
        bytes_cap = capacity.get("bytes") or 0
    else:
        bytes_cap = 0
    data = {
        "model": model,
        "serial": serial,
        "drive_type": drive_type,
        "capacity_bytes": bytes_cap,
        "_cached_ts": datetime.now().isoformat(),
    }
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, separators=(",", ":"))
    except OSError:
        pass
    return data


# -----------------------------------------------------------------------------
# Attribute Extraction (NVMe + SATA)
# -----------------------------------------------------------------------------


def extract_nvme_attrs(smart_json: dict[str, Any]) -> dict[str, Any]:
    """Extract NVMe attributes from smartctl JSON."""
    attrs: dict[str, Any] = {}
    nvme_log = smart_json.get("nvme_smart_health_information_log") or {}
    attrs["percentage_used"] = nvme_log.get("percentage_used")
    attrs["available_spare"] = nvme_log.get("available_spare")
    attrs["available_spare_threshold"] = nvme_log.get("available_spare_threshold")
    attrs["media_errors"] = nvme_log.get("media_and_data_integrity_errors")
    attrs["critical_warning"] = smart_json.get("critical_warning", 0)
    attrs["temperature"] = nvme_log.get("temperature")
    if attrs["temperature"] is None:
        attrs["temperature"] = smart_json.get("temperature")
    attrs["power_on_hours"] = None  # NVMe may not expose in same place
    for key in ("power_on_hours", "power_cycle_count"):
        if key in nvme_log:
            attrs[key] = nvme_log[key]
    attrs["data_units_written"] = nvme_log.get("data_units_written")
    return {k: v for k, v in attrs.items() if v is not None}


def extract_sata_attrs(smart_json: dict[str, Any]) -> dict[str, Any]:
    """Extract SATA attributes by ID from smartctl JSON."""
    attrs: dict[str, Any] = {}
    table = smart_json.get("ata_smart_attributes", {}).get("table") or []
    by_id: dict[int, Any] = {}
    for item in table:
        if isinstance(item, dict) and "id" in item:
            raw = item.get("raw", {})
            val = raw.get("value") if isinstance(raw, dict) else None
            if val is not None:
                by_id[item["id"]] = val
    for aid, name in SATA_ATTR_IDS.items():
        if aid in by_id:
            attrs[name] = by_id[aid]
    # Temperature: prefer 194, else 190
    if "temperature_celsius" in attrs:
        attrs["temperature"] = attrs["temperature_celsius"]
    elif "airflow_temperature_cel" in attrs:
        attrs["temperature"] = attrs["airflow_temperature_cel"]
    return attrs


def smart_passed(smart_json: dict[str, Any]) -> bool:
    """Check SMART overall health (PASSED/FAILED)."""
    status = smart_json.get("smart_status", {})
    if isinstance(status, dict):
        return status.get("passed", True)
    return True


def collect_host(
    host_config: HostConfig,
    state_dir: str,
    log: logging.Logger,
) -> tuple[list[DriveReading], list[str], bool]:
    """
    Collect all drive readings for one host.
    Returns (readings, errors, raid_ok).
    """
    readings: list[DriveReading] = []
    errors: list[str] = []
    raid_ok: bool | None = None

    # One mounts + mdstat per host
    mount_ro: dict[str, bool] = {}
    if any(d.mount for d in host_config.devices):
        ok, mounts_out = get_proc_mounts(host_config, log)
        if ok:
            mount_ro = parse_mounts_ro(mounts_out)
        else:
            log.warning("%s:%s: could not read /proc/mounts", host_config.datacentre, host_config.hostname)
    if host_config.check_raid:
        ok, md_out = get_mdstat(host_config, log)
        if ok:
            raid_ok = not parse_mdstat_degraded(md_out)
        else:
            raid_ok = False
            errors.append(f"{host_config.hostname}: failed to read mdstat")

    for dev_config in host_config.devices:
        device = dev_config.path
        ok, smart_json = get_smart_json(host_config, device, log)
        if not ok:
            if dev_config.removable:
                log.info("%s:%s:%s: device not found (removable)", host_config.datacentre, host_config.hostname, device)
            else:
                log.warning("%s:%s:%s: smartctl failed", host_config.datacentre, host_config.hostname, device)
                errors.append(f"{host_config.hostname}:{device}: smartctl failed")
            continue
        if not smart_json:
            continue
        info = get_drive_info_cached(state_dir, host_config.hostname, device, host_config, log)
        model = info.get("model", "Unknown")
        serial = info.get("serial", "")
        drive_type = info.get("drive_type", "sata_hdd")
        if "nvme_smart_health_information_log" in smart_json:
            drive_type = "nvme"
            attrs = extract_nvme_attrs(smart_json)
        else:
            attrs = extract_sata_attrs(smart_json)
            if "rotation_rate" in smart_json:
                drive_type = "sata_ssd" if smart_json["rotation_rate"] == 0 else "sata_hdd"
            else:
                drive_type = info.get("drive_type", "sata_hdd")
        passed = smart_passed(smart_json)
        fs_ro: bool | None = None
        if dev_config.mount and dev_config.mount in mount_ro:
            fs_ro = mount_ro[dev_config.mount]
        readings.append(
            DriveReading(
                host=host_config.host,
                hostname=host_config.hostname,
                device=device,
                datacentre=host_config.datacentre,
                mount=dev_config.mount,
                ts=datetime.now().isoformat(),
                raw_json=smart_json,
                drive_type=drive_type,
                model=model,
                serial=serial,
                smart_passed=passed,
                attrs=attrs,
                filesystem_ro=fs_ro,
                raid_ok=raid_ok,
            )
        )
    return readings, errors, raid_ok if host_config.check_raid else None


def collect_all(
    host_configs: list[HostConfig],
    state_dir: str,
    max_workers: int,
    log: logging.Logger,
) -> tuple[list[DriveReading], list[str]]:
    """Collect readings from all hosts in parallel. Returns (readings, all_errors)."""
    all_readings: list[DriveReading] = []
    all_errors: list[str] = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(collect_host, hc, state_dir, log): hc for hc in host_configs}
        for fut in as_completed(futures):
            hc = futures[fut]
            try:
                readings, errs, _ = fut.result()
                all_readings.extend(readings)
                all_errors.extend(errs)
            except Exception as e:
                log.warning("%s:%s: collection failed: %s", hc.datacentre, hc.hostname, e)
                all_errors.append(f"{hc.hostname}: {e}")
    return all_readings, all_errors


# -----------------------------------------------------------------------------
# History Management
# -----------------------------------------------------------------------------


def append_history(
    state_dir: str,
    hostname: str,
    device: str,
    record: dict[str, Any],
    retention_days: int,
    log: logging.Logger,
) -> None:
    """Append one JSONL record and prune old lines."""
    path = history_path(state_dir, hostname, device)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    line = json.dumps(record, separators=(",", ":")) + "\n"
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write(line)
    except OSError as e:
        log.warning("Could not write history %s: %s", path, e)
        return
    cutoff = (datetime.now() - timedelta(days=retention_days)).isoformat()
    try:
        with open(path, "r", encoding="utf-8") as f:
            lines = [ln for ln in f if ln.strip()]
        kept = []
        for ln in lines:
            try:
                obj = json.loads(ln)
                if (obj.get("ts") or "") >= cutoff:
                    kept.append(ln.rstrip("\n"))
            except json.JSONDecodeError:
                kept.append(ln.rstrip("\n"))
        if len(kept) < len(lines):
            with open(path, "w", encoding="utf-8") as f:
                for ln in kept:
                    f.write(ln + "\n")
    except OSError:
        pass


def read_history(
    state_dir: str,
    hostname: str,
    device: str,
    within_days: int,
    log: logging.Logger,
) -> list[dict[str, Any]]:
    """Stream history file and return records within the last N days."""
    path = history_path(state_dir, hostname, device)
    if not os.path.exists(path):
        return []
    cutoff = (datetime.now() - timedelta(days=within_days)).isoformat()
    records = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    if (obj.get("ts") or "") >= cutoff:
                        records.append(obj)
                except json.JSONDecodeError:
                    continue
    except OSError:
        return []
    return records


# -----------------------------------------------------------------------------
# Failure Prediction Engine
# -----------------------------------------------------------------------------


def findings_for_reading(
    reading: DriveReading,
    host_config: HostConfig,
    state_dir: str,
    log: logging.Logger,
) -> list[Finding]:
    """Compute all findings for one drive reading (critical + warning)."""
    findings: list[Finding] = []
    horizon = host_config.failure_horizon_days
    history = read_history(
        state_dir,
        reading.hostname,
        reading.device,
        horizon + 30,
        log,
    )
    attrs = reading.attrs
    dt = reading.drive_type

    # --- Immediate CRITICAL ---
    if dt == "nvme":
        cw = attrs.get("critical_warning")
        if cw is not None and cw != 0:
            findings.append(
                Finding(
                    severity="CRITICAL",
                    condition="NVMe critical_warning is non-zero",
                    evidence=f"critical_warning={cw}",
                    condition_type="nvme_critical_warning",
                )
            )
        spare = attrs.get("available_spare")
        thr = attrs.get("available_spare_threshold")
        if spare is not None and thr is not None and spare <= thr:
            findings.append(
                Finding(
                    severity="CRITICAL",
                    condition="NVMe available spare at or below threshold",
                    evidence=f"available_spare={spare}% threshold={thr}%",
                    condition_type="nvme_spare_exhausted",
                )
            )
        me = attrs.get("media_errors")
        if me is not None and me > 0 and history:
            prev_me = None
            for h in reversed(history):
                p = h.get("attrs", {}).get("media_errors")
                if p is not None:
                    prev_me = p
                    break
            if prev_me is not None and me > prev_me:
                findings.append(
                    Finding(
                        severity="CRITICAL",
                        condition="NVMe media errors increasing",
                        evidence=f"media_errors: {prev_me} -> {me}",
                        condition_type="nvme_media_errors_rising",
                    )
                )

    if dt in ("sata_ssd", "sata_hdd"):
        if not reading.smart_passed:
            findings.append(
                Finding(
                    severity="CRITICAL",
                    condition="SMART overall status FAILED",
                    evidence="smart_status failed",
                    condition_type="sata_smart_failed",
                )
            )
        pending = attrs.get("current_pending_sector") or 0
        if pending > 0 and history:
            prev_p = None
            for h in reversed(history):
                p = h.get("attrs", {}).get("current_pending_sector")
                if p is not None:
                    prev_p = p
                    break
            if prev_p is not None and pending > prev_p:
                findings.append(
                    Finding(
                        severity="CRITICAL",
                        condition="Current pending sector count increasing",
                        evidence=f"current_pending_sector: {prev_p} -> {pending}",
                        condition_type="sata_pending_rising",
                    )
                )

    # Read-only filesystem (but not configured readonly in inventory)
    if reading.filesystem_ro is True and reading.mount:
        device_readonly_inventory = False
        for d in host_config.devices:
            if d.path == reading.device:
                device_readonly_inventory = d.readonly
                break
        if not device_readonly_inventory:
            findings.append(
                Finding(
                    severity="CRITICAL",
                    condition="Filesystem mounted read-only (likely after I/O errors)",
                    evidence=f"mount {reading.mount} is ro",
                    action="Check dmesg and replace drive",
                    condition_type="fs_ro",
                )
            )

    # RAID degraded
    if host_config.check_raid and reading.raid_ok is False:
        findings.append(
            Finding(
                severity="CRITICAL",
                condition="RAID array degraded",
                evidence="/proc/mdstat shows degraded array",
                action="Replace failed member and rebuild",
                condition_type="raid_degraded",
            )
        )

    # --- Trend-based CRITICAL ---
    if dt in ("sata_ssd", "sata_hdd") and history:
        realloc_now = attrs.get("reallocated_sector_ct") or 0
        realloc_old = None
        for h in reversed(history):
            r = h.get("attrs", {}).get("reallocated_sector_ct")
            if r is not None:
                realloc_old = r
                break
        if realloc_old is not None and realloc_now > realloc_old:
            findings.append(
                Finding(
                    severity="CRITICAL",
                    condition="Reallocated sector count growing within failure horizon",
                    evidence=f"reallocated_sector_ct: {realloc_old} -> {realloc_now}",
                    rate="active degradation",
                    projected=f"~30 days (research: 14x higher failure after first realloc)",
                    action="Replace drive soon",
                    condition_type="sata_realloc_growth",
                )
            )

    if dt == "nvme" and history:
        # Endurance: percentage_used growth
        pu_now = attrs.get("percentage_used")
        if pu_now is not None and len(history) >= 2:
            first_ts = history[0].get("ts")
            last_ts = history[-1].get("ts")
            pu_first = history[0].get("attrs", {}).get("percentage_used")
            if pu_first is not None and first_ts and last_ts:
                try:
                    t0 = datetime.fromisoformat(first_ts.replace("Z", "+00:00"))
                    t1 = datetime.fromisoformat(last_ts.replace("Z", "+00:00"))
                    days_elapsed = max((t1 - t0).total_seconds() / 86400, 0.1)
                    growth = pu_now - pu_first
                    if growth > 0:
                        rate_per_day = growth / days_elapsed
                        days_to_100 = (100 - pu_now) / rate_per_day if rate_per_day > 0 else 9999
                        if days_to_100 < horizon:
                            findings.append(
                                Finding(
                                    severity="CRITICAL",
                                    condition=f"NVMe endurance projected to 100% within {horizon} days",
                                    evidence=f"percentage_used: {pu_first}% -> {pu_now}% over {days_elapsed:.0f} days",
                                    rate=f"{rate_per_day:.2f}%/day",
                                    projected=f"~{int(days_to_100)} days to 100%",
                                    action="Replace drive before wear-out",
                                    condition_type="nvme_endurance",
                                )
                            )
                except (ValueError, TypeError):
                    pass
        # Spare depletion
        spare_now = attrs.get("available_spare")
        thr = attrs.get("available_spare_threshold")
        if spare_now is not None and thr is not None and len(history) >= 2:
            spare_first = history[0].get("attrs", {}).get("available_spare")
            if spare_first is not None and spare_now < spare_first:
                try:
                    first_ts = history[0].get("ts")
                    last_ts = history[-1].get("ts")
                    t0 = datetime.fromisoformat(first_ts.replace("Z", "+00:00"))
                    t1 = datetime.fromisoformat(last_ts.replace("Z", "+00:00"))
                    days_elapsed = max((t1 - t0).total_seconds() / 86400, 0.1)
                    decline = spare_first - spare_now
                    rate_per_day = decline / days_elapsed
                    if rate_per_day > 0 and thr > 0:
                        days_to_thr = (spare_now - thr) / rate_per_day
                        if days_to_thr < horizon and days_to_thr > 0:
                            findings.append(
                                Finding(
                                    severity="CRITICAL",
                                    condition="NVMe spare blocks depleting; projected below threshold within horizon",
                                    evidence=f"available_spare: {spare_first}% -> {spare_now}%",
                                    rate=f"{rate_per_day:.2f}%/day",
                                    projected=f"~{int(days_to_thr)} days to threshold",
                                    action="Replace drive",
                                    condition_type="nvme_spare_depletion",
                                )
                            )
                except (ValueError, TypeError):
                    pass

    # --- WARNING (log only) ---
    th = host_config.thresholds
    temp = attrs.get("temperature")
    if temp is not None:
        warn_key = "nvme_temp_warn" if dt == "nvme" else "sata_ssd_temp_warn" if dt == "sata_ssd" else "sata_hdd_temp_warn"
        limit = th.get(warn_key, 70 if dt == "nvme" else 60 if dt == "sata_ssd" else 50)
        consecutive = th.get("temp_consecutive_readings", 3)
        if temp >= limit and len(history) >= consecutive:
            recent = history[-consecutive:]
            if all((r.get("attrs") or {}).get("temperature", 0) >= limit for r in recent):
                findings.append(
                    Finding(
                        severity="WARNING",
                        condition=f"Sustained high temperature ({consecutive}+ readings >= {limit}°C)",
                        evidence=f"current temperature={temp}°C",
                        condition_type="temp_sustained",
                    )
                )
    if dt in ("sata_ssd", "sata_hdd"):
        crc = attrs.get("udma_crc_error_count") or 0
        if crc > 0 and history:
            prev_crc = None
            for h in reversed(history):
                p = h.get("attrs", {}).get("udma_crc_error_count")
                if p is not None:
                    prev_crc = p
                    break
            if prev_crc is not None and crc > prev_crc:
                findings.append(
                    Finding(
                        severity="WARNING",
                        condition="UDMA CRC errors growing — check SATA cable/controller",
                        evidence=f"udma_crc_error_count: {prev_crc} -> {crc}",
                        condition_type="udma_crc",
                    )
                )
        if (attrs.get("spin_retry_count") or 0) > 0:
            findings.append(
                Finding(
                    severity="WARNING",
                    condition="Spin retry count > 0 (HDD mechanical stress)",
                    evidence=f"spin_retry_count={attrs.get('spin_retry_count')}",
                    condition_type="spin_retry",
                )
            )
        cmd_to = attrs.get("command_timeout") or 0
        if cmd_to > 0 and history:
            prev_ct = None
            for h in reversed(history):
                p = h.get("attrs", {}).get("command_timeout")
                if p is not None:
                    prev_ct = p
                    break
            if prev_ct is not None and cmd_to > prev_ct:
                findings.append(
                    Finding(
                        severity="WARNING",
                        condition="Command timeout count growing",
                        evidence=f"command_timeout: {prev_ct} -> {cmd_to}",
                        condition_type="command_timeout",
                    )
                )

    return findings


# -----------------------------------------------------------------------------
# Alert Channels
# -----------------------------------------------------------------------------


def load_alerts_state(state_dir: str) -> dict[str, Any]:
    """Load alerts.json for deduplication."""
    path = os.path.join(state_dir, "alerts.json")
    if not os.path.exists(path):
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def save_alerts_state(state_dir: str, data: dict[str, Any]) -> None:
    path = os.path.join(state_dir, "alerts.json")
    os.makedirs(state_dir, exist_ok=True)
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, separators=(",", ":"))
    except OSError:
        pass


def dedup_key(reading: DriveReading, finding: Finding) -> str:
    return f"{reading.hostname}:{reading.device}:{finding.condition_type or finding.condition}"


def should_fire_alert(
    alerts_state: dict[str, Any],
    reading: DriveReading,
    finding: Finding,
    host_config: HostConfig,
    force: bool,
) -> bool:
    """True if we should fire this alert (not deduplicated)."""
    if force:
        return True
    key = dedup_key(reading, finding)
    last = alerts_state.get(key)
    if not last:
        return True
    try:
        last_ts = datetime.fromisoformat(last.get("ts", "").replace("Z", "+00:00"))
        if (datetime.now(last_ts.tzinfo) - last_ts).days >= host_config.alert_dedup_days:
            return True
        # Re-alert if severity increased (e.g. same condition worse)
        if last.get("severity") != finding.severity or last.get("evidence") != finding.evidence:
            return True
    except (ValueError, TypeError):
        return True
    return False


def record_alert_fired(alerts_state: dict[str, Any], reading: DriveReading, finding: Finding) -> None:
    key = dedup_key(reading, finding)
    alerts_state[key] = {
        "ts": datetime.now().isoformat(),
        "severity": finding.severity,
        "evidence": finding.evidence,
        "condition_type": finding.condition_type,
    }


def format_alert_message(alert: Alert) -> str:
    """Human-readable alert body."""
    r = alert.reading
    f = alert.finding
    return f"""SMART ALERT: Drive failure predicted within horizon

Datacentre: {r.datacentre}
Host:       {r.hostname} ({r.host})
Device:     {r.device}
Model:      {r.model}
Serial:     {r.serial}
Mount:      {r.mount or '\u2014'}
Condition:  {f.condition}
Evidence:   {f.evidence}
Rate:       {f.rate or '\u2014'}
Projected:  {f.projected or '\u2014'}
Action:     {f.action or '\u2014'}

Full SMART dump:
{json.dumps(r.raw_json, indent=2)}
"""


def send_alert_log(alert: Alert, host_config: HostConfig, state_dir: str) -> None:
    """Write CRITICAL to log file."""
    log_path = host_config.log_file or os.path.join(state_dir, "diskpulse.log")
    os.makedirs(os.path.dirname(log_path) or ".", exist_ok=True)
    msg = format_alert_message(alert)
    try:
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(f"\n[{datetime.now().isoformat()}] SMART ALERT\n{msg}\n")
    except OSError:
        pass


def send_alert_webhook(alert: Alert, webhook_url: str, log: logging.Logger) -> None:
    """POST alert JSON to webhook URL."""
    r = alert.reading
    f = alert.finding
    payload = {
        "datacentre": r.datacentre,
        "host": r.hostname,
        "host_ip": r.host,
        "device": r.device,
        "model": r.model,
        "serial": r.serial,
        "mount": r.mount,
        "condition": f.condition,
        "evidence": f.evidence,
        "rate": f.rate,
        "projected": f.projected,
        "action": f.action,
        "severity": f.severity,
        "full_smart": r.raw_json,
    }
    try:
        req = urllib.request.Request(
            webhook_url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            pass
    except Exception as e:
        log.warning("Webhook POST failed: %s", e)


def send_alert_notify_send(alert: Alert) -> None:
    """Desktop notification via notify-send."""
    try:
        title = "SMART Alert"
        body = f"{alert.reading.hostname} {alert.reading.device}: {alert.finding.condition}"
        subprocess.run(
            ["notify-send", title, body, "--urgency=critical"],
            capture_output=True,
            timeout=5,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass


def send_alert_script(alert: Alert, script_path: str, log: logging.Logger) -> None:
    """Run custom script with alert JSON on stdin."""
    payload = {
        "reading": {
            "host": alert.reading.host,
            "hostname": alert.reading.hostname,
            "device": alert.reading.device,
            "datacentre": alert.reading.datacentre,
            "model": alert.reading.model,
            "serial": alert.reading.serial,
            "mount": alert.reading.mount,
        },
        "finding": {
            "severity": alert.finding.severity,
            "condition": alert.finding.condition,
            "evidence": alert.finding.evidence,
            "rate": alert.finding.rate,
            "projected": alert.finding.projected,
            "action": alert.finding.action,
        },
    }
    try:
        proc = subprocess.Popen(
            [script_path],
            stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        proc.communicate(input=json.dumps(payload).encode("utf-8"), timeout=30)
    except Exception as e:
        log.warning("Alert script failed: %s", e)


def fire_alert(
    alert: Alert,
    dry_run: bool,
    state_dir: str,
    alerts_state: dict[str, Any],
    log: logging.Logger,
) -> None:
    """Send alert to all configured channels (unless dry_run)."""
    hc = alert.host_config
    if dry_run:
        log.critical("DRY-RUN ALERT: %s", format_alert_message(alert))
        return
    record_alert_fired(alerts_state, alert.reading, alert.finding)
    send_alert_log(alert, hc, state_dir)
    if hc.webhook_url:
        send_alert_webhook(alert, hc.webhook_url, log)
    if hc.notify_send:
        send_alert_notify_send(alert)
    if hc.alert_script:
        send_alert_script(alert, hc.alert_script, log)


# -----------------------------------------------------------------------------
# CLI and Status Display
# -----------------------------------------------------------------------------


def validate_scan_target(target: str) -> None:
    """Validate --scan target is localhost or user@host pattern. Exits on invalid."""
    if target == "localhost":
        return
    if re.match(r"^[a-zA-Z0-9._-]+@[a-zA-Z0-9._-]+$", target):
        return
    print(f"Error: Invalid --scan target: {target!r} (expected 'localhost' or 'user@host')", file=sys.stderr)
    sys.exit(3)


def run_scan(target: str) -> None:
    """Scan a host for drives and print YAML snippet."""
    validate_scan_target(target)
    if target == "localhost":
        result = subprocess.run(
            ["sudo", "smartctl", "--scan"],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode != 0:
            print(result.stderr or result.stdout, file=sys.stderr)
            sys.exit(2)
        scan_out = result.stdout

        def run_info(dev: str) -> subprocess.CompletedProcess:
            if not DEVICE_PATH_RE.match(dev):
                return subprocess.CompletedProcess(args=[], returncode=1, stdout="", stderr="invalid device")
            return subprocess.run(
                ["sudo", "smartctl", "-j", "-i", dev],
                capture_output=True,
                text=True,
                timeout=10,
            )
    else:
        result = subprocess.run(
            ["ssh", "-o", "ConnectTimeout=15", "-o", "BatchMode=yes", target, "sudo smartctl --scan"],
            capture_output=True,
            text=True,
            timeout=25,
        )
        if result.returncode != 0:
            print(result.stderr or result.stdout, file=sys.stderr)
            sys.exit(2)
        scan_out = result.stdout

        def run_info(dev: str) -> subprocess.CompletedProcess:
            if not DEVICE_PATH_RE.match(dev):
                return subprocess.CompletedProcess(args=[], returncode=1, stdout="", stderr="invalid device")
            return subprocess.run(
                ["ssh", "-o", "ConnectTimeout=15", "-o", "BatchMode=yes", target,
                 f"sudo smartctl -j -i {dev}"],
                capture_output=True,
                text=True,
                timeout=15,
            )

    devices: list[str] = []
    for line in scan_out.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if parts and DEVICE_PATH_RE.match(parts[0]):
            devices.append(parts[0])
    host_for_yaml = target.split("@")[-1] if "@" in target else target
    print(f"# Discovered drives on {target}:")
    print("    hosts:")
    print("      new-host:")
    print(f"        host: {host_for_yaml}")
    print("        devices:")
    for dev in devices:
        r = run_info(dev)
        model = "Unknown"
        capacity = ""
        dtype = "Unknown"
        if r.returncode == 0 and r.stdout:
            try:
                j = json.loads(r.stdout)
                model = (j.get("model_name") or j.get("model_family") or "Unknown").strip()
                cap = j.get("user_capacity", {})
                if isinstance(cap, dict) and cap.get("bytes"):
                    gb = cap["bytes"] / (1024**3)
                    capacity = f", {gb:.0f}GB"
                rot = j.get("rotation_rate")
                if "nvme" in dev.lower() or j.get("protocol", "").lower() == "nvme":
                    dtype = "NVMe"
                elif rot == 0:
                    dtype = "SATA SSD"
                else:
                    dtype = "SATA HDD"
            except json.JSONDecodeError:
                pass
        print(f"          - path: {dev}       # {model} ({dtype}{capacity})")
    sys.exit(0)


def status_display(
    readings: list[DriveReading],
    host_configs: list[HostConfig],
    raid_status: dict[tuple[str, str], bool],
    findings_map: dict[str, list[Finding]] | None = None,
) -> None:
    """Print human-readable status table."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"SMART Drive Health \u2014 {now}")
    print("=" * 79)
    by_dc: dict[str, list[tuple[HostConfig, list[DriveReading]]]] = {}
    for hc in host_configs:
        if hc.datacentre not in by_dc:
            by_dc[hc.datacentre] = []
        rs = [r for r in readings if r.hostname == hc.hostname and r.datacentre == hc.datacentre]
        by_dc[hc.datacentre].append((hc, rs))

    total_drives = 0
    failed_drives = 0
    warning_count = 0
    critical_count = 0
    for dc_name in sorted(by_dc.keys()):
        print(f"\n{dc_name}")
        print("-" * 79)
        for hc, host_readings in sorted(by_dc[dc_name], key=lambda x: x[0].hostname):
            raid_ok = raid_status.get((hc.datacentre, hc.hostname))
            raid_str = " RAID: OK [UU]" if raid_ok is True else " RAID: DEGRADED" if raid_ok is False else ""
            print(f"  {hc.hostname} ({hc.host}){raid_str}")
            for r in sorted(host_readings, key=lambda x: x.device):
                total_drives += 1
                if not r.smart_passed:
                    failed_drives += 1
                model_short = (r.model or "Unknown")[:20]
                temp = r.attrs.get("temperature")
                if temp is None:
                    temp = r.attrs.get("temperature_celsius")
                temp_str = f"{temp}\u00b0C" if temp is not None else "\u2014"
                if r.drive_type == "nvme":
                    spare = r.attrs.get("available_spare")
                    used = r.attrs.get("percentage_used")
                    extra = f"spare:{spare}% used:{used}%" if spare is not None else f"used:{used}%" if used is not None else ""
                    hours = r.attrs.get("power_on_hours") or ""
                else:
                    realloc = r.attrs.get("reallocated_sector_ct", "\u2014")
                    pending = r.attrs.get("current_pending_sector", "\u2014")
                    extra = f"realloc:{realloc} pending:{pending}"
                    hours = r.attrs.get("power_on_hours")
                hours_str = f"{hours:,}h" if isinstance(hours, (int, float)) else str(hours) if hours else "\u2014"
                ro_str = "RO!" if r.filesystem_ro else "rw" if r.filesystem_ro is False else "\u2014"
                health = "PASSED" if r.smart_passed else "FAILED"
                # Count findings for this drive
                drive_key = f"{r.datacentre}:{r.hostname}:{r.device}"
                drive_findings = findings_map.get(drive_key, []) if findings_map else []
                for f in drive_findings:
                    if f.severity == "CRITICAL":
                        critical_count += 1
                    elif f.severity == "WARNING":
                        warning_count += 1
                print(f"    {r.device}   {model_short:20} {hours_str:>8}  {temp_str:>4}  {extra:24}  {ro_str:3}  {health}")

    # Dynamic summary line
    parts = [f"{total_drives} drives across {len(host_configs)} hosts"]
    if critical_count == 0 and warning_count == 0 and failed_drives == 0:
        parts.append("all healthy")
    else:
        if failed_drives > 0:
            parts.append(f"{failed_drives} FAILED")
        if critical_count > 0:
            parts.append(f"{critical_count} critical")
        if warning_count > 0:
            parts.append(f"{warning_count} warnings")
    print(f"\n{' \u2014 '.join(parts)}")


def main() -> int:
    """Main entry point. Returns exit code 0/1/2/3."""
    parser = argparse.ArgumentParser(description="SMART drive health monitor")
    parser.add_argument("-c", "--config", dest="inventory", help="Path to inventory YAML")
    parser.add_argument("--state-dir", default=DEFAULT_STATE_DIR, help="State directory")
    parser.add_argument("--dry-run", action="store_true", help="Do not fire alerts")
    parser.add_argument("--status", action="store_true", help="Show status dashboard")
    parser.add_argument("--dc", dest="dc_filter", help="Filter by datacentre name")
    parser.add_argument("--host", dest="host_filter", help="Filter by host name")
    parser.add_argument("--tags", help="Comma-separated tags to filter (only run hosts with any tag)")
    parser.add_argument("--force", action="store_true", help="Ignore alert deduplication")
    parser.add_argument("-v", "--verbose", action="count", default=0, help="Verbose (repeat for debug)")
    parser.add_argument("--max-workers", type=int, default=DEFAULT_MAX_WORKERS, help="Parallel hosts")
    parser.add_argument("--scan", metavar="USER@HOST_OR_LOCALHOST", help="Discover drives and print YAML")
    args = parser.parse_args()

    if args.scan:
        run_scan(args.scan)
        return 0

    if not args.inventory:
        print("Error: Inventory file required (-c/--config) unless using --scan", file=sys.stderr)
        sys.exit(3)
    state_dir = args.state_dir
    os.makedirs(os.path.join(state_dir, "cache"), exist_ok=True)
    os.makedirs(os.path.join(state_dir, "history"), exist_ok=True)

    log_level = logging.WARNING
    if args.verbose >= 2:
        log_level = logging.DEBUG
    elif args.verbose >= 1:
        log_level = logging.INFO
    logging.basicConfig(level=log_level, format="%(levelname)s %(message)s")
    log = logging.getLogger("diskpulse")

    inventory = load_inventory(args.inventory)
    tags_filter = [t.strip() for t in args.tags.split(",")] if args.tags else None
    host_configs = build_host_configs(
        inventory,
        state_dir,
        tags_filter=tags_filter,
        dc_filter=args.dc_filter,
        host_filter=args.host_filter,
    )
    if not host_configs:
        log.warning("No hosts matched inventory/filters")
        return 0

    readings, errors = collect_all(host_configs, state_dir, args.max_workers, log)
    hc_by_key = {(h.datacentre, h.hostname): h for h in host_configs}
    raid_status: dict[tuple[str, str], bool] = {}
    for r in readings:
        if r.raid_ok is not None:
            raid_status[(r.datacentre, r.hostname)] = r.raid_ok

    # Persist history
    for r in readings:
        record = {"ts": r.ts, "attrs": r.attrs}
        append_history(
            state_dir,
            r.hostname,
            r.device,
            record,
            hc_by_key.get((r.datacentre, r.hostname), host_configs[0]).history_retention_days,
            log,
        )

    # Compute findings for all drives
    findings_map: dict[str, list[Finding]] = {}
    for r in readings:
        hc = hc_by_key.get((r.datacentre, r.hostname))
        if not hc:
            continue
        drive_key = f"{r.datacentre}:{r.hostname}:{r.device}"
        findings_map[drive_key] = findings_for_reading(r, hc, state_dir, log)

    if args.status:
        status_display(readings, host_configs, raid_status, findings_map)
        return 0

    # Fire alerts — load dedup state once for the whole run
    alerts_state = load_alerts_state(state_dir)
    critical_count = 0
    for r in readings:
        hc = hc_by_key.get((r.datacentre, r.hostname))
        if not hc:
            continue
        drive_key = f"{r.datacentre}:{r.hostname}:{r.device}"
        for f in findings_map.get(drive_key, []):
            if f.severity == "CRITICAL":
                alert = Alert(finding=f, reading=r, host_config=hc)
                if should_fire_alert(alerts_state, r, f, hc, args.force):
                    fire_alert(alert, args.dry_run, state_dir, alerts_state, log)
                    critical_count += 1
    # Persist dedup state once at the end
    if critical_count > 0:
        save_alerts_state(state_dir, alerts_state)

    if critical_count > 0:
        return 1
    if errors:
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
