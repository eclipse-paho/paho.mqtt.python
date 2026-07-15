"""Host / process / container telemetry sampling."""

from __future__ import annotations

import os
import platform
import subprocess
import threading
import time
from typing import Dict, List, Optional


def _read_text(path: str) -> Optional[str]:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return fh.read()
    except OSError:
        return None


def cpu_model() -> Optional[str]:
    text = _read_text("/proc/cpuinfo")
    if not text:
        return platform.processor() or None
    for line in text.splitlines():
        if line.lower().startswith("model name"):
            return line.split(":", 1)[1].strip()
    return None


def scaling_governor() -> Optional[str]:
    return (_read_text("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor") or "").strip() or None


def loadavg() -> List[float]:
    try:
        return list(os.getloadavg())
    except OSError:
        return []


def process_stats(pid: int) -> dict:
    status = _read_text(f"/proc/{pid}/status") or ""
    rss_kb = None
    voluntary = None
    nonvoluntary = None
    for line in status.splitlines():
        if line.startswith("VmRSS:"):
            rss_kb = int(line.split()[1])
        elif line.startswith("voluntary_ctxt_switches:"):
            voluntary = int(line.split()[1])
        elif line.startswith("nonvoluntary_ctxt_switches:"):
            nonvoluntary = int(line.split()[1])
    return {
        "pid": pid,
        "rss_kb": rss_kb,
        "voluntary_ctxt_switches": voluntary,
        "nonvoluntary_ctxt_switches": nonvoluntary,
    }


def docker_stats(container_name: str) -> Optional[dict]:
    try:
        proc = subprocess.run(
            [
                "docker",
                "stats",
                "--no-stream",
                "--format",
                "{{.CPUPerc}};{{.MemUsage}};{{.Name}}",
                container_name,
            ],
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError:
        return None
    if proc.returncode != 0 or not proc.stdout.strip():
        return None
    line = proc.stdout.strip().splitlines()[0]
    parts = line.split(";")
    cpu = parts[0].replace("%", "").strip() if parts else None
    try:
        cpu_pct = float(cpu) if cpu else None
    except ValueError:
        cpu_pct = None
    return {"name": container_name, "cpu_pct": cpu_pct, "mem": parts[1] if len(parts) > 1 else None}


def physical_cpu_groups() -> List[List[int]]:
    """Return groups of logical CPUs sharing a physical core (SMT siblings)."""
    path = "/sys/devices/system/cpu"
    if not os.path.isdir(path):
        count = os.cpu_count() or 1
        return [[i] for i in range(count)]
    groups = {}
    for entry in sorted(os.listdir(path)):
        if not entry.startswith("cpu") or not entry[3:].isdigit():
            continue
        cpu = int(entry[3:])
        topo = os.path.join(path, entry, "topology", "core_cpus_list")
        text = _read_text(topo)
        if not text:
            groups[cpu] = [cpu]
            continue
        siblings = []
        for part in text.strip().split(","):
            if "-" in part:
                a, b = part.split("-", 1)
                siblings.extend(range(int(a), int(b) + 1))
            else:
                siblings.append(int(part))
        key = tuple(sorted(siblings))
        groups[key] = list(key)
    # Deduplicate by frozenset of siblings.
    unique = {}
    for siblings in groups.values():
        unique[frozenset(siblings)] = sorted(siblings)
    return sorted(unique.values(), key=lambda g: g[0])


def allocate_cpuset(roles: List[str], profile: str = "standard") -> Dict[str, str]:
    """Assign disjoint physical-core groups to roles.

    Rejects standard profile when fewer than len(roles) physical groups exist.
    """
    groups = physical_cpu_groups()
    if profile == "standard" and len(groups) < len(roles):
        raise RuntimeError(
            f"need {len(roles)} physical CPU groups for standard profile, found {len(groups)}"
        )
    mapping = {}
    for i, role in enumerate(roles):
        if i < len(groups):
            mapping[role] = ",".join(str(c) for c in groups[i])
        else:
            # smoke fallback: share remaining cores
            mapping[role] = ",".join(str(c) for c in groups[i % len(groups)])
    return mapping


class TelemetrySampler:
    def __init__(self, pids: Optional[Dict[str, int]] = None, containers: Optional[List[str]] = None):
        self.pids = pids or {}
        self.containers = containers or []
        self.samples: List[dict] = []
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="bench-telemetry", daemon=True)
        self._thread.start()

    def stop(self) -> List[dict]:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
        return list(self.samples)

    def _loop(self) -> None:
        while not self._stop.is_set():
            sample = {
                "ts": time.time(),
                "loadavg": loadavg(),
                "processes": {name: process_stats(pid) for name, pid in self.pids.items()},
                "containers": {name: docker_stats(name) for name in self.containers},
            }
            self.samples.append(sample)
            self._stop.wait(1.0)


def environment_metadata() -> dict:
    return {
        "python": platform.python_version(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "cpu_model": cpu_model(),
        "cpu_count": os.cpu_count(),
        "physical_cpu_groups": physical_cpu_groups(),
        "scaling_governor": scaling_governor(),
        "loadavg": loadavg(),
    }
