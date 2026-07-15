"""emqtt-bench load generator wrapper and output parser."""

from __future__ import annotations

import re
import subprocess
import time
from dataclasses import dataclass
from typing import List, Optional

from broker import EMQTT_BENCH_IMAGE, image_digest

# Modern emqtt-bench lines look like:
# 1s pub total=40 rate=39.92/sec
# 1s connect_succ total=2 rate=2.00/sec
# Older builds used:
# pub(27140): total=12345 rate=9876(msg/sec)
RATE_RE = re.compile(
    r"(?:(?P<kind>pub|recv|conn|connect_succ|connect_fail)\(\d+\):\s*total=(?P<total>\d+)(?:,)?\s*rate=(?P<rate>[\d.]+)(?:\(msg/sec\))?"
    r"|\d+s\s+(?P<kind2>pub|recv|conn|connect_succ|connect_fail)\s+total=(?P<total2>\d+)\s+rate=(?P<rate2>[\d.]+)/sec)",
    re.IGNORECASE,
)


@dataclass
class LoadgenSpec:
    host: str = "127.0.0.1"
    port: int = 11883
    topic: str = "bench/topic"
    qos: int = 0
    clients: int = 32
    interval_ms: int = 1
    payload_size: int = 256
    inflight: int = 100
    mqtt_version: int = 5  # emqtt-bench defaults to v5; v3.1.1 (-V 4) can reject generated client IDs
    duration_s: float = 60.0
    connect_interval_ms: int = 10
    limit: int = 0


def nominal_rate(clients: int, interval_ms: int) -> float:
    """emqtt-bench -I is per-client interval; global rate ≈ clients * 1000 / I."""
    if interval_ms <= 0:
        return float("inf")
    return clients * 1000.0 / interval_ms


def interval_for_rate(clients: int, target_msgs_per_s: float) -> int:
    if clients <= 0 or target_msgs_per_s <= 0:
        return 1000
    return max(1, int(round(clients * 1000.0 / target_msgs_per_s)))


def parse_emqtt_output(text: str) -> dict:
    totals = []
    rates = []
    kinds = []
    for match in RATE_RE.finditer(text or ""):
        kind = match.group("kind") or match.group("kind2")
        total = match.group("total") or match.group("total2")
        rate = match.group("rate") or match.group("rate2")
        if not kind or total is None or rate is None:
            continue
        kind_l = kind.lower()
        if kind_l.startswith("connect"):
            continue
        kinds.append(kind_l)
        totals.append(int(total))
        rates.append(float(rate))
    return {
        "samples": len(rates),
        "kinds": kinds,
        "totals": totals,
        "rates": rates,
        "last_total": totals[-1] if totals else None,
        "last_rate": rates[-1] if rates else None,
        "max_rate": max(rates) if rates else None,
        "median_rate": sorted(rates)[len(rates) // 2] if rates else None,
    }


def build_pub_args(spec: LoadgenSpec) -> List[str]:
    args = [
        "pub",
        "-h",
        spec.host,
        "-p",
        str(spec.port),
        "-V",
        str(spec.mqtt_version),
        "-c",
        str(spec.clients),
        "-i",
        str(spec.connect_interval_ms),
        "-I",
        str(spec.interval_ms),
        "-t",
        spec.topic,
        "-s",
        str(spec.payload_size),
        "-q",
        str(spec.qos),
        "-F",
        str(spec.inflight),
    ]
    if spec.limit > 0:
        args.extend(["-L", str(spec.limit)])
    return args


class EmqttBenchProcess:
    def __init__(self, spec: LoadgenSpec, cpuset: Optional[str] = None):
        self.spec = spec
        self.cpuset = cpuset
        self.proc: Optional[subprocess.Popen] = None
        self.stdout_text = ""
        self.started_at = None
        self.image = EMQTT_BENCH_IMAGE

    def start(self) -> None:
        args = build_pub_args(self.spec)
        cmd = ["docker", "run", "--rm", "--network", "host"]
        if self.cpuset:
            cmd.extend(["--cpuset-cpus", self.cpuset])
        cmd.append(self.image)
        cmd.extend(args)
        self.started_at = time.time()
        self.proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

    def stop(self, timeout_s: float = 10.0) -> dict:
        if self.proc is None:
            return {"emitted": None, "rates": [], "image_digest": image_digest(self.image.split("@")[0])}
        try:
            self.proc.terminate()
            out, _ = self.proc.communicate(timeout=timeout_s)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            out, _ = self.proc.communicate(timeout=5)
        self.stdout_text = out or ""
        parsed = parse_emqtt_output(self.stdout_text)
        return {
            "nominal_rate": nominal_rate(self.spec.clients, self.spec.interval_ms),
            "args": build_pub_args(self.spec),
            "image": self.image,
            "image_digest": image_digest(self.image.split("@")[0]),
            "parsed": parsed,
            "stdout_tail": "\n".join(self.stdout_text.splitlines()[-20:]),
        }

    def wait_duration(self, duration_s: float) -> None:
        if self.proc is None:
            return
        deadline = time.time() + duration_s
        while time.time() < deadline:
            if self.proc.poll() is not None:
                break
            time.sleep(0.2)
