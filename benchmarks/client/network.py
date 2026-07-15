"""Network profile helpers using Linux netem/tbf when available."""

from __future__ import annotations

import shutil
import subprocess
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class NetworkProfile:
    name: str
    delay: str = "0ms"
    jitter: str = "0ms"
    loss: str = "0%"
    rate: Optional[str] = None
    burst: Optional[str] = None
    limit: Optional[str] = None
    mtu: int = 1500
    description: str = ""


PROFILES = {
    "localhost": NetworkProfile(
        name="localhost",
        description="Loopback reference; no emulation.",
        mtu=65536,
    ),
    "lan": NetworkProfile(
        name="lan",
        delay="1ms",
        jitter="0.2ms",
        loss="0%",
        rate="100mbit",
        burst="32kbit",
        limit="64kb",
        description="Clean LAN approximation (RTT ~2ms).",
    ),
    "wan": NetworkProfile(
        name="wan",
        delay="40ms",
        jitter="5ms",
        loss="0%",
        rate="20mbit",
        burst="16kbit",
        limit="256kb",
        description="WAN approximation (RTT ~80ms).",
    ),
    "edge": NetworkProfile(
        name="edge",
        delay="150ms",
        jitter="30ms",
        loss="1%",
        rate="1mbit",
        burst="4kbit",
        limit="32kb",
        description="Degraded edge link; robustness only, not CPU verdicts.",
    ),
    "wan_cut": NetworkProfile(
        name="wan_cut",
        delay="40ms",
        jitter="5ms",
        loss="0%",
        rate="20mbit",
        burst="16kbit",
        limit="256kb",
        description="WAN profile with a controlled blackhole outage.",
    ),
}


def has_net_admin() -> bool:
    if shutil.which("tc") is None:
        return False
    # Best-effort probe; actual apply may still fail without CAP_NET_ADMIN.
    proc = subprocess.run(["tc", "qdisc", "show"], capture_output=True, text=True)
    return proc.returncode == 0


def apply_profile(profile_name: str, ifname: str = "lo", seed: int = 42) -> dict:
    """Apply netem on an interface. Localhost is a no-op.

    For v1 we support documenting and validating profiles. Full namespace/veth
    topology is optional; when CAP_NET_ADMIN or tc is missing the harness marks
    network scenarios inconclusive rather than silently succeeding.
    """
    profile = PROFILES[profile_name]
    if profile_name == "localhost":
        return {"applied": False, "reason": "localhost_noop", "profile": profile.name}

    if not has_net_admin():
        return {"applied": False, "reason": "missing_tc_or_cap_net_admin", "profile": profile.name}

    # Prefer shaping a dedicated ifb/veth in future; for now attempt on given ifname.
    netem = [
        "tc",
        "qdisc",
        "replace",
        "dev",
        ifname,
        "root",
        "handle",
        "1:",
        "netem",
        "delay",
        profile.delay,
        profile.jitter,
        "distribution",
        "normal",
        "loss",
        profile.loss,
    ]
    proc = subprocess.run(netem, capture_output=True, text=True)
    if proc.returncode != 0:
        return {
            "applied": False,
            "reason": "tc_netem_failed",
            "stderr": proc.stderr.strip(),
            "profile": profile.name,
        }
    result = {"applied": True, "profile": profile.name, "ifname": ifname, "seed": seed}
    if profile.rate:
        tbf = [
            "tc",
            "qdisc",
            "replace",
            "dev",
            ifname,
            "parent",
            "1:1",
            "handle",
            "10:",
            "tbf",
            "rate",
            profile.rate,
            "burst",
            profile.burst or "32kbit",
            "limit",
            profile.limit or "64kb",
        ]
        tbf_proc = subprocess.run(tbf, capture_output=True, text=True)
        result["tbf_ok"] = tbf_proc.returncode == 0
        if tbf_proc.returncode != 0:
            result["tbf_stderr"] = tbf_proc.stderr.strip()
    return result


def clear_profile(ifname: str = "lo") -> dict:
    if not has_net_admin():
        return {"cleared": False, "reason": "missing_tc_or_cap_net_admin"}
    proc = subprocess.run(["tc", "qdisc", "del", "dev", ifname, "root"], capture_output=True, text=True)
    return {"cleared": proc.returncode == 0, "stderr": proc.stderr.strip()}


def blackhole(ifname: str = "lo") -> dict:
    if not has_net_admin():
        return {"applied": False, "reason": "missing_tc_or_cap_net_admin"}
    proc = subprocess.run(
        ["tc", "qdisc", "replace", "dev", ifname, "root", "netem", "loss", "100%"],
        capture_output=True,
        text=True,
    )
    return {"applied": proc.returncode == 0, "stderr": proc.stderr.strip()}


def qdisc_stats(ifname: str = "lo") -> str:
    proc = subprocess.run(["tc", "-s", "qdisc", "show", "dev", ifname], capture_output=True, text=True)
    return proc.stdout if proc.returncode == 0 else proc.stderr
