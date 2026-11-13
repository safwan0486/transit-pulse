"""Registry of GTFS feeds published by data.gov.my (Malaysia open transport API).

Docs: https://developer.data.gov.my/realtime-api/gtfs-static
      https://developer.data.gov.my/realtime-api/gtfs-realtime
"""

from __future__ import annotations

import os
from dataclasses import dataclass

BASE_URL = "https://api.data.gov.my"


@dataclass(frozen=True)
class Feed:
    name: str
    agency: str
    category: str | None = None
    has_realtime: bool = True

    def _query(self) -> str:
        return f"?category={self.category}" if self.category else ""

    @property
    def static_url(self) -> str:
        return f"{BASE_URL}/gtfs-static/{self.agency}/{self._query()}"

    @property
    def realtime_url(self) -> str:
        return f"{BASE_URL}/gtfs-realtime/vehicle-position/{self.agency}/{self._query()}"


FEEDS: dict[str, Feed] = {
    f.name: f
    for f in [
        Feed("rapid-bus-kl", "prasarana", "rapid-bus-kl"),
        Feed("rapid-bus-mrtfeeder", "prasarana", "rapid-bus-mrtfeeder"),
        Feed("rapid-bus-kuantan", "prasarana", "rapid-bus-kuantan"),
        Feed("rapid-bus-penang", "prasarana", "rapid-bus-penang"),
        Feed("rapid-rail-kl", "prasarana", "rapid-rail-kl", has_realtime=False),
        Feed("ktmb", "ktmb"),
    ]
}


def feeds_from_env(var: str, default: str) -> list[Feed]:
    """Resolve a comma-separated env var (e.g. REALTIME_FEEDS) into Feed objects."""
    names = [n.strip() for n in os.getenv(var, default).split(",") if n.strip()]
    unknown = [n for n in names if n not in FEEDS]
    if unknown:
        raise ValueError(f"Unknown feed(s) in {var}: {unknown}. Known: {sorted(FEEDS)}")
    return [FEEDS[n] for n in names]
