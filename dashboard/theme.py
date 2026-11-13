"""Colors and chart styling (reference palette from the dataviz method).

Feed colors are fixed by identity, never by rank, and validated for color
vision deficiency (all-pairs ΔE ≥ 9.2). KTMB's aqua is below 3:1 contrast on
the light surface, so it is always shown with a text label too.
"""

from __future__ import annotations

import altair as alt

SURFACE = "#fcfcfb"
TEXT_PRIMARY = "#0b0b0b"
TEXT_SECONDARY = "#52514e"
MUTED = "#898781"
GRID = "#e1e0d9"
BASELINE = "#c3c2b7"

FEED_COLORS = {  # categorical slots 1-3, fixed order
    "rapid-bus-kl": "#2a78d6",
    "rapid-bus-mrtfeeder": "#eb6834",
    "ktmb": "#1baf7a",
}
FEED_LABELS = {
    "rapid-bus-kl": "Rapid KL bus",
    "rapid-bus-mrtfeeder": "MRT feeder bus",
    "ktmb": "KTMB train",
}

# Diverging delay scale: early (blue) ← on time (neutral gray) → late (red)
DELAY_DOMAIN = [-5, 0, 10]
DELAY_RANGE = ["#1c5cab", "#f0efec", "#e34948"]

STATUS = {  # reserved status roles, always icon + label
    "healthy": "✅ Healthy",
    "degraded": "⚠️ Degraded",
    "frozen": "🧊 Frozen",
}


def hex_to_rgb(hex_color: str) -> list[int]:
    h = hex_color.lstrip("#")
    return [int(h[i : i + 2], 16) for i in (0, 2, 4)]


def altair_theme() -> dict:
    font = 'system-ui, -apple-system, "Segoe UI", sans-serif'
    return {
        "config": {
            "background": SURFACE,
            "font": font,
            "view": {"stroke": None},
            "axis": {
                "labelColor": MUTED,
                "titleColor": TEXT_SECONDARY,
                "titleFontWeight": "normal",
                "gridColor": GRID,
                "gridWidth": 1,
                "domainColor": BASELINE,
                "tickColor": BASELINE,
                "labelFontSize": 11,
                "titleFontSize": 12,
            },
            "legend": {
                "labelColor": TEXT_SECONDARY,
                "titleColor": TEXT_SECONDARY,
                "titleFontWeight": "normal",
            },
            "title": {"color": TEXT_PRIMARY, "fontWeight": 600, "anchor": "start"},
        }
    }


alt.themes.register("transit_pulse", altair_theme)
alt.themes.enable("transit_pulse")
