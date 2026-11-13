"""Transit Pulse dashboard: how reliable are Klang Valley buses today?

Reads the gold marts (exported by dbt after every build) and live positions
from silver, straight from the data lake with DuckDB.

    streamlit run dashboard/app.py
"""

from __future__ import annotations

import altair as alt
import pandas as pd
import pydeck as pdk
import streamlit as st

import data
import theme

st.set_page_config(page_title="Transit Pulse", page_icon="🚌", layout="wide")

REFRESH_SECONDS = 300


@st.cache_resource
def _con():
    return data.connect()


@st.cache_data(ttl=REFRESH_SECONDS, show_spinner=False)
def load(name: str, *args):
    return getattr(data, name)(_con(), data.lake_root(), *args)


@st.cache_data(ttl=30, show_spinner=False)
def load_live():
    root = data.lake_root()
    try:
        files = None
        if root.startswith("s3://"):
            now = data.datetime.now(data.UTC).replace(tzinfo=None)
            files = data.recent_silver_files(root, 10, now)
        return data.live_vehicles(_con(), root, files=files)
    except Exception:  # no files yet for today (e.g. just after midnight)
        return pd.DataFrame(columns=["feed", "vehicle_id", "route_id", "latitude", "longitude"])


def fmt(value, pattern: str, missing: str = "–") -> str:
    return missing if value is None or pd.isna(value) else pattern.format(value)


# ------------------------------------------------------------------ header + filter
st.title("🚌 Transit Pulse")
st.caption(
    "How reliable are Klang Valley buses? Live GPS from data.gov.my GTFS-Realtime, "
    "matched to the published timetables. Times are Malaysia time (UTC+8)."
)

try:
    dates = load("service_dates")
except Exception as exc:  # gold not exported yet
    st.warning(f"No warehouse data yet. Run the `transit_dbt_build` DAG first.\n\n{exc}")
    st.stop()

filter_col, refresh_col, _ = st.columns([2, 1, 5])
service_date = filter_col.selectbox(
    "Service day", dates, format_func=lambda d: pd.Timestamp(d).strftime("%a %d %b %Y")
)
refresh_help = f"Data refreshes automatically every {REFRESH_SECONDS // 60} min"
if refresh_col.button("↻ Refresh", help=refresh_help):
    st.cache_data.clear()
    st.rerun()

# ------------------------------------------------------------------ headline numbers
on_time = load("network_on_time", service_date)
headway = load("network_headway", service_date)
live = load_live()
health = load("feed_health")

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric(
    "MRT feeder on time",
    fmt(on_time["pct_on_time"], "{:.0f}%"),
    help="Share of stop arrivals between 1 min early and 5 min late (industry standard).",
)
k2.metric(
    "MRT feeder median delay",
    fmt(on_time["delay_min"], "{:+.1f} min"),
    help="Positive = late, negative = early. Arrival-weighted across the day.",
)
k3.metric(
    "Rapid KL evenly spaced",
    fmt(headway["pct_even"], "{:.0f}%"),
    help="Share of departures between 0.5× and 1.5× the published gap after the previous "
    "bus on the same route (neither bunched nor a gap). Based on actual departure times.",
)
k4.metric(
    "Rapid KL buses bunched",
    fmt(headway["pct_bunched"], "{:.0f}%"),
    help="Departures less than half the scheduled gap after the previous bus on the same route.",
)
k5.metric("Vehicles live now", f"{len(live):,}", help="Reported in the last 10 minutes.")

st.divider()

# ------------------------------------------------------------------ live map + feed health
map_col, health_col = st.columns([3, 2])

with map_col:
    st.subheader("Live vehicles")
    if live.empty:
        st.info("No vehicle positions in the last 10 minutes.")
    else:
        live = live.assign(
            color=live["feed"].map(lambda f: theme.hex_to_rgb(theme.FEED_COLORS.get(f, "#898781"))),
            feed_label=live["feed"].map(lambda f: theme.FEED_LABELS.get(f, f)),
            route=live["route_id"].fillna("–"),
        )
        layer = pdk.Layer(
            "ScatterplotLayer",
            live,
            get_position=["longitude", "latitude"],
            get_fill_color="color",
            get_line_color=theme.hex_to_rgb(theme.SURFACE),  # 2px surface ring between overlaps
            line_width_min_pixels=2,
            stroked=True,
            radius_min_pixels=5,
            radius_max_pixels=9,
            get_radius=80,
            pickable=True,
        )
        st.pydeck_chart(
            pdk.Deck(
                layers=[layer],
                initial_view_state=pdk.ViewState(latitude=3.10, longitude=101.62, zoom=9.6),
                tooltip={"text": "{feed_label}\nVehicle {vehicle_id}\nRoute {route}"},
                map_style="light",
            ),
            height=420,
        )
        # Legend with labels and counts: identity never by color alone.
        counts = live["feed"].value_counts()
        chips = " &nbsp;&nbsp; ".join(
            f'<span style="color:{c};font-size:18px">●</span> '
            f'{theme.FEED_LABELS[f]} <span style="color:{theme.MUTED}">({counts.get(f, 0)})</span>'
            for f, c in theme.FEED_COLORS.items()
        )
        st.markdown(chips, unsafe_allow_html=True)

with health_col:
    st.subheader("Feed health")
    st.caption("Latest complete hour per feed.")
    table = pd.DataFrame(
        {
            "Feed": health["feed"].map(lambda f: theme.FEED_LABELS.get(f, f)),
            "Status": health["status"].map(theme.STATUS),
            "Vehicles": health["vehicles"],
            "Not moving": health["pct_frozen"].map(lambda v: f"{v:.0f}%"),
            "Match timetable": health["pct_trip_matched"].map(lambda v: f"{v:.1f}%"),
        }
    )
    st.dataframe(table, hide_index=True, use_container_width=True)
    if (health["status"] == "frozen").any():
        st.caption(
            "🧊 **Frozen** = most vehicles report but never move. KTMB sends placeholder "
            "positions (the same spot and a constant 90 speed), so it's excluded from "
            "punctuality metrics."
        )

st.divider()

# ------------------------------------------------------------------ delay heatmap
st.subheader("When is the network late?")
st.caption(
    "Median delay of MRT feeder buses by hour (blue = early, gray = on time, red = late). "
    "Hover a cell for details."
)
hourly = load("delay_by_hour", 7)
if hourly.empty:
    st.info("Not enough data yet.")
else:
    hourly = hourly.assign(day=pd.to_datetime(hourly["service_date"]).dt.strftime("%a %d %b"))
    heat = (
        alt.Chart(hourly)
        .mark_rect(cornerRadius=4, stroke=theme.SURFACE, strokeWidth=2)
        .encode(
            x=alt.X("hour_local:O", title="Hour of day", axis=alt.Axis(labelAngle=0)),
            y=alt.Y(
                "day:N", title=None,
                sort=alt.EncodingSortField("service_date", order="descending"),
            ),
            color=alt.Color(
                "median_delay_min:Q",
                title="Median delay (min)",
                # "lab" keeps the early arm blue; Vega's default hcl blend drifts to purple
                scale=alt.Scale(
                    domain=theme.DELAY_DOMAIN, range=theme.DELAY_RANGE,
                    interpolate="lab", clamp=True,
                ),
            ),
            tooltip=[
                alt.Tooltip("day:N", title="Day"),
                alt.Tooltip("hour_local:O", title="Hour"),
                alt.Tooltip("median_delay_min:Q", title="Median delay (min)", format="+.1f"),
                alt.Tooltip("p90_delay_min:Q", title="90th percentile (min)", format="+.1f"),
                alt.Tooltip("pct_on_time:Q", title="On time (%)", format=".0f"),
                alt.Tooltip("observed_arrivals:Q", title="Stop arrivals", format=","),
            ],
        )
        .properties(height=max(90, 34 * hourly["day"].nunique()))
    )
    st.altair_chart(heat, use_container_width=True)
    with st.expander("Table view"):
        st.dataframe(hourly.drop(columns=["day"]), hide_index=True, use_container_width=True)

st.divider()

# ------------------------------------------------------------------ route rankings
left, right = st.columns(2)


def ranked_bars(df: pd.DataFrame, value: str, title: str, color: str, fmt_str: str, domain=None):
    base = alt.Chart(df).encode(
        y=alt.Y(
            "route_name:N", sort=None, title=None,
            axis=alt.Axis(labelOverlap=False, labelLimit=160, labelColor=theme.TEXT_SECONDARY),
        ),
        x=alt.X(
            f"{value}:Q", title=title,
            scale=alt.Scale(domain=domain) if domain else alt.Undefined,
        ),
    )
    bars = base.mark_bar(color=color, cornerRadiusEnd=4, size=14)
    labels = base.mark_text(align="left", dx=4, color=theme.TEXT_SECONDARY, fontSize=11).encode(
        text=alt.Text(f"{value}:Q", format=fmt_str)
    )
    # A fixed step per bar (not a total height) so every route keeps its label.
    return (bars + labels).properties(height=alt.Step(26))


with left:
    st.subheader("Least punctual MRT feeder routes")
    st.caption("On-time share of stop arrivals (routes with at least 20 observed arrivals).")
    routes = load("on_time_routes", service_date)
    if routes.empty:
        st.info("Not enough arrivals yet for this day.")
    else:
        worst = routes.head(10).copy()
        mrt_color = theme.FEED_COLORS["rapid-bus-mrtfeeder"]
        chart = ranked_bars(
            worst, "pct_on_time", "On time (%)", mrt_color, ".0f", [0, 100]
        ).encode(
            tooltip=[
                alt.Tooltip("route_name:N", title="Route"),
                alt.Tooltip("pct_on_time:Q", title="On time (%)", format=".0f"),
                alt.Tooltip("pct_early:Q", title="Early (%)", format=".0f"),
                alt.Tooltip("pct_late:Q", title="Late (%)", format=".0f"),
                alt.Tooltip("median_delay_min:Q", title="Median delay (min)", format="+.1f"),
                alt.Tooltip("observed_arrivals:Q", title="Stop arrivals", format=","),
            ]
        )
        st.altair_chart(chart, use_container_width=True)
        with st.expander(f"All {len(routes)} routes"):
            st.dataframe(routes, hide_index=True, use_container_width=True)

with right:
    st.subheader("Least regular Rapid KL routes")
    st.caption(
        "Share of departures evenly spaced (0.5–1.5× the published gap), from actual "
        "departure times (routes with at least 5 gaps observed). Hover for bunching, gaps "
        "and Excess Wait Time."
    )
    hw = load("headway_routes", service_date)
    if hw.empty:
        st.info("Not enough headways yet for this day.")
    else:
        top = hw.head(10).copy()
        chart = ranked_bars(
            top, "pct_evenly_spaced", "Evenly spaced (%)",
            theme.FEED_COLORS["rapid-bus-kl"], ".0f", [0, 100],
        ).encode(
            tooltip=[
                alt.Tooltip("route_name:N", title="Route"),
                alt.Tooltip("pct_evenly_spaced:Q", title="Evenly spaced (%)", format=".0f"),
                alt.Tooltip("excess_wait_min:Q", title="Excess wait (min)", format="+.1f"),
                alt.Tooltip("avg_wait_min:Q", title="Average wait (min)", format=".1f"),
                alt.Tooltip("scheduled_wait_min:Q", title="Promised wait (min)", format=".1f"),
                alt.Tooltip("pct_bunched:Q", title="Bunched (%)", format=".0f"),
                alt.Tooltip("pct_gaps:Q", title="Gaps (%)", format=".0f"),
                alt.Tooltip("observed_headways:Q", title="Headways", format=","),
            ]
        )
        st.altair_chart(chart, use_container_width=True)
        with st.expander(f"All {len(hw)} routes"):
            st.dataframe(hw, hide_index=True, use_container_width=True)

# ------------------------------------------------------------------ footer
with st.expander("How these numbers are made"):
    st.markdown(
        """
- **Stop arrivals are inferred from GPS.** The feeds only send positions, so each ping is
  matched to its trip's scheduled stops (within 150 m). Loop routes are handled by assigning
  each ping to the stop occurrence closest to its scheduled time.
- **MRT feeder** publishes exact times per stop → *on-time performance* (1 min early to 5 min late).
- **Rapid KL** publishes "a bus every N minutes" → *headway adherence*: how many departures
  are evenly spaced, bunched (< 0.5× the gap) or gaps (> 1.5×), measured between **actual
  departures** the feed reports. A trip that never reports GPS counts as a gap, so Excess
  Wait Time (shown on hover) can overstate waits when buses run with GPS off.
  (Measuring gaps at stops instead overstated them about 2×: with roughly one GPS ping a minute,
  only about a third of stop passes are caught.)
- Data: [data.gov.my](https://developer.data.gov.my/realtime-api/gtfs-realtime) (CC BY 4.0).
  Source and methodology: [github.com/safwan0486/transit-pulse](https://github.com/safwan0486/transit-pulse).
        """
    )
