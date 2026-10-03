"""Vélib' Paris dashboard, built only on the dbt marts (velib_marts.*) and the monitoring alerts.

    streamlit run dashboard/app.py          (WAREHOUSE_DSN defaults to the local warehouse)
"""

from __future__ import annotations

import os

import altair as alt
import pandas as pd
import psycopg
import pydeck as pdk
import streamlit as st

DSN = os.environ.get("WAREHOUSE_DSN", "postgresql://de:de@localhost:5435/warehouse")

st.set_page_config(page_title="Vélib' Paris: live availability", layout="wide")


@st.cache_data(ttl=120)
def query(sql: str) -> pd.DataFrame:
    with psycopg.connect(DSN) as conn:
        cur = conn.execute(sql)
        return pd.DataFrame(cur.fetchall(), columns=[c.name for c in cur.description])


def table_exists(name: str) -> bool:
    return bool(query(f"SELECT to_regclass('{name}') IS NOT NULL AS ok")["ok"][0])


st.title("Vélib' Paris: live bike availability")
if not table_exists("velib_marts.mart_latest_status"):
    st.info("No data yet: the pipeline hasn't built the marts. Start the stack and wait for the first runs.")
    st.stop()

latest = query("SELECT * FROM velib_marts.mart_latest_status")
series = query("SELECT * FROM velib_marts.mart_network_timeseries ORDER BY taken_at")
taken_at = pd.to_datetime(latest["taken_at"].max())
st.caption(f"Latest snapshot: {taken_at.tz_convert('Europe/Paris'):%A %d %B %Y, %H:%M} (Paris time) · "
           f"{len(series)} snapshots collected since {pd.to_datetime(series['taken_at'].min()).tz_convert('Europe/Paris'):%d %b %H:%M}")

operating = latest[latest["is_operating"]]
c1, c2, c3, c4 = st.columns(4)
c1.metric("Bikes available", f"{int(latest['bikes'].sum()):,}")
c2.metric("E-bike share", f"{100 * latest['ebike'].sum() / max(latest['bikes'].sum(), 1):.0f}%")
c3.metric("Empty stations", f"{100 * operating['is_empty'].mean():.1f}%")
c4.metric("Full stations", f"{100 * operating['is_full'].mean():.1f}%")

left, right = st.columns([3, 2])
with left:
    st.subheader("Every station now")
    view = latest.assign(
        color=latest.apply(lambda r: [220, 60, 60, 200] if r["is_empty"] else [42, 120, 214, 200] if r["is_full"]
                           else [27, 175, 122, 200], axis=1),
        radius=25 + latest["capacity"].clip(upper=80))
    st.pydeck_chart(pdk.Deck(
        map_provider="carto", map_style=pdk.map_styles.CARTO_LIGHT,     # free basemap, no API token
        initial_view_state=pdk.ViewState(latitude=48.857, longitude=2.35, zoom=11),
        layers=[pdk.Layer("ScatterplotLayer", view, get_position="[lon, lat]", get_fill_color="color",
                          get_radius="radius", pickable=True)],
        tooltip={"text": "{name}\n{bikes} bikes ({ebike} e-bikes) · {docks} free docks"}))
    st.caption("Red: no bike · Blue: no free dock · Green: both available")
with right:
    st.subheader("The network over time")
    long = series.melt(id_vars="taken_at_paris", value_vars=["empty_station_pct", "full_station_pct"],
                       var_name="metric", value_name="pct")
    long["metric"] = long["metric"].map({"empty_station_pct": "empty stations %", "full_station_pct": "full stations %"})
    st.altair_chart(alt.Chart(long).mark_line().encode(
        x=alt.X("taken_at_paris:T", title="Paris time", axis=alt.Axis(format="%H:%M")), y=alt.Y("pct:Q", title="% of operating stations"),
        color=alt.Color("metric:N", legend=alt.Legend(orient="bottom", title=None))), use_container_width=True)
    st.altair_chart(alt.Chart(series).mark_area(opacity=0.4).encode(
        x=alt.X("taken_at_paris:T", title="Paris time", axis=alt.Axis(format="%H:%M")), y=alt.Y("bikes:Q", title="bikes available")),
        use_container_width=True)

left, right = st.columns(2)
with left:
    st.subheader("Stations most often empty")
    worst = query("""SELECT name, area, snapshots, empty_pct, longest_empty_minutes
                     FROM velib_marts.mart_station_reliability
                     WHERE snapshots >= 3 ORDER BY empty_pct DESC, longest_empty_minutes DESC LIMIT 15""")
    st.dataframe(worst, hide_index=True, use_container_width=True)
with right:
    st.subheader("Empty stations by area and hour")
    hourly = query("SELECT area, area_order, hour, empty_pct FROM velib_marts.mart_area_hourly "
                    "WHERE area <> 'Event stations' ORDER BY area_order")
    st.altair_chart(alt.Chart(hourly).mark_rect().encode(
        x=alt.X("hour:O", title="hour (Paris)"),
        y=alt.Y("area:N", sort=alt.EncodingSortField("area_order", order="ascending"), title=None),
        color=alt.Color("empty_pct:Q", title="% empty",
                        scale=alt.Scale(scheme="blues", domain=[0, max(20, float(hourly["empty_pct"].max()))]))), use_container_width=True)

st.subheader("Pipeline alerts")
if table_exists("monitoring.alerts"):
    alerts = query("SELECT raised_at, severity, source, message FROM monitoring.alerts ORDER BY raised_at DESC LIMIT 20")
    st.dataframe(alerts, hide_index=True, use_container_width=True) if len(alerts) else st.success("No alerts.")
else:
    st.success("No alerts.")
