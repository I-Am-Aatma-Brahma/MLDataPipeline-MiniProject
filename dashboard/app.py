"""Streamlit dashboard: weather insights, data-quality results, and pipeline health, all read from the warehouse."""
import os

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

st.set_page_config(page_title="Weather Pipeline Dashboard", page_icon="🌦️", layout="wide")


@st.cache_resource
def get_engine():
    return create_engine(os.environ["WAREHOUSE_URL"], pool_pre_ping=True)


@st.cache_data(ttl=20)
def run_query(sql: str) -> pd.DataFrame:
    with get_engine().connect() as conn:
        return pd.read_sql(text(sql), conn)


NUMERIC_DAILY = ["avg_temp_c", "min_temp_c", "max_temp_c", "avg_humidity_pct", "total_precip_mm", "max_wind_kmh"]

st.title("🌦️ Weather Data Pipeline: Monitoring & Insights")
st.caption("Open-Meteo API → Airflow → Postgres (silver / gold). Auto-refreshes data every 20 seconds; use the browser refresh for a new view.")

try:
    daily = run_query("SELECT * FROM gold.weather_daily ORDER BY obs_date, city")
    dq = run_query(
        "SELECT run_id, target_date, check_name, severity, status, failed_count, details, checked_at "
        "FROM dq.check_results ORDER BY checked_at DESC, id DESC LIMIT 300"
    )
    runs = run_query(
        "SELECT logged_at, dag_run_id, target_date, stage, rows_processed, status, message "
        "FROM ops.pipeline_run_log ORDER BY logged_at DESC, id DESC LIMIT 100"
    )
    alerts = run_query(
        "SELECT created_at, task_id, run_id, try_number, error_message "
        "FROM ops.alerts ORDER BY created_at DESC LIMIT 50"
    )
    counts = run_query(
        "SELECT (SELECT COUNT(*) FROM silver.weather_hourly) AS silver_rows, "
        "(SELECT COUNT(*) FROM gold.weather_daily) AS gold_rows, "
        "(SELECT COUNT(*) FROM silver.dim_city) AS cities"
    )
except Exception as exc:  # noqa: BLE001
    st.error(f"Could not read from the warehouse: {exc}")
    st.stop()

tab_weather, tab_quality, tab_health = st.tabs(["🌡️ Weather insights", "✅ Data quality", "🛠️ Pipeline health"])

with tab_weather:
    if daily.empty:
        st.info("No data yet. Trigger the `weather_pipeline` DAG in Airflow (http://localhost:8082) and refresh this page.")
    else:
        for col in NUMERIC_DAILY:
            daily[col] = pd.to_numeric(daily[col])
        daily["obs_date"] = pd.to_datetime(daily["obs_date"])

        all_cities = sorted(daily["city"].unique())
        chosen = st.multiselect("Cities", all_cities, default=all_cities)
        view = daily[daily["city"].isin(chosen)]

        if view.empty:
            st.warning("Select at least one city.")
        else:
            latest_date = view["obs_date"].max()
            latest = view[view["obs_date"] == latest_date]
            hottest = latest.loc[latest["max_temp_c"].idxmax()]
            wettest = latest.loc[latest["total_precip_mm"].idxmax()]

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Latest date", latest_date.strftime("%Y-%m-%d"))
            c2.metric("Hottest city", hottest["city"], f"{float(hottest['max_temp_c']):.1f} °C")
            c3.metric("Wettest city", wettest["city"], f"{float(wettest['total_precip_mm']):.1f} mm")
            c4.metric("Days loaded", int(view["obs_date"].nunique()))

            st.subheader("Average temperature per day (°C)")
            st.line_chart(view.pivot(index="obs_date", columns="city", values="avg_temp_c"))

            left, right = st.columns(2)
            with left:
                st.subheader(f"Total precipitation on {latest_date.strftime('%Y-%m-%d')} (mm)")
                st.bar_chart(latest.set_index("city")["total_precip_mm"])
            with right:
                st.subheader("Days per weather category")
                st.bar_chart(view["weather_category"].value_counts())

            st.subheader("Gold table: gold.weather_daily")
            st.dataframe(view.sort_values(["obs_date", "city"], ascending=[False, True]), use_container_width=True)

with tab_quality:
    if dq.empty:
        st.info("No quality-check results yet.")
    else:
        latest_run = dq.iloc[0]["run_id"]
        latest_checks = dq[dq["run_id"] == latest_run].copy()
        latest_checks["result"] = latest_checks["status"].map({"pass": "✅ pass", "fail": "❌ fail"})
        passed = int((latest_checks["status"] == "pass").sum())
        failed = int((latest_checks["status"] == "fail").sum())

        st.subheader("Most recent run")
        st.caption(f"run_id: {latest_run}  |  target date: {latest_checks.iloc[0]['target_date']}")
        m1, m2 = st.columns(2)
        m1.metric("Checks passed", passed)
        m2.metric("Checks failed", failed)
        st.dataframe(
            latest_checks[["result", "check_name", "severity", "failed_count", "details"]],
            use_container_width=True,
        )

        st.subheader("All failed checks (history)")
        history = dq[dq["status"] == "fail"]
        if history.empty:
            st.success("No failed checks recorded.")
        else:
            st.dataframe(history, use_container_width=True)

with tab_health:
    k1, k2, k3 = st.columns(3)
    k1.metric("silver.weather_hourly rows", int(counts.iloc[0]["silver_rows"]))
    k2.metric("gold.weather_daily rows", int(counts.iloc[0]["gold_rows"]))
    k3.metric("Cities", int(counts.iloc[0]["cities"]))

    st.subheader("Recent pipeline stages (ops.pipeline_run_log)")
    if runs.empty:
        st.info("No runs logged yet.")
    else:
        st.dataframe(runs, use_container_width=True)

    st.subheader("Alerts (ops.alerts)")
    if alerts.empty:
        st.success("No alerts. All tasks have succeeded so far.")
    else:
        st.dataframe(alerts, use_container_width=True)
