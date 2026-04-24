
import math
from datetime import date
from typing import List, Dict
from html import escape

import pandas as pd
import altair as alt
import streamlit as st


# ============================================================
# DM Capacity - Streamlit / Snowsight prototype
# ------------------------------------------------------------
# Purpose:
#   Wave 1 project-capacity app using mock data shaped to
#   V_METRICS_WORKITEM_SEMANTIC.
#
# Snowflake swap later:
#   Replace:
#       df = load_mock_semantic_data()
#   with:
#       from snowflake.snowpark.context import get_active_session
#       session = get_active_session()
#       df = session.table("DATABASE.SCHEMA.V_METRICS_WORKITEM_SEMANTIC").to_pandas()
# ============================================================


HOURS_PER_DAY = 7
PROJECT_ALLOCATION_POLICY = 0.70

BRAND_PRIMARY = "#470054"
BRAND_SECONDARY = "#00818A"
BRAND_SURFACE = "#F8FAFC"
BRAND_BORDER = "rgba(71, 0, 84, 0.14)"
APP_NAME = "Capacity Lens"
APP_SUBTITLE = "Project demand vs Data Management capacity"
APP_VERSION = "v0.1 prototype"

REPORTING_MONTHS = pd.DataFrame(
    [
        {"REPORTING_YEAR": 2026, "REPORTING_MONTH": 1, "MONTH_LABEL": "Jan 2026", "WORKING_DAYS": 22},
        {"REPORTING_YEAR": 2026, "REPORTING_MONTH": 2, "MONTH_LABEL": "Feb 2026", "WORKING_DAYS": 20},
        {"REPORTING_YEAR": 2026, "REPORTING_MONTH": 3, "MONTH_LABEL": "Mar 2026", "WORKING_DAYS": 22},
        {"REPORTING_YEAR": 2026, "REPORTING_MONTH": 4, "MONTH_LABEL": "Apr 2026", "WORKING_DAYS": 21},
        {"REPORTING_YEAR": 2026, "REPORTING_MONTH": 5, "MONTH_LABEL": "May 2026", "WORKING_DAYS": 20},
        {"REPORTING_YEAR": 2026, "REPORTING_MONTH": 6, "MONTH_LABEL": "Jun 2026", "WORKING_DAYS": 22},
    ]
)

ADVISORS = [
    "Alex Carter",
    "Mina Patel",
    "Tom Reid",
    "Priya Shah",
    "Daniel Okafor",
    "Sara Lindqvist",
]


def load_mock_semantic_data() -> pd.DataFrame:
    """Mock data shaped to V_METRICS_WORKITEM_SEMANTIC."""

    base_items = [
        # Alex Carter
        {
            "WORKITEM_ID": 200001,
            "TITLE": "Project Orion — Client Data Hub",
            "SERVICE_CODE": "PRJ",
            "SERVICE_NAME": "Project",
            "ASSIGNED_TO": "Alex Carter",
            "STATE": "Active",
            "THEME": "Client Growth",
            "PROGRAM_MANAGER": "Nina Foster",
            "PROJECT_MANAGER": "Luca Bennett",
            "GO_LIVE_TARGET": "2026-09-30",
            "PROJECT_REFERENCE": "PRJ-ORION",
            "ALLOCATION_PERCENTAGE": 0.45,
            "CREATED_DATE": "2026-01-05 09:00:00",
            "DG_CHECKLIST": "In Progress",
            "BDA_STATUS": "In Progress",
            "TDDA_STATUS_WITH_FOLLOW_UP": "In Progress",
            "DATA_OFFICE_RISKS_CONTROLS_PROCEDURES_STATUS": "Not Started",
            "FINAL_SIGN_OFF": "Not Started",
            "SOLIDATUS_IMPACT_FLAG": "Required",
        },
        {
            "WORKITEM_ID": 200002,
            "TITLE": "Client 360 Reporting Uplift",
            "SERVICE_CODE": "PRJ",
            "SERVICE_NAME": "Project",
            "ASSIGNED_TO": "Alex Carter",
            "STATE": "Active",
            "THEME": "Client Growth",
            "PROGRAM_MANAGER": "Nina Foster",
            "PROJECT_MANAGER": "Luca Bennett",
            "GO_LIVE_TARGET": "2026-06-30",
            "PROJECT_REFERENCE": "PRJ-C360",
            "ALLOCATION_PERCENTAGE": 0.30,
            "CREATED_DATE": "2026-01-08 09:00:00",
            "DG_CHECKLIST": "In Progress",
            "BDA_STATUS": "Not Started",
            "TDDA_STATUS_WITH_FOLLOW_UP": "Not Started",
            "DATA_OFFICE_RISKS_CONTROLS_PROCEDURES_STATUS": "Not Started",
            "FINAL_SIGN_OFF": "Not Started",
            "SOLIDATUS_IMPACT_FLAG": "No",
        },
        {
            "WORKITEM_ID": 200003,
            "TITLE": "Onboarding KYC Data Migration",
            "SERVICE_CODE": "PRJ",
            "SERVICE_NAME": "Project",
            "ASSIGNED_TO": "Alex Carter",
            "STATE": "In Review",
            "THEME": "Client Growth",
            "PROGRAM_MANAGER": "Nina Foster",
            "PROJECT_MANAGER": "Ava Romero",
            "GO_LIVE_TARGET": "2026-05-15",
            "PROJECT_REFERENCE": "PRJ-KYC",
            "ALLOCATION_PERCENTAGE": 0.20,
            "CREATED_DATE": "2026-01-12 09:00:00",
            "DG_CHECKLIST": "Complete",
            "BDA_STATUS": "In Progress",
            "TDDA_STATUS_WITH_FOLLOW_UP": "In Progress",
            "DATA_OFFICE_RISKS_CONTROLS_PROCEDURES_STATUS": "In Progress",
            "FINAL_SIGN_OFF": "Not Started",
            "SOLIDATUS_IMPACT_FLAG": "No",
        },
        # Mina Patel
        {
            "WORKITEM_ID": 200004,
            "TITLE": "Regulatory Change 2026 — Best Execution",
            "SERVICE_CODE": "PRJ",
            "SERVICE_NAME": "Project",
            "ASSIGNED_TO": "Mina Patel",
            "STATE": "New",
            "THEME": "Regulatory",
            "PROGRAM_MANAGER": "Iris Nolan",
            "PROJECT_MANAGER": "Ethan Cole",
            "GO_LIVE_TARGET": "2026-12-31",
            "PROJECT_REFERENCE": "PRJ-BEX",
            "ALLOCATION_PERCENTAGE": 0.40,
            "CREATED_DATE": "2026-01-08 09:00:00",
            "DG_CHECKLIST": "Not Started",
            "BDA_STATUS": "Not Started",
            "TDDA_STATUS_WITH_FOLLOW_UP": "Not Started",
            "DATA_OFFICE_RISKS_CONTROLS_PROCEDURES_STATUS": "Not Started",
            "FINAL_SIGN_OFF": "Not Started",
            "SOLIDATUS_IMPACT_FLAG": "TBC",
        },
        {
            "WORKITEM_ID": 200005,
            "TITLE": "MIFID II Reference Data Remediation",
            "SERVICE_CODE": "PRJ",
            "SERVICE_NAME": "Project",
            "ASSIGNED_TO": "Mina Patel",
            "STATE": "Active",
            "THEME": "Regulatory",
            "PROGRAM_MANAGER": "Iris Nolan",
            "PROJECT_MANAGER": "Ethan Cole",
            "GO_LIVE_TARGET": "2026-08-31",
            "PROJECT_REFERENCE": "PRJ-MIFID",
            "ALLOCATION_PERCENTAGE": 0.40,
            "CREATED_DATE": "2026-01-15 09:00:00",
            "DG_CHECKLIST": "In Progress",
            "BDA_STATUS": "Not Started",
            "TDDA_STATUS_WITH_FOLLOW_UP": "Not Started",
            "DATA_OFFICE_RISKS_CONTROLS_PROCEDURES_STATUS": "Not Started",
            "FINAL_SIGN_OFF": "Not Started",
            "SOLIDATUS_IMPACT_FLAG": "No",
        },
        {
            "WORKITEM_ID": 200006,
            "TITLE": "EMIR Refit Reporting Data Quality",
            "SERVICE_CODE": "PRJ",
            "SERVICE_NAME": "Project",
            "ASSIGNED_TO": "Mina Patel",
            "STATE": "Blocked",
            "THEME": "Regulatory",
            "PROGRAM_MANAGER": "Iris Nolan",
            "PROJECT_MANAGER": "Ethan Cole",
            "GO_LIVE_TARGET": "2026-04-30",
            "PROJECT_REFERENCE": "PRJ-EMIR",
            "ALLOCATION_PERCENTAGE": 0.30,
            "CREATED_DATE": "2026-01-20 09:00:00",
            "DG_CHECKLIST": "In Progress",
            "BDA_STATUS": "Blocked",
            "TDDA_STATUS_WITH_FOLLOW_UP": "Not Started",
            "DATA_OFFICE_RISKS_CONTROLS_PROCEDURES_STATUS": "Not Started",
            "FINAL_SIGN_OFF": "Not Started",
            "SOLIDATUS_IMPACT_FLAG": "No",
        },
        # Tom Reid
        {
            "WORKITEM_ID": 200007,
            "TITLE": "Pricing Engine Data Lineage",
            "SERVICE_CODE": "PRJ",
            "SERVICE_NAME": "Project",
            "ASSIGNED_TO": "Tom Reid",
            "STATE": "Active",
            "THEME": "Operational Resilience",
            "PROGRAM_MANAGER": "Olivia Ward",
            "PROJECT_MANAGER": "Marc Devlin",
            "GO_LIVE_TARGET": "2026-07-31",
            "PROJECT_REFERENCE": "PRJ-PRICE",
            "ALLOCATION_PERCENTAGE": 0.40,
            "CREATED_DATE": "2026-01-03 09:00:00",
            "DG_CHECKLIST": "Complete",
            "BDA_STATUS": "Complete",
            "TDDA_STATUS_WITH_FOLLOW_UP": "In Progress",
            "DATA_OFFICE_RISKS_CONTROLS_PROCEDURES_STATUS": "In Progress",
            "FINAL_SIGN_OFF": "Not Started",
            "SOLIDATUS_IMPACT_FLAG": "Required",
        },
        {
            "WORKITEM_ID": 200008,
            "TITLE": "Trade Capture Reconciliation Uplift",
            "SERVICE_CODE": "PRJ",
            "SERVICE_NAME": "Project",
            "ASSIGNED_TO": "Tom Reid",
            "STATE": "Active",
            "THEME": "Operational Resilience",
            "PROGRAM_MANAGER": "Olivia Ward",
            "PROJECT_MANAGER": "Marc Devlin",
            "GO_LIVE_TARGET": "2026-06-15",
            "PROJECT_REFERENCE": "PRJ-RECON",
            "ALLOCATION_PERCENTAGE": 0.30,
            "CREATED_DATE": "2026-01-19 09:00:00",
            "DG_CHECKLIST": "In Progress",
            "BDA_STATUS": "In Progress",
            "TDDA_STATUS_WITH_FOLLOW_UP": "Not Started",
            "DATA_OFFICE_RISKS_CONTROLS_PROCEDURES_STATUS": "Not Started",
            "FINAL_SIGN_OFF": "Not Started",
            "SOLIDATUS_IMPACT_FLAG": "No",
        },
        # Priya Shah
        {
            "WORKITEM_ID": 200009,
            "TITLE": "ESG Data Sourcing Platform",
            "SERVICE_CODE": "PRJ",
            "SERVICE_NAME": "Project",
            "ASSIGNED_TO": "Priya Shah",
            "STATE": "Active",
            "THEME": "ESG",
            "PROGRAM_MANAGER": "Nina Foster",
            "PROJECT_MANAGER": "Ava Romero",
            "GO_LIVE_TARGET": "2026-11-30",
            "PROJECT_REFERENCE": "PRJ-ESG",
            "ALLOCATION_PERCENTAGE": 0.50,
            "CREATED_DATE": "2026-01-06 09:00:00",
            "DG_CHECKLIST": "In Progress",
            "BDA_STATUS": "Not Started",
            "TDDA_STATUS_WITH_FOLLOW_UP": "Not Started",
            "DATA_OFFICE_RISKS_CONTROLS_PROCEDURES_STATUS": "Not Started",
            "FINAL_SIGN_OFF": "Not Started",
            "SOLIDATUS_IMPACT_FLAG": "Required",
        },
        {
            "WORKITEM_ID": 200010,
            "TITLE": "Carbon Metrics Disclosure Pipeline",
            "SERVICE_CODE": "PRJ",
            "SERVICE_NAME": "Project",
            "ASSIGNED_TO": "Priya Shah",
            "STATE": "New",
            "THEME": "ESG",
            "PROGRAM_MANAGER": "Nina Foster",
            "PROJECT_MANAGER": "Ava Romero",
            "GO_LIVE_TARGET": "2026-10-31",
            "PROJECT_REFERENCE": "PRJ-CARB",
            "ALLOCATION_PERCENTAGE": 0.35,
            "CREATED_DATE": "2026-01-22 09:00:00",
            "DG_CHECKLIST": "Not Started",
            "BDA_STATUS": "Not Started",
            "TDDA_STATUS_WITH_FOLLOW_UP": "Not Started",
            "DATA_OFFICE_RISKS_CONTROLS_PROCEDURES_STATUS": "Not Started",
            "FINAL_SIGN_OFF": "Not Started",
            "SOLIDATUS_IMPACT_FLAG": "No",
        },
        # Daniel Okafor
        {
            "WORKITEM_ID": 200011,
            "TITLE": "Risk Aggregation Data Mart",
            "SERVICE_CODE": "PRJ",
            "SERVICE_NAME": "Project",
            "ASSIGNED_TO": "Daniel Okafor",
            "STATE": "Active",
            "THEME": "Risk",
            "PROGRAM_MANAGER": "Iris Nolan",
            "PROJECT_MANAGER": "Marc Devlin",
            "GO_LIVE_TARGET": "2026-09-15",
            "PROJECT_REFERENCE": "PRJ-RISK",
            "ALLOCATION_PERCENTAGE": 0.40,
            "CREATED_DATE": "2026-01-09 09:00:00",
            "DG_CHECKLIST": "In Progress",
            "BDA_STATUS": "In Progress",
            "TDDA_STATUS_WITH_FOLLOW_UP": "In Progress",
            "DATA_OFFICE_RISKS_CONTROLS_PROCEDURES_STATUS": "In Progress",
            "FINAL_SIGN_OFF": "Not Started",
            "SOLIDATUS_IMPACT_FLAG": "No",
        },
        {
            "WORKITEM_ID": 200012,
            "TITLE": "Counterparty Hierarchy Cleanup",
            "SERVICE_CODE": "PRJ",
            "SERVICE_NAME": "Project",
            "ASSIGNED_TO": "Daniel Okafor",
            "STATE": "In Review",
            "THEME": "Risk",
            "PROGRAM_MANAGER": "Iris Nolan",
            "PROJECT_MANAGER": "Marc Devlin",
            "GO_LIVE_TARGET": "2026-05-30",
            "PROJECT_REFERENCE": "PRJ-CPTY",
            "ALLOCATION_PERCENTAGE": 0.25,
            "CREATED_DATE": "2026-01-14 09:00:00",
            "DG_CHECKLIST": "Complete",
            "BDA_STATUS": "Complete",
            "TDDA_STATUS_WITH_FOLLOW_UP": "Complete",
            "DATA_OFFICE_RISKS_CONTROLS_PROCEDURES_STATUS": "Complete",
            "FINAL_SIGN_OFF": "Pending",
            "SOLIDATUS_IMPACT_FLAG": "No",
        },
        # Sara Lindqvist
        {
            "WORKITEM_ID": 200013,
            "TITLE": "Data Catalogue Federation Phase 2",
            "SERVICE_CODE": "PRJ",
            "SERVICE_NAME": "Project",
            "ASSIGNED_TO": "Sara Lindqvist",
            "STATE": "Active",
            "THEME": "Data Foundations",
            "PROGRAM_MANAGER": "Olivia Ward",
            "PROJECT_MANAGER": "Luca Bennett",
            "GO_LIVE_TARGET": "2026-08-31",
            "PROJECT_REFERENCE": "PRJ-CAT2",
            "ALLOCATION_PERCENTAGE": 0.45,
            "CREATED_DATE": "2026-01-04 09:00:00",
            "DG_CHECKLIST": "Complete",
            "BDA_STATUS": "In Progress",
            "TDDA_STATUS_WITH_FOLLOW_UP": "In Progress",
            "DATA_OFFICE_RISKS_CONTROLS_PROCEDURES_STATUS": "In Progress",
            "FINAL_SIGN_OFF": "Not Started",
            "SOLIDATUS_IMPACT_FLAG": "Required",
        },
        {
            "WORKITEM_ID": 200014,
            "TITLE": "Reference Data Golden Source Migration",
            "SERVICE_CODE": "PRJ",
            "SERVICE_NAME": "Project",
            "ASSIGNED_TO": "Sara Lindqvist",
            "STATE": "Active",
            "THEME": "Data Foundations",
            "PROGRAM_MANAGER": "Olivia Ward",
            "PROJECT_MANAGER": "Luca Bennett",
            "GO_LIVE_TARGET": "2026-07-15",
            "PROJECT_REFERENCE": "PRJ-REF",
            "ALLOCATION_PERCENTAGE": 0.30,
            "CREATED_DATE": "2026-01-11 09:00:00",
            "DG_CHECKLIST": "In Progress",
            "BDA_STATUS": "Not Started",
            "TDDA_STATUS_WITH_FOLLOW_UP": "Not Started",
            "DATA_OFFICE_RISKS_CONTROLS_PROCEDURES_STATUS": "Not Started",
            "FINAL_SIGN_OFF": "Not Started",
            "SOLIDATUS_IMPACT_FLAG": "No",
        },
        {
            "WORKITEM_ID": 200015,
            "TITLE": "Lineage Tooling Rollout",
            "SERVICE_CODE": "PRJ",
            "SERVICE_NAME": "Project",
            "ASSIGNED_TO": "Sara Lindqvist",
            "STATE": "New",
            "THEME": "Data Foundations",
            "PROGRAM_MANAGER": "Olivia Ward",
            "PROJECT_MANAGER": "Ethan Cole",
            "GO_LIVE_TARGET": "2026-12-15",
            "PROJECT_REFERENCE": "PRJ-LIN",
            "ALLOCATION_PERCENTAGE": 0.15,
            "CREATED_DATE": "2026-01-25 09:00:00",
            "DG_CHECKLIST": "Not Started",
            "BDA_STATUS": "Not Started",
            "TDDA_STATUS_WITH_FOLLOW_UP": "Not Started",
            "DATA_OFFICE_RISKS_CONTROLS_PROCEDURES_STATUS": "Not Started",
            "FINAL_SIGN_OFF": "Not Started",
            "SOLIDATUS_IMPACT_FLAG": "No",
        },
    ]

    rows: List[Dict] = []
    for item in base_items:
        go_live = pd.to_datetime(item["GO_LIVE_TARGET"])
        for _, m in REPORTING_MONTHS.iterrows():
            month_end = pd.Timestamp(int(m["REPORTING_YEAR"]), int(m["REPORTING_MONTH"]), 1) + pd.offsets.MonthEnd(0)
            is_past_go_live = month_end > go_live

            # deterministic month ripple to make the board feel realistic
            ripple = 1 + (((item["WORKITEM_ID"] + int(m["REPORTING_MONTH"])) % 5) * 0.04) - 0.08
            base_hours = m["WORKING_DAYS"] * HOURS_PER_DAY * item["ALLOCATION_PERCENTAGE"]
            allocated_hours = 0 if is_past_go_live else int(round(base_hours * ripple, 0))

            row = dict(item)
            row["BATCH_YEAR"] = 2026
            row["BATCH_MONTH"] = 1
            row["REPORTING_YEAR"] = int(m["REPORTING_YEAR"])
            row["REPORTING_MONTH"] = int(m["REPORTING_MONTH"])
            row["MONTH_LABEL"] = m["MONTH_LABEL"]
            row["WORKING_DAYS"] = int(m["WORKING_DAYS"])
            row["ALLOCATED_EFFORT_HOURS_THIS_MONTH"] = allocated_hours
            rows.append(row)

    df = pd.DataFrame(rows)
    df["GO_LIVE_TARGET"] = pd.to_datetime(df["GO_LIVE_TARGET"]).dt.date
    df["CREATED_DATE"] = pd.to_datetime(df["CREATED_DATE"])
    return df


def shorten(text: str, max_len: int = 30) -> str:
    if len(text) <= max_len:
        return text
    return text[: max_len - 1] + "…"


def capacity_hours_for_month(working_days: int) -> float:
    return working_days * HOURS_PER_DAY * PROJECT_ALLOCATION_POLICY


def add_project_key(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["PROJECT_KEY"] = df["PROJECT_REFERENCE"].astype(str) + " | " + df["TITLE"].astype(str)
    return df


def filter_df(df: pd.DataFrame) -> pd.DataFrame:
    st.sidebar.header("Filters")

    themes = sorted(df["THEME"].dropna().unique())
    programme_managers = sorted(df["PROGRAM_MANAGER"].dropna().unique())
    project_managers = sorted(df["PROJECT_MANAGER"].dropna().unique())
    dm_leads = sorted(df["ASSIGNED_TO"].dropna().unique())
    states = sorted(df["STATE"].dropna().unique())
    months = REPORTING_MONTHS["MONTH_LABEL"].tolist()

    selected_month = st.sidebar.selectbox("Month", months, index=0)

    selected_themes = st.sidebar.multiselect("Theme", themes)
    selected_prog_mgrs = st.sidebar.multiselect("Programme Manager", programme_managers)
    selected_pms = st.sidebar.multiselect("Project Manager", project_managers)
    selected_dm_leads = st.sidebar.multiselect("DM Lead", dm_leads)
    selected_states = st.sidebar.multiselect("Status", states)

    filtered = df.copy()
    filtered = filtered[filtered["SERVICE_CODE"] == "PRJ"]
    filtered = filtered[filtered["MONTH_LABEL"] == selected_month]

    if selected_themes:
        filtered = filtered[filtered["THEME"].isin(selected_themes)]
    if selected_prog_mgrs:
        filtered = filtered[filtered["PROGRAM_MANAGER"].isin(selected_prog_mgrs)]
    if selected_pms:
        filtered = filtered[filtered["PROJECT_MANAGER"].isin(selected_pms)]
    if selected_dm_leads:
        filtered = filtered[filtered["ASSIGNED_TO"].isin(selected_dm_leads)]
    if selected_states:
        filtered = filtered[filtered["STATE"].isin(selected_states)]

    st.sidebar.caption("Service: **Project** only. Wave 2: BAU + Adhoc.")

    return filtered, selected_month


def person_capacity(df_month: pd.DataFrame, selected_month: str) -> pd.DataFrame:
    month_meta = REPORTING_MONTHS[REPORTING_MONTHS["MONTH_LABEL"] == selected_month].iloc[0]
    cap = capacity_hours_for_month(int(month_meta["WORKING_DAYS"]))

    base = pd.DataFrame({"ASSIGNED_TO": ADVISORS})
    alloc = (
        df_month.groupby("ASSIGNED_TO", as_index=False)
        .agg(
            ALLOCATED_HOURS=("ALLOCATED_EFFORT_HOURS_THIS_MONTH", "sum"),
            PROJECT_COUNT=("PROJECT_KEY", "nunique"),
        )
    )

    out = base.merge(alloc, on="ASSIGNED_TO", how="left")
    out["ALLOCATED_HOURS"] = out["ALLOCATED_HOURS"].fillna(0).astype(int)
    out["PROJECT_COUNT"] = out["PROJECT_COUNT"].fillna(0).astype(int)
    out["CAPACITY_HOURS"] = cap
    out["UTILISATION_PCT"] = (out["ALLOCATED_HOURS"] / out["CAPACITY_HOURS"] * 100).round(0)
    out["HEADROOM_HOURS"] = (out["CAPACITY_HOURS"] - out["ALLOCATED_HOURS"]).round(0)

    def status(pct):
        if pct > 100:
            return "Overloaded"
        if pct > 80:
            return "Near capacity"
        return "Available"

    out["CAPACITY_STATUS"] = out["UTILISATION_PCT"].apply(status)
    return out.sort_values("UTILISATION_PCT", ascending=False)


def governance_at_risk_count(df_month: pd.DataFrame) -> int:
    status_cols = [
        "DG_CHECKLIST",
        "BDA_STATUS",
        "TDDA_STATUS_WITH_FOLLOW_UP",
        "DATA_OFFICE_RISKS_CONTROLS_PROCEDURES_STATUS",
        "FINAL_SIGN_OFF",
    ]
    risky = df_month[status_cols].isin(["Not Started", "Blocked"]).any(axis=1)
    return int(df_month.loc[risky, "WORKITEM_ID"].nunique())


def kpis(df_month: pd.DataFrame, selected_month: str) -> Dict:
    pc = person_capacity(df_month, selected_month)
    total_capacity = pc["CAPACITY_HOURS"].sum()
    total_allocated = pc["ALLOCATED_HOURS"].sum()
    util = 0 if total_capacity == 0 else total_allocated / total_capacity * 100

    month_meta = REPORTING_MONTHS[REPORTING_MONTHS["MONTH_LABEL"] == selected_month].iloc[0]
    month_end = pd.Timestamp(int(month_meta["REPORTING_YEAR"]), int(month_meta["REPORTING_MONTH"]), 1) + pd.offsets.MonthEnd(0)
    horizon = month_end + pd.Timedelta(days=60)

    go_live_next_60 = df_month[
        (pd.to_datetime(df_month["GO_LIVE_TARGET"]) >= month_end)
        & (pd.to_datetime(df_month["GO_LIVE_TARGET"]) <= horizon)
    ]["WORKITEM_ID"].nunique()

    return {
        "team_utilisation_pct": util,
        "total_capacity_hours": total_capacity,
        "total_allocated_hours": total_allocated,
        "headroom_hours": max(0, total_capacity - total_allocated),
        "overloaded_people": int((pc["UTILISATION_PCT"] > 100).sum()),
        "total_people": len(pc),
        "active_projects": int(df_month["PROJECT_KEY"].nunique()),
        "go_live_next_60": int(go_live_next_60),
        "governance_at_risk": governance_at_risk_count(df_month),
    }


def draw_kpi_cards(k: Dict):
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Team utilisation", f"{k['team_utilisation_pct']:.0f}%", f"{k['team_utilisation_pct'] - 100:.0f}% vs cap")
    c2.metric("People overloaded", f"{k['overloaded_people']} of {k['total_people']}", ">100% project capacity")
    c3.metric("Active projects", f"{k['active_projects']}", f"{k['go_live_next_60']} go-live in 60d")
    c4.metric("Headroom", f"{k['headroom_hours']:.0f}h", "available project hours")
    c5.metric("Governance at risk", f"{k['governance_at_risk']}", "missing DG / BDA / TDDA / sign-off")


def draw_saturation_bar(df_month: pd.DataFrame, k: Dict, selected_month: str):
    st.subheader(f"Project capacity saturation — {selected_month}")

    util = k["team_utilisation_pct"]
    st.markdown(
        f"### Team is at <span style='color:#d90429'>{util:.0f}%</span> of project capacity",
        unsafe_allow_html=True,
    )

    theme = (
        df_month.groupby("THEME", as_index=False)
        .agg(HOURS=("ALLOCATED_EFFORT_HOURS_THIS_MONTH", "sum"))
        .sort_values("HOURS", ascending=False)
    )
    total_capacity = k["total_capacity_hours"]
    total_allocated = k["total_allocated_hours"]

    # Width normalised against max(total demand, capacity) so overflow is visible.
    denominator = max(total_capacity, total_allocated, 1)
    html_segments = ""
    colors = ["#f97316", "#0d9488", "#164e63", "#fbbf24", "#fb923c", "#6b7280", "#1f2937"]

    for i, row in theme.iterrows():
        width = row["HOURS"] / denominator * 100
        color = colors[i % len(colors)]
        html_segments += (
            f"<div title='{row['THEME']}: {row['HOURS']}h' "
            f"style='width:{width:.2f}%;background:{color};height:32px;'></div>"
        )

    if total_allocated > total_capacity:
        overflow_width = (total_allocated - total_capacity) / denominator * 100
        html_segments += (
            f"<div title='Over capacity: {total_allocated - total_capacity:.0f}h' "
            f"style='width:{overflow_width:.2f}%;background:#ef4444;height:32px;'></div>"
        )

    capacity_line_left = total_capacity / denominator * 100

    st.markdown(
        f"""
        <div style="border:1px solid #e5e7eb;border-radius:8px;padding:16px;background:white;">
            <div style="display:flex;position:relative;width:100%;border-radius:8px;overflow:hidden;background:#f3f4f6;">
                {html_segments}
                <div style="position:absolute;left:{capacity_line_left:.2f}%;top:0;bottom:0;width:2px;background:#111827;"></div>
            </div>
            <div style="margin-top:8px;color:#475569;font-size:13px;">
                {total_allocated:.0f}h allocated / {total_capacity:.0f}h available · Total demand: {util:.0f}%
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    legend = " · ".join([f"{row['THEME']}: {row['HOURS']:.0f}h" for _, row in theme.iterrows()])
    st.caption(legend)


def draw_overview(df_month: pd.DataFrame, selected_month: str):
    k = kpis(df_month, selected_month)
    draw_kpi_cards(k)
    st.divider()

    draw_saturation_bar(df_month, k, selected_month)
    st.divider()

    left, right = st.columns(2)

    pc = person_capacity(df_month, selected_month)

    with left:
        st.subheader("Most loaded people")
        chart_data = pc[["ASSIGNED_TO", "UTILISATION_PCT"]].rename(
            columns={"ASSIGNED_TO": "DM Lead", "UTILISATION_PCT": "Utilisation %"}
        )
        light_bar_chart(
            chart_data,
            x_col="Utilisation %",
            y_col="DM Lead",
            x_title="Utilisation %",
            y_title="DM Lead",
            horizontal=True,
            height=300,
        )
        people_display = (
            pc[["ASSIGNED_TO", "ALLOCATED_HOURS", "CAPACITY_HOURS", "UTILISATION_PCT", "CAPACITY_STATUS"]]
            .rename(
                columns={
                    "ASSIGNED_TO": "DM Lead",
                    "ALLOCATED_HOURS": "Allocated h",
                    "CAPACITY_HOURS": "Capacity h",
                    "UTILISATION_PCT": "Utilisation %",
                    "CAPACITY_STATUS": "Status",
                }
            )
        )
        st.dataframe(
            light_dataframe_style(people_display),
            use_container_width=True,
            hide_index=True,
        )

    with right:
        st.subheader("Demand by theme — share of team project capacity")
        theme = (
            df_month.groupby("THEME", as_index=False)
            .agg(HOURS=("ALLOCATED_EFFORT_HOURS_THIS_MONTH", "sum"))
            .sort_values("HOURS", ascending=False)
        )
        theme["SHARE_OF_CAPACITY_PCT"] = (theme["HOURS"] / max(k["total_capacity_hours"], 1) * 100).round(0)
        theme_chart = theme[["THEME", "SHARE_OF_CAPACITY_PCT"]].rename(
            columns={"THEME": "Theme", "SHARE_OF_CAPACITY_PCT": "% of capacity"}
        )
        light_bar_chart(
            theme_chart,
            x_col="% of capacity",
            y_col="Theme",
            x_title="% of capacity",
            y_title="Theme",
            horizontal=True,
            height=300,
        )
        theme_display = theme.rename(
            columns={
                "THEME": "Theme",
                "HOURS": "Allocated h",
                "SHARE_OF_CAPACITY_PCT": "% of capacity",
            }
        )
        st.dataframe(
            light_dataframe_style(theme_display),
            use_container_width=True,
            hide_index=True,
        )

    st.divider()
    st.subheader("Why we cannot absorb more work")

    top_people = pc[pc["UTILISATION_PCT"] > 100]["ASSIGNED_TO"].tolist()
    top_theme = (
        df_month.groupby("THEME", as_index=False)
        .agg(HOURS=("ALLOCATED_EFFORT_HOURS_THIS_MONTH", "sum"))
        .sort_values("HOURS", ascending=False)
        .head(1)
    )

    bullets = []
    if k["team_utilisation_pct"] > 100:
        bullets.append(
            f"Project demand is **{k['team_utilisation_pct']:.0f}%** of project capacity in **{selected_month}** — the team is over-committed by **{k['team_utilisation_pct'] - 100:.0f}%**."
        )
    else:
        bullets.append(
            f"Project demand is **{k['team_utilisation_pct']:.0f}%** of project capacity in **{selected_month}**."
        )

    if top_people:
        bullets.append(
            f"**{len(top_people)} of {len(ADVISORS)}** DM leads are over their project allocation: {', '.join(top_people[:3])}{'…' if len(top_people) > 3 else ''}."
        )

    if not top_theme.empty:
        th = top_theme.iloc[0]
        share = th["HOURS"] / max(k["total_capacity_hours"], 1) * 100
        bullets.append(
            f"Theme **{th['THEME']}** alone consumes **{share:.0f}%** of available project capacity this month."
        )

    if k["go_live_next_60"] > 0:
        bullets.append(f"**{k['go_live_next_60']}** project(s) go live in the next 60 days.")

    for b in bullets:
        st.markdown(f"- {b}")


def style_capacity_cells(val):
    try:
        pct = float(val)
    except Exception:
        return ""
    if pct > 100:
        return "background-color: #f8b4b4; color: #b91c1c; font-weight: 700;"
    if pct > 80:
        return "background-color: #fde3b0; color: #92400e;"
    if pct > 0:
        return "background-color: #c7f0df; color: #065f46;"
    return ""


def light_dataframe_style(df: pd.DataFrame):
    """Force readable table styling regardless of Streamlit/browser theme."""
    return (
        df.style
        .set_table_styles(
            [
                {"selector": "table", "props": [("background-color", "#FFFFFF"), ("color", "#0F172A")]},
                {"selector": "thead th", "props": [("background-color", "#F1F5F9"), ("color", "#0F172A"), ("font-weight", "700"), ("border-color", "#CBD5E1")]},
                {"selector": "tbody td", "props": [("background-color", "#FFFFFF"), ("color", "#0F172A"), ("border-color", "#E5E7EB")]},
                {"selector": "tbody th", "props": [("background-color", "#FFFFFF"), ("color", "#0F172A"), ("border-color", "#E5E7EB")]},
            ]
        )
    )


def light_bar_chart(
    data: pd.DataFrame,
    x_col: str,
    y_col: str,
    *,
    x_title: str,
    y_title: str,
    horizontal: bool = False,
    height: int = 320,
):
    """Altair chart with explicit light theme, avoiding Streamlit dark-mode inheritance."""
    chart_data = data.copy()

    if horizontal:
        chart = (
            alt.Chart(chart_data)
            .mark_bar(color="#00818A")
            .encode(
                y=alt.Y(f"{y_col}:N", sort="-x", title=y_title, axis=alt.Axis(labelColor="#0F172A", titleColor="#0F172A")),
                x=alt.X(f"{x_col}:Q", title=x_title, axis=alt.Axis(labelColor="#0F172A", titleColor="#0F172A", gridColor="#E5E7EB")),
                tooltip=[alt.Tooltip(f"{y_col}:N"), alt.Tooltip(f"{x_col}:Q", format=".0f")],
            )
        )
    else:
        chart = (
            alt.Chart(chart_data)
            .mark_bar(color="#00818A")
            .encode(
                x=alt.X(f"{x_col}:N", sort="-y", title=x_title, axis=alt.Axis(labelColor="#0F172A", titleColor="#0F172A", labelAngle=-35)),
                y=alt.Y(f"{y_col}:Q", title=y_title, axis=alt.Axis(labelColor="#0F172A", titleColor="#0F172A", gridColor="#E5E7EB")),
                tooltip=[alt.Tooltip(f"{x_col}:N"), alt.Tooltip(f"{y_col}:Q", format=".0f")],
            )
        )

    chart = chart.properties(height=height).configure_view(
        fill="#FFFFFF",
        stroke="#E5E7EB",
    ).configure_axis(
        labelColor="#0F172A",
        titleColor="#0F172A",
        gridColor="#E5E7EB",
        domainColor="#CBD5E1",
        tickColor="#CBD5E1",
    ).configure_legend(
        labelColor="#0F172A",
        titleColor="#0F172A",
    ).configure_title(
        color="#0F172A",
    ).configure(background="#FFFFFF")

    st.altair_chart(chart, use_container_width=True)





def util_class(pct: float) -> str:
    if pct > 100:
        return "bad"
    if pct > 80:
        return "warn"
    if pct > 0:
        return "ok"
    return ""


def render_html_table(headers: List[str], rows: List[List[str]]) -> None:
    header_html = "".join(f"<th>{escape(str(h))}</th>" for h in headers)
    row_html = ""
    for row in rows:
        row_html += "<tr>" + "".join(str(cell) for cell in row) + "</tr>"
    st.markdown(
        f"""
        <div class="table-scroll">
            <table class="capacity-board-table">
                <thead><tr>{header_html}</tr></thead>
                <tbody>{row_html}</tbody>
            </table>
        </div>
        """,
        unsafe_allow_html=True,
    )


def td(value, cls: str = "") -> str:
    class_attr = f' class="{cls}"' if cls else ""
    return f"<td{class_attr}>{escape(str(value))}</td>"


def td_num(value, cls: str = "") -> str:
    full_cls = "num" if not cls else f"num {cls}"
    return td(value, full_cls)



def draw_capacity_board(df_all: pd.DataFrame, df_month: pd.DataFrame, selected_month: str):
    mode = st.radio(
        "Board mode",
        ["By project", "By month (6mo)"],
        horizontal=True,
        label_visibility="collapsed",
    )

    if mode == "By project":
        st.markdown(
            f"""
            <div class="board-heading">Capacity Board — by project ({escape(selected_month)})</div>
            <div class="board-caption">Person × project allocation in hours. Total / Cap uses project allocation capacity.</div>
            """,
            unsafe_allow_html=True,
        )

        matrix = df_month.pivot_table(
            index="ASSIGNED_TO",
            columns="PROJECT_KEY",
            values="ALLOCATED_EFFORT_HOURS_THIS_MONTH",
            aggfunc="sum",
            fill_value=0,
        )

        matrix = matrix.reindex(ADVISORS, fill_value=0)

        pc = person_capacity(df_month, selected_month).set_index("ASSIGNED_TO")
        matrix["Total h"] = matrix.sum(axis=1)
        matrix["Capacity h"] = pc["CAPACITY_HOURS"]
        matrix["Utilisation %"] = (matrix["Total h"] / matrix["Capacity h"] * 100).round(0)

        project_cols = [c for c in matrix.columns if c not in ["Total h", "Capacity h", "Utilisation %"]]
        headers = ["DM Lead"] + [shorten(str(c), 24) for c in project_cols] + ["Total h", "Capacity h", "Utilisation %"]

        rows = []
        for advisor, row in matrix.iterrows():
            util_pct = float(row["Utilisation %"])
            row_cells = [td(advisor)]
            for c in project_cols:
                v = int(row[c])
                row_cells.append(td_num(v if v > 0 else "·", "muted" if v == 0 else ""))
            row_cells.append(td_num(int(row["Total h"]), "total-col"))
            row_cells.append(td_num(f"{row['Capacity h']:.0f}", "total-col"))
            row_cells.append(td_num(f"{util_pct:.0f}%", util_class(util_pct)))
            rows.append(row_cells)

        total_h = matrix["Total h"].sum()
        capacity_h = matrix["Capacity h"].sum()
        util_total = total_h / max(capacity_h, 1) * 100

        total_cells = [td("PROJECT TOTAL", "total-col")]
        for c in project_cols:
            total_cells.append(td_num(int(matrix[c].sum()), "total-col"))
        total_cells.append(td_num(int(total_h), "total-col"))
        total_cells.append(td_num(f"{capacity_h:.0f}", "total-col"))
        total_cells.append(td_num(f"{util_total:.0f}%", util_class(util_total)))
        rows.append(total_cells)

        render_html_table(headers, rows)

    else:
        st.markdown(
            """
            <div class="board-heading">Capacity Board — by month (6mo)</div>
            <div class="board-caption">Person × month utilisation. Cell values are percentages; tooltip-level hours are shown in the detail table below.</div>
            """,
            unsafe_allow_html=True,
        )

        headers = ["DM Lead"] + REPORTING_MONTHS["MONTH_LABEL"].tolist()
        rows = []
        detail_rows = []

        for advisor in ADVISORS:
            row_cells = [td(advisor)]
            for _, m in REPORTING_MONTHS.iterrows():
                month_label = m["MONTH_LABEL"]
                month_df = df_all[
                    (df_all["SERVICE_CODE"] == "PRJ")
                    & (df_all["REPORTING_YEAR"] == int(m["REPORTING_YEAR"]))
                    & (df_all["REPORTING_MONTH"] == int(m["REPORTING_MONTH"]))
                    & (df_all["ASSIGNED_TO"] == advisor)
                ]
                capacity = capacity_hours_for_month(int(m["WORKING_DAYS"]))
                allocated = month_df["ALLOCATED_EFFORT_HOURS_THIS_MONTH"].sum()
                pct = allocated / max(capacity, 1) * 100
                row_cells.append(td_num(f"{pct:.0f}%", util_class(pct)))
                detail_rows.append(
                    {
                        "DM Lead": advisor,
                        "Month": month_label,
                        "Allocated h": int(allocated),
                        "Capacity h": int(capacity),
                        "Utilisation %": round(pct, 0),
                    }
                )
            rows.append(row_cells)

        render_html_table(headers, rows)

        with st.expander("Show monthly hour detail"):
            detail_df = pd.DataFrame(detail_rows)
            st.dataframe(light_dataframe_style(detail_df), use_container_width=True, hide_index=True)

    st.caption("Legend: green ≤ 80% of project capacity · amber 81–100% · red > 100%.")
    st.caption("Capacity basis: working days × 7h × 70% project allocation policy.")


def draw_project_detail(df_month: pd.DataFrame):
    st.subheader("Project detail")
    cols = [
        "PROJECT_REFERENCE",
        "TITLE",
        "THEME",
        "PROGRAM_MANAGER",
        "PROJECT_MANAGER",
        "ASSIGNED_TO",
        "STATE",
        "GO_LIVE_TARGET",
        "ALLOCATION_PERCENTAGE",
        "ALLOCATED_EFFORT_HOURS_THIS_MONTH",
        "DG_CHECKLIST",
        "BDA_STATUS",
        "TDDA_STATUS_WITH_FOLLOW_UP",
        "DATA_OFFICE_RISKS_CONTROLS_PROCEDURES_STATUS",
        "FINAL_SIGN_OFF",
        "SOLIDATUS_IMPACT_FLAG",
    ]
    display = df_month[cols].copy()
    display["ALLOCATION_PERCENTAGE"] = (display["ALLOCATION_PERCENTAGE"] * 100).round(0).astype(int).astype(str) + "%"
    display = display.rename(
        columns={
            "PROJECT_REFERENCE": "Project ref",
            "TITLE": "Title",
            "THEME": "Theme",
            "PROGRAM_MANAGER": "Programme manager",
            "PROJECT_MANAGER": "Project manager",
            "ASSIGNED_TO": "DM Lead",
            "STATE": "Status",
            "GO_LIVE_TARGET": "Go-live target",
            "ALLOCATION_PERCENTAGE": "Allocation %",
            "ALLOCATED_EFFORT_HOURS_THIS_MONTH": "Internal hours",
            "DG_CHECKLIST": "DG checklist",
            "BDA_STATUS": "BDA",
            "TDDA_STATUS_WITH_FOLLOW_UP": "TDDA",
            "DATA_OFFICE_RISKS_CONTROLS_PROCEDURES_STATUS": "Risks / controls / procedures",
            "FINAL_SIGN_OFF": "Final sign-off",
            "SOLIDATUS_IMPACT_FLAG": "Solidatus",
        }
    )
    st.dataframe(light_dataframe_style(display), use_container_width=True, hide_index=True)


def inject_brand_css():
    st.markdown(
        f"""
        <style>
            #MainMenu {{ visibility: hidden; }}
            .stDeployButton {{ display: none; }}
            footer {{ visibility: hidden; }}

            /* Force a light, readable app surface even if Streamlit Cloud is in dark mode */
            html, body, [data-testid="stAppViewContainer"], [data-testid="stApp"] {{
                background: #F8FAFC !important;
                color: #0F172A !important;
            }}

            .block-container {{
                padding-top: 1rem;
                padding-left: 1.25rem;
                padding-right: 1.25rem;
                max-width: none;
            }}

            /* Streamlit Cloud toolbar can overlap aggressive top banners; keep header inside page flow */
            .enterprise-header {{
                background: {BRAND_PRIMARY};
                color: white;
                padding: 1.1rem 1.4rem;
                margin: 0 0 1rem 0;
                border-bottom: 4px solid {BRAND_SECONDARY};
                border-radius: 0 0 12px 12px;
                display: flex;
                align-items: center;
                gap: 1rem;
                box-shadow: 0 2px 8px rgba(15, 23, 42, 0.16);
            }}

            .enterprise-logo {{
                background: white;
                color: {BRAND_PRIMARY};
                padding: 0.42rem 0.62rem;
                border-radius: 10px;
                font-size: 1.15rem;
                line-height: 1;
                font-weight: 800;
                min-width: 2.2rem;
                text-align: center;
            }}

            .enterprise-title {{ flex: 1; min-width: 0; }}
            .enterprise-title h1 {{
                margin: 0;
                font-size: 1.45rem;
                font-weight: 800;
                color: white !important;
            }}
            .enterprise-subtitle {{
                margin: 0.2rem 0 0 0;
                font-size: 0.9rem;
                opacity: 0.92;
                color: white !important;
            }}
            .version-badge {{
                background: {BRAND_SECONDARY};
                color: white;
                padding: 0.28rem 0.75rem;
                border-radius: 999px;
                font-size: 0.76rem;
                font-weight: 700;
                white-space: nowrap;
            }}

            h1, h2, h3, h4, h5, h6, p, span, label, div {{
                color: inherit;
            }}

            h2, h3 {{
                color: #111827 !important;
            }}

            /* KPI cards: the previous version created white cards with near-white text in dark mode */
            div[data-testid="stMetric"] {{
                background: #FFFFFF !important;
                color: #0F172A !important;
                border-left: 4px solid {BRAND_PRIMARY};
                border-radius: 10px;
                padding: 0.85rem 0.95rem;
                box-shadow: 0 1px 4px rgba(15, 23, 42, 0.08);
                border-top: 1px solid #E5E7EB;
                border-right: 1px solid #E5E7EB;
                border-bottom: 1px solid #E5E7EB;
            }}

            div[data-testid="stMetric"] * {{
                color: #0F172A !important;
            }}

            [data-testid="stMetricValue"] {{
                font-size: 1.65rem;
                font-weight: 800;
                color: #111827 !important;
            }}
            [data-testid="stMetricLabel"] {{
                color: #475569 !important;
                font-weight: 650;
            }}
            [data-testid="stMetricDelta"] {{
                font-size: 0.82rem;
                color: #166534 !important;
            }}
            [data-testid="stMetricDelta"] svg {{
                fill: #16A34A !important;
            }}

            .stTabs [data-baseweb="tab-list"] {{
                gap: 6px;
                border-bottom: 1px solid #CBD5E1;
            }}
            .stTabs [data-baseweb="tab"] {{
                height: 42px;
                padding: 0 18px;
                border-radius: 8px 8px 0 0;
                font-weight: 650;
                color: #334155 !important;
                background: #E2E8F0;
            }}
            .stTabs [aria-selected="true"] {{
                background-color: {BRAND_PRIMARY} !important;
                color: white !important;
            }}
            .stTabs [aria-selected="true"] * {{
                color: white !important;
            }}

            section[data-testid="stSidebar"] {{
                background: #F1F5F9 !important;
                color: #0F172A !important;
                border-right: 1px solid #E2E8F0;
            }}
            section[data-testid="stSidebar"] * {{
                color: #0F172A !important;
            }}

            .brand-note {{
                background: #FFFFFF;
                border: 1px solid #E5E7EB;
                border-left: 4px solid {BRAND_SECONDARY};
                padding: 0.9rem 1rem;
                border-radius: 10px;
                margin-bottom: 1rem;
                color: #334155 !important;
                font-size: 0.94rem;
                box-shadow: 0 1px 3px rgba(15, 23, 42, 0.05);
            }}
            .brand-note * {{
                color: #334155 !important;
            }}


            /* Form controls/selects: prevent dark browser/theme inheritance */
            div[data-baseweb="select"] > div,
            div[data-baseweb="input"] > div,
            textarea,
            input {{
                background-color: #FFFFFF !important;
                color: #0F172A !important;
                border-color: #CBD5E1 !important;
            }}
            div[data-baseweb="select"] span,
            div[data-baseweb="select"] div,
            div[data-baseweb="popover"] div,
            div[data-baseweb="menu"] div {{
                color: #0F172A !important;
            }}
            div[data-baseweb="popover"] {{
                background-color: #FFFFFF !important;
            }}
            ul[role="listbox"] {{
                background-color: #FFFFFF !important;
                color: #0F172A !important;
            }}


            /* Main content text defaults */
            [data-testid="stMainBlockContainer"],
            [data-testid="stMainBlockContainer"] p,
            [data-testid="stMainBlockContainer"] span,
            [data-testid="stMainBlockContainer"] label,
            [data-testid="stMainBlockContainer"] div {{
                color: #0F172A !important;
            }}

            [data-testid="stMainBlockContainer"] h1,
            [data-testid="stMainBlockContainer"] h2,
            [data-testid="stMainBlockContainer"] h3,
            [data-testid="stMainBlockContainer"] h4 {{
                color: #0F172A !important;
            }}

            /* Radio controls */
            div[role="radiogroup"] label,
            div[role="radiogroup"] span,
            div[role="radiogroup"] p {{
                color: #0F172A !important;
            }}

            div[role="radiogroup"] label {{
                background: #E2E8F0 !important;
                border-radius: 8px;
                padding: 0.25rem 0.5rem;
            }}

            div[role="radiogroup"] input:checked + div {{
                color: #FFFFFF !important;
            }}

            /* Explicit light HTML board/table styling */
            .capacity-board-table {{
                width: 100%;
                border-collapse: separate;
                border-spacing: 0;
                background: #FFFFFF;
                color: #0F172A;
                border: 1px solid #CBD5E1;
                border-radius: 10px;
                overflow: hidden;
                font-size: 0.86rem;
            }}

            .capacity-board-table th {{
                background: #F1F5F9;
                color: #0F172A !important;
                font-weight: 750;
                text-align: left;
                padding: 0.65rem 0.55rem;
                border-bottom: 1px solid #CBD5E1;
                border-right: 1px solid #E2E8F0;
                white-space: nowrap;
                position: sticky;
                top: 0;
                z-index: 1;
            }}

            .capacity-board-table td {{
                background: #FFFFFF;
                color: #0F172A !important;
                padding: 0.58rem 0.55rem;
                border-bottom: 1px solid #E5E7EB;
                border-right: 1px solid #F1F5F9;
                white-space: nowrap;
            }}

            .capacity-board-table .num {{
                text-align: right;
                font-variant-numeric: tabular-nums;
            }}

            .capacity-board-table .muted {{
                color: #64748B !important;
            }}

            .capacity-board-table .total-col {{
                font-weight: 750;
                background: #F8FAFC;
            }}

            .capacity-board-table .ok {{
                background: #DFF7EA !important;
                color: #065F46 !important;
                font-weight: 700;
            }}

            .capacity-board-table .warn {{
                background: #FDE7B8 !important;
                color: #92400E !important;
                font-weight: 700;
            }}

            .capacity-board-table .bad {{
                background: #F8B4B4 !important;
                color: #991B1B !important;
                font-weight: 800;
            }}

            .table-scroll {{
                overflow-x: auto;
                border-radius: 10px;
                background: #FFFFFF;
                border: 1px solid #E5E7EB;
            }}

            .board-heading {{
                color: #0F172A !important;
                font-size: 1.55rem;
                font-weight: 800;
                margin: 1.1rem 0 0.25rem 0;
            }}

            .board-caption {{
                color: #475569 !important;
                font-size: 0.9rem;
                margin-bottom: 0.75rem;
            }}

            /* Dataframe/chart containers */
            [data-testid="stDataFrame"],
            [data-testid="stTable"],
            [data-testid="stVegaLiteChart"] {{
                background: #FFFFFF !important;
                color: #0F172A !important;
                border-radius: 10px;
            }}

            .enterprise-header,
            .enterprise-header *,
            .version-badge,
            .version-badge * {
                color: #FFFFFF !important;
            }

            .enterprise-footer {{
                background: #FFFFFF;
                color: #475569 !important;
                padding: 0.85rem 1.2rem;
                margin: 2rem 0 0 0;
                border-top: 3px solid {BRAND_SECONDARY};
                display: flex;
                justify-content: space-between;
                align-items: center;
                font-size: 0.82rem;
                border-radius: 10px 10px 0 0;
            }}
            .enterprise-footer * {{
                color: #475569 !important;
            }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_brand_header():
    st.markdown(
        f"""
        <div class="enterprise-header">
            <div class="enterprise-logo">CL</div>
            <div class="enterprise-title">
                <h1>{APP_NAME}</h1>
                <p class="enterprise-subtitle">{APP_SUBTITLE}</p>
            </div>
            <div class="version-badge">{APP_VERSION}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_brand_footer():
    st.markdown(
        f"""
        <div class="enterprise-footer">
            <div><strong>{APP_NAME}</strong> · Data Management planning prototype</div>
            <div>Project-only Wave 1 · Snowflake semantic layer ready</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def main():
    st.set_page_config(
        page_title=APP_NAME,
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    inject_brand_css()
    render_brand_header()

    st.markdown(
        """
        <div class="brand-note">
            <strong>Wave 1 scope:</strong> project work only. This view is designed to show where project demand has consumed available Data Management capacity.
        </div>
        """,
        unsafe_allow_html=True,
    )

    df = add_project_key(load_mock_semantic_data())

    df_month, selected_month = filter_df(df)

    tab1, tab2, tab3 = st.tabs(["Overview", "Capacity Board", "Project Detail"])

    with tab1:
        draw_overview(df_month, selected_month)

    with tab2:
        draw_capacity_board(df, df_month, selected_month)

    with tab3:
        draw_project_detail(df_month)

    render_brand_footer()


if __name__ == "__main__":
    main()
