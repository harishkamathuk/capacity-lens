
import math
from datetime import date
from typing import List, Dict

import pandas as pd
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
        chart_data = pc.set_index("ASSIGNED_TO")[["UTILISATION_PCT"]]
        st.bar_chart(chart_data)
        st.dataframe(
            pc[["ASSIGNED_TO", "ALLOCATED_HOURS", "CAPACITY_HOURS", "UTILISATION_PCT", "CAPACITY_STATUS"]]
            .rename(
                columns={
                    "ASSIGNED_TO": "DM Lead",
                    "ALLOCATED_HOURS": "Allocated h",
                    "CAPACITY_HOURS": "Capacity h",
                    "UTILISATION_PCT": "Utilisation %",
                    "CAPACITY_STATUS": "Status",
                }
            ),
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
        st.bar_chart(theme.set_index("THEME")[["SHARE_OF_CAPACITY_PCT"]])
        st.dataframe(
            theme.rename(
                columns={
                    "THEME": "Theme",
                    "HOURS": "Allocated h",
                    "SHARE_OF_CAPACITY_PCT": "% of capacity",
                }
            ),
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


def draw_capacity_board(df_all: pd.DataFrame, df_month: pd.DataFrame, selected_month: str):
    mode = st.radio(
        "Board mode",
        ["By project", "By month (6mo)"],
        horizontal=True,
        label_visibility="collapsed",
    )

    if mode == "By project":
        st.subheader(f"Capacity Board — by project ({selected_month})")
        st.caption("Person × project allocation in hours. Total / Cap uses project allocation capacity.")

        matrix = df_month.pivot_table(
            index="ASSIGNED_TO",
            columns="PROJECT_KEY",
            values="ALLOCATED_EFFORT_HOURS_THIS_MONTH",
            aggfunc="sum",
            fill_value=0,
        )

        # Ensure every advisor appears.
        matrix = matrix.reindex(ADVISORS, fill_value=0)

        pc = person_capacity(df_month, selected_month).set_index("ASSIGNED_TO")
        matrix["Total h"] = matrix.sum(axis=1)
        matrix["Capacity h"] = pc["CAPACITY_HOURS"]
        matrix["Utilisation %"] = (matrix["Total h"] / matrix["Capacity h"] * 100).round(0)

        # Shorten project column labels.
        rename_cols = {
            col: shorten(str(col), 28)
            for col in matrix.columns
            if col not in ["Total h", "Capacity h", "Utilisation %"]
        }
        display = matrix.rename(columns=rename_cols)

        st.dataframe(
            display.style.applymap(style_capacity_cells, subset=["Utilisation %"]).format(precision=0),
            use_container_width=True,
            height=460,
        )

        totals = pd.DataFrame(
            {
                "Metric": ["Allocated project hours", "Available project hours", "Utilisation %"],
                "Value": [
                    f"{matrix['Total h'].sum():.0f}h",
                    f"{matrix['Capacity h'].sum():.0f}h",
                    f"{matrix['Total h'].sum() / max(matrix['Capacity h'].sum(), 1) * 100:.0f}%",
                ],
            }
        )
        st.dataframe(totals, use_container_width=True, hide_index=True)

    else:
        st.subheader("Capacity Board — by month (6mo)")
        st.caption("Person × month utilisation. Cell values are percentages; hours are shown underneath in the expanded detail.")

        rows = []
        detail_rows = []

        for advisor in ADVISORS:
            row = {"DM Lead": advisor}
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
                row[month_label] = round(pct, 0)
                detail_rows.append(
                    {
                        "DM Lead": advisor,
                        "Month": month_label,
                        "Allocated h": int(allocated),
                        "Capacity h": int(capacity),
                        "Utilisation %": round(pct, 0),
                    }
                )
            rows.append(row)

        month_matrix = pd.DataFrame(rows)
        month_cols = REPORTING_MONTHS["MONTH_LABEL"].tolist()
        st.dataframe(
            month_matrix.style.applymap(style_capacity_cells, subset=month_cols).format(precision=0),
            use_container_width=True,
            height=360,
            hide_index=True,
        )

        with st.expander("Show monthly hour detail"):
            st.dataframe(pd.DataFrame(detail_rows), use_container_width=True, hide_index=True)

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
    st.dataframe(display, use_container_width=True, hide_index=True)


def main():
    st.set_page_config(
        page_title="DM Capacity",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    st.markdown(
        """
        <style>
        .block-container { padding-top: 1.25rem; }
        [data-testid="stMetricValue"] { font-size: 1.7rem; }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.title("DM Capacity")
    st.caption("Wave 1 · Project work · Data Management team · Snowsight-ready Streamlit prototype")

    df = add_project_key(load_mock_semantic_data())

    df_month, selected_month = filter_df(df)

    tab1, tab2, tab3 = st.tabs(["Overview", "Capacity Board", "Project Detail"])

    with tab1:
        draw_overview(df_month, selected_month)

    with tab2:
        draw_capacity_board(df, df_month, selected_month)

    with tab3:
        draw_project_detail(df_month)


if __name__ == "__main__":
    main()
