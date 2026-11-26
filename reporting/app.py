"""Streamlit Reporting Skeleton for the CorpTech Enterprise Lakehouse.

Students must build out the 4 required business & DQ views in this module
(or split into sub-modules) reading strictly from Gold tables and
`dq_audit_results` via `reporting.db`.
"""

from __future__ import annotations

import pandas as pd
import streamlit as st

from reporting.db import (
    get_lakehouse_root,
    get_table_row_count,
    read_delta_table,
    table_exists,
)

EXPECTED_TABLES: list[tuple[str, str, str]] = [
    ("Reference Silver", "silver/silver_clients", "Reference starter table"),
    ("DQ Audit", "dq/dq_audit_results", "Pipeline data quality audit log"),
    ("Gold", "gold/gold_project_profitability", "Project revenue, labor cost & margin"),
    ("Gold", "gold/gold_department_finance", "Department expenses & utilization"),
    ("Gold", "gold/gold_client_engagement", "CRM events, API latency & ARR delta"),
    ("Gold", "gold/gold_hr_enps_analytics", "eNPS, Q02-Q10 drivers & attrition"),
    ("Gold", "gold/gold_employee_churn_features", "Point-in-time ML churn features"),
]


def render_overview_tab() -> None:
    """Render the Lakehouse status overview home page."""
    st.subheader("Lakehouse Table Availability")
    st.caption(f"Reading Delta Lake tables from `{get_lakehouse_root().resolve()}`")

    rows: list[dict[str, object]] = []
    for layer, rel_path, description in EXPECTED_TABLES:
        exists = table_exists(rel_path)
        row_count = get_table_row_count(rel_path) if exists else None
        rows.append(
            {
                "Layer": layer,
                "Table Path": rel_path,
                "Status": "Ready" if exists else "Not built yet",
                "Row Count": row_count if row_count is not None else 0,
                "Purpose": description,
            }
        )

    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    st.info(
        "Next steps: Run the reference `clients` pipeline (`ingest_clients.py` -> "
        "`transform_clients.py`) to verify `silver/silver_clients` and "
        "`dq/dq_audit_results` appear as **Ready** above, then implement the "
        "remaining Bronze, Silver, Gold, and Reporting views."
    )


def main() -> None:
    st.set_page_config(
        page_title="CorpTech Lakehouse Analytics",
        page_icon=":bar_chart:",
        layout="wide",
    )

    st.title("CorpTech Consulting — Enterprise Lakehouse")
    st.markdown(
        "Starter Reporting Layer powered by **Streamlit + DuckDB (`delta_scan`)**. "
        "Build out the 4 required views below using your Gold & DQ Delta tables."
    )

    with st.sidebar:
        st.header("Lakehouse Status")
        st.code(str(get_lakehouse_root()), language="text")
        if st.button("Refresh Status"):
            st.rerun()
        st.divider()
        for _, rel_path, _ in EXPECTED_TABLES:
            icon = "Ready" if table_exists(rel_path) else "Missing"
            st.text(f"[{icon}] {rel_path}")

    tab_home, tab_finance, tab_hr, tab_crm, tab_dq = st.tabs(
        [
            "Home / Overview",
            "1. Finance & Profitability",
            "2. HR, Attrition & eNPS",
            "3. CRM Engagement",
            "4. Pipeline & DQ Health",
        ]
    )

    with tab_home:
        render_overview_tab()

    with tab_finance:
        st.subheader("Finance & Project Profitability (TODO)")
        st.warning(
            "TODO: Implement this view reading from `gold/gold_project_profitability` "
            "and `gold/gold_department_finance`.\n\n"
            "Required KPIs (see `docs/STUDENT_ASSIGNMENT.md` Section 4.5):\n"
            "- Net revenue vs. credit notes (EUR)\n"
            "- Point-in-time labor cost (EUR)\n"
            "- Top/bottom gross margin projects & budget utilization\n"
            "- Departmental expense breakdown"
        )

    with tab_hr:
        st.subheader("HR, Attrition & eNPS Pulse (TODO)")
        st.warning(
            "TODO: Implement this view reading from `gold/gold_hr_enps_analytics` "
            "(and optional comment insights).\n\n"
            "Required KPIs (see `docs/STUDENT_ASSIGNMENT.md` Section 4.5):\n"
            "- Company & departmental eNPS score\n"
            "- 10-question driver breakdown (`Q02`–`Q10`)\n"
            "- Attrition breakdown by department, `work_mode`, and `compa_ratio` bucket\n"
            "- Filterable written employee comments"
        )

    with tab_crm:
        st.subheader("CRM Client Engagement (TODO)")
        st.warning(
            "TODO: Implement this view reading from `gold/gold_client_engagement`.\n\n"
            "Required KPIs (see `docs/STUDENT_ASSIGNMENT.md` Section 4.5):\n"
            "- Event volume by client tier and `event_type`\n"
            "- `VIP_CORP_001` traffic share\n"
            "- Average API latency & critical support ticket trends"
        )

    with tab_dq:
        st.subheader("Pipeline & Data Quality Health (TODO)")
        st.warning(
            "TODO: Build interactive DQ health charts, daily ingested vs. quarantined "
            "metrics, and rule/table failure breakdowns from `dq/dq_audit_results` "
            "and `silver_quarantine/*`."
        )
        audit_df = read_delta_table("dq/dq_audit_results", limit=200)
        if audit_df is not None:
            st.caption("Preview of `dq/dq_audit_results` (DuckDB `delta_scan` connectivity check):")
            st.dataframe(audit_df, use_container_width=True, hide_index=True)
        else:
            st.info("No `dq/dq_audit_results` table found yet.")


if __name__ == "__main__":
    main()
