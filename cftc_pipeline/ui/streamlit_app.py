"""Streamlit analyst interface for CFTC comment pipeline.

Run with:
    streamlit run cftc_pipeline/ui/streamlit_app.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

# Ensure project root is on path when run directly
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import streamlit as st
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from cftc_pipeline.db.session import SessionLocal
from cftc_pipeline.db.models import (
    ClusterMembership,
    DedupeGroup,
    Docket,
    ExtractionResult,
    LLMAnalysis,
    PipelineJob,
    ReportRun,
    Submission,
    SubmissionDedupe,
    ThemeCluster,
)

st.set_page_config(
    page_title="CFTC Comment Analyst",
    page_icon="⚖️",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ---------------------------------------------------------------------------
# Database helper
# ---------------------------------------------------------------------------


def _create_db_session() -> Session:
    session = SessionLocal()
    session.execute(text("SELECT 1"))
    return session


def db() -> Session:
    session = st.session_state.get("db_session")
    if session is None:
        session = _create_db_session()
        st.session_state["db_session"] = session
        return session

    try:
        session.execute(text("SELECT 1"))
    except OperationalError:
        session.close()
        session = _create_db_session()
        st.session_state["db_session"] = session

    return session


# ---------------------------------------------------------------------------
# Sidebar navigation
# ---------------------------------------------------------------------------


def sidebar():
    st.sidebar.title("⚖️ CFTC Comment Analyst")

    dockets = db().query(Docket).all()
    if not dockets:
        st.sidebar.warning("No dockets found. Run `cftc init-docket` first.")
        return None

    docket_options = {f"{d.docket_id}: {d.title or 'Untitled'}": d.id for d in dockets}
    selected = st.sidebar.selectbox("Select Docket", list(docket_options.keys()))
    docket_id = docket_options[selected]

    page = st.sidebar.radio(
        "View",
        [
            "Dashboard",
            "Submission List",
            "Submission Detail",
            "Dedupe Families",
            "Theme Clusters",
            "Report",
            "Pipeline Status",
        ],
    )
    return docket_id, page


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------


def page_dashboard(docket_id: int):
    docket = db().get(Docket, docket_id)
    st.title(f"Dashboard — {docket.docket_id}")
    if docket.title:
        st.subheader(docket.title)

    col1, col2, col3, col4 = st.columns(4)

    total = db().query(Submission).filter(Submission.docket_id == docket_id).count()
    analyzed = (
        db()
        .query(LLMAnalysis)
        .join(Submission)
        .filter(Submission.docket_id == docket_id)
        .count()
    )
    unique = (
        db()
        .query(SubmissionDedupe)
        .join(Submission, SubmissionDedupe.submission_id == Submission.id)
        .filter(Submission.docket_id == docket_id, SubmissionDedupe.is_canonical == True)
        .count()
    )
    clusters = db().query(ThemeCluster).filter(ThemeCluster.docket_id == docket_id).count()

    col1.metric("Total Submissions", total)
    col2.metric("Unique Submissions", unique or total)
    col3.metric("LLM Analyzed", analyzed)
    col4.metric("Theme Clusters", clusters)

    # Stance breakdown
    analyses = (
        db()
        .query(LLMAnalysis)
        .join(Submission)
        .filter(Submission.docket_id == docket_id)
        .all()
    )
    if analyses:
        from collections import Counter

        stance_counts = Counter(
            a.stance.value for a in analyses if a.stance
        )
        st.subheader("Stance Breakdown")
        stance_df = pd.DataFrame(
            [{"Stance": k, "Count": v} for k, v in stance_counts.most_common()]
        )
        st.bar_chart(stance_df.set_index("Stance"))

        # Commenter type breakdown
        type_counts = Counter(
            a.commenter_type.value for a in analyses if a.commenter_type
        )
        st.subheader("Commenter Types")
        type_df = pd.DataFrame(
            [{"Type": k, "Count": v} for k, v in type_counts.most_common()]
        )
        st.bar_chart(type_df.set_index("Type"))


# ---------------------------------------------------------------------------
# Submission List
# ---------------------------------------------------------------------------


def page_submission_list(docket_id: int):
    st.title("Submission List")

    # Filters
    with st.expander("Filters", expanded=True):
        col1, col2, col3, col4 = st.columns(4)
        stance_filter = col1.multiselect(
            "Stance",
            ["support", "oppose", "mixed", "neutral", "unclear"],
            default=[],
        )
        type_filter = col2.multiselect(
            "Commenter Type",
            ["individual", "company", "trade_association", "nonprofit", "academic", "government", "other"],
            default=[],
        )
        canonical_only = col3.checkbox("Unique submissions only", value=False)
        search_term = col4.text_input("Search (commenter name / org)")

    # Query
    query = (
        db()
        .query(Submission, LLMAnalysis, SubmissionDedupe)
        .outerjoin(LLMAnalysis, LLMAnalysis.submission_id == Submission.id)
        .outerjoin(SubmissionDedupe, SubmissionDedupe.submission_id == Submission.id)
        .filter(Submission.docket_id == docket_id)
    )

    if stance_filter:
        from sqlalchemy import cast, Enum as SAEnum
        query = query.filter(
            LLMAnalysis.stance.in_(stance_filter)
        )
    if type_filter:
        query = query.filter(LLMAnalysis.commenter_type.in_(type_filter))
    if canonical_only:
        query = query.filter(SubmissionDedupe.is_canonical == True)
    if search_term:
        query = query.filter(
            (Submission.commenter_name.ilike(f"%{search_term}%"))
            | (Submission.organization.ilike(f"%{search_term}%"))
        )

    rows = query.limit(500).all()

    data = []
    for sub, analysis, dedup in rows:
        data.append(
            {
                "ID": sub.id,
                "External ID": sub.external_id,
                "Commenter": sub.commenter_name or "",
                "Organization": sub.organization or (analysis.organization_extracted if analysis else ""),
                "Date": str(sub.submission_date.date() if sub.submission_date else ""),
                "Stance": analysis.stance.value if analysis and analysis.stance else "",
                "Type": analysis.commenter_type.value if analysis and analysis.commenter_type else "",
                "Score": round(analysis.substantive_score or 0, 3) if analysis else 0,
                "Template": round(analysis.template_likelihood or 0, 2) if analysis else 0,
                "Canonical": dedup.is_canonical if dedup else True,
                "Summary": (analysis.summary_short or "")[:120] if analysis else "",
            }
        )

    df = pd.DataFrame(data)
    if not df.empty:
        st.dataframe(df, use_container_width=True, height=500)
        st.caption(f"Showing {len(df)} submissions")

        # Click-through
        selected_id = st.number_input(
            "Enter Submission ID to view detail", min_value=1, step=1, value=None
        )
        if selected_id:
            _show_submission_detail(int(selected_id))
    else:
        st.info("No submissions match the selected filters.")


# ---------------------------------------------------------------------------
# Submission Detail
# ---------------------------------------------------------------------------


def page_submission_detail(docket_id: int):
    st.title("Submission Detail")
    sub_id = st.number_input("Submission ID", min_value=1, step=1)
    if sub_id:
        _show_submission_detail(int(sub_id))


def _show_submission_detail(sub_id: int):
    sub = db().get(Submission, sub_id)
    if not sub:
        st.error(f"Submission {sub_id} not found.")
        return

    analysis = (
        db()
        .query(LLMAnalysis)
        .filter(LLMAnalysis.submission_id == sub_id)
        .first()
    )
    dedup = (
        db()
        .query(SubmissionDedupe)
        .filter(SubmissionDedupe.submission_id == sub_id)
        .first()
    )

    st.subheader(f"Submission #{sub_id}: {sub.commenter_name}")

    col1, col2 = st.columns(2)
    with col1:
        st.markdown(f"**External ID:** {sub.external_id}")
        st.markdown(f"**Organization:** {sub.organization or (analysis.organization_extracted if analysis else 'N/A')}")
        st.markdown(f"**Date:** {sub.submission_date}")
        st.markdown(f"**Comment URL:** {sub.comment_url}")
    with col2:
        if analysis:
            st.markdown(f"**Stance:** {analysis.stance.value if analysis.stance else 'N/A'}")
            st.markdown(f"**Commenter Type:** {analysis.commenter_type.value if analysis.commenter_type else 'N/A'}")
            st.markdown(f"**Substantive Score:** {round(analysis.substantive_score or 0, 3)}")
            st.markdown(f"**Template Likelihood:** {round(analysis.template_likelihood or 0, 2)}")
        if dedup:
            st.markdown(f"**Is Canonical:** {dedup.is_canonical}")
            st.markdown(f"**Dedupe Group:** {dedup.dedupe_group_id} (method: {dedup.dedup_method.value if dedup.dedup_method else 'N/A'})")

    if analysis:
        st.markdown("---")
        st.subheader("LLM Analysis")
        st.markdown(f"**Short summary:** {analysis.summary_short}")
        with st.expander("Detailed summary"):
            st.write(analysis.summary_detailed)

        tabs = st.tabs(["Issues", "Arguments", "Quotes", "Source Spans", "Raw"])
        with tabs[0]:
            st.write("**Issues raised:**")
            for issue in (analysis.issues or []):
                st.write(f"• {issue}")
            st.write("**Requested changes:**")
            for rc in (analysis.requested_changes or []):
                st.write(f"• {rc}")
        with tabs[1]:
            for label, args in [
                ("Legal", analysis.legal_arguments),
                ("Economic", analysis.economic_arguments),
                ("Operational", analysis.operational_arguments),
                ("Policy", analysis.policy_arguments),
            ]:
                if args:
                    st.write(f"**{label}:**")
                    for a in args:
                        st.write(f"• {a}")
            if analysis.cited_authorities:
                st.write("**Cited authorities:**")
                for c in analysis.cited_authorities:
                    st.write(f"• {c}")
        with tabs[2]:
            for q in (analysis.notable_quotes or []):
                if isinstance(q, dict):
                    st.markdown(f"> {q.get('quote', '')}")
        with tabs[3]:
            for s in (analysis.source_spans or []):
                if isinstance(s, dict):
                    st.markdown(f"**Claim:** {s.get('claim', '')}")
                    st.markdown(f"> {s.get('excerpt', '')}")
                    st.markdown("---")
        with tabs[4]:
            st.json(analysis.analysis or {})

    # Source text
    st.markdown("---")
    st.subheader("Source Text")
    canonical_er = (
        db()
        .query(ExtractionResult)
        .filter(
            ExtractionResult.submission_id == sub_id,
            ExtractionResult.source_type == "canonical",
        )
        .first()
    )
    if canonical_er:
        with st.expander("Canonical text (combined)"):
            st.text(canonical_er.cleaned_text or "(empty)")

    # Attachments
    attachments = sub.attachments
    if attachments:
        st.subheader("Attachments")
        for att in attachments:
            col1, col2, col3 = st.columns([3, 1, 1])
            col1.write(att.filename or att.original_url)
            col2.write(att.file_type or "unknown")
            col3.write(att.download_status)
            if att.file_path and Path(att.file_path).exists():
                with open(att.file_path, "rb") as f:
                    st.download_button(
                        f"Download {att.filename}",
                        f.read(),
                        file_name=att.filename,
                    )


# ---------------------------------------------------------------------------
# Dedupe Families
# ---------------------------------------------------------------------------


def page_dedupe_families(docket_id: int):
    st.title("Dedupe Families")

    groups = (
        db()
        .query(DedupeGroup)
        .filter(DedupeGroup.docket_id == docket_id, DedupeGroup.group_size > 1)
        .order_by(DedupeGroup.group_size.desc())
        .limit(50)
        .all()
    )

    if not groups:
        st.info("No duplicate groups found. Run dedupe_submissions stage first.")
        return

    for group in groups:
        with st.expander(
            f"Group {group.id} — {group.dedup_method.value if group.dedup_method else 'N/A'} — {group.group_size} members"
        ):
            members = (
                db()
                .query(SubmissionDedupe, Submission)
                .join(Submission, SubmissionDedupe.submission_id == Submission.id)
                .filter(SubmissionDedupe.dedupe_group_id == group.id)
                .all()
            )
            data = [
                {
                    "ID": sub.id,
                    "Commenter": sub.commenter_name,
                    "Organization": sub.organization,
                    "Date": str(sub.submission_date.date() if sub.submission_date else ""),
                    "Is Canonical": dm.is_canonical,
                    "Similarity": round(dm.similarity_score or 1.0, 3),
                }
                for dm, sub in members
            ]
            st.dataframe(pd.DataFrame(data), use_container_width=True)


# ---------------------------------------------------------------------------
# Theme Clusters
# ---------------------------------------------------------------------------


def page_theme_clusters(docket_id: int):
    st.title("Theme Clusters")

    clusters = (
        db()
        .query(ThemeCluster)
        .filter(ThemeCluster.docket_id == docket_id)
        .order_by(ThemeCluster.total_count.desc())
        .all()
    )

    if not clusters:
        st.info("No clusters found. Run cluster_themes stage first.")
        return

    for cluster in clusters:
        label = cluster.analyst_label or cluster.auto_label or f"Cluster {cluster.id}"
        with st.expander(f"{label} — {cluster.total_count} submissions"):
            col1, col2 = st.columns(2)
            with col1:
                st.markdown(f"**Auto label:** {cluster.auto_label}")
                new_label = st.text_input(
                    "Analyst label (edit to rename)",
                    value=cluster.analyst_label or "",
                    key=f"label_{cluster.id}",
                )
                if st.button("Save label", key=f"save_{cluster.id}"):
                    cluster.analyst_label = new_label
                    db().commit()
                    st.success("Label saved.")

            with col2:
                if cluster.keywords:
                    st.markdown(f"**Keywords:** {', '.join(cluster.keywords)}")
                if cluster.description:
                    st.markdown(f"**Description:** {cluster.description}")

            if cluster.rep_arguments_for:
                st.markdown("**Arguments in support:**")
                for arg in cluster.rep_arguments_for:
                    st.write(f"• {arg}")
            if cluster.rep_arguments_against:
                st.markdown("**Arguments against:**")
                for arg in cluster.rep_arguments_against:
                    st.write(f"• {arg}")

            # Representative submissions
            rep_memberships = (
                db()
                .query(ClusterMembership, Submission, LLMAnalysis)
                .join(Submission, ClusterMembership.submission_id == Submission.id)
                .outerjoin(LLMAnalysis, LLMAnalysis.submission_id == Submission.id)
                .filter(
                    ClusterMembership.cluster_id == cluster.id,
                    ClusterMembership.is_representative == True,
                )
                .limit(5)
                .all()
            )
            if rep_memberships:
                st.markdown("**Representative submissions:**")
                for cm, sub, analysis in rep_memberships:
                    st.markdown(
                        f"- **#{sub.id}** {sub.commenter_name}"
                        + (f" ({sub.organization})" if sub.organization else "")
                        + (f" — {analysis.summary_short[:100]}" if analysis else "")
                    )


# ---------------------------------------------------------------------------
# Report View
# ---------------------------------------------------------------------------


def page_report(docket_id: int):
    st.title("Report")

    report_runs = (
        db()
        .query(ReportRun)
        .filter(ReportRun.docket_id == docket_id, ReportRun.status == "completed")
        .order_by(ReportRun.created_at.desc())
        .all()
    )

    if not report_runs:
        st.info("No completed reports. Run the generate_report pipeline stage.")
        return

    options = {f"Report #{r.id} ({r.created_at})": r for r in report_runs}
    selected = st.selectbox("Select report", list(options.keys()))
    report_run = options[selected]

    if report_run.report_path and Path(report_run.report_path).exists():
        md = Path(report_run.report_path).read_text(encoding="utf-8")
        st.markdown(md)
        st.download_button(
            "Download report",
            md.encode("utf-8"),
            file_name=f"cftc_report_{report_run.id}.md",
            mime="text/markdown",
        )
    else:
        st.warning("Report file not found.")


# ---------------------------------------------------------------------------
# Pipeline Status
# ---------------------------------------------------------------------------


def page_pipeline_status(docket_id: int):
    st.title("Pipeline Status")

    from cftc_pipeline.pipeline.runner import get_pipeline_status

    status = get_pipeline_status(db(), docket_id)
    df = pd.DataFrame(status)
    st.dataframe(df, use_container_width=True)

    st.subheader("Rerun a stage")
    stage = st.selectbox("Stage", [row["stage"] for row in status])
    force = st.checkbox("Force (re-run even if completed)")
    if st.button("Run stage"):
        from cftc_pipeline.pipeline.runner import run_stage

        with st.spinner(f"Running {stage}..."):
            try:
                result = run_stage(db(), docket_id, stage, force=force)
                st.success(f"Done: {result}")
            except Exception as exc:
                st.error(f"Failed: {exc}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    result = sidebar()
    if result is None:
        st.info("Initialize a docket to get started.")
        return

    docket_id, page = result

    if page == "Dashboard":
        page_dashboard(docket_id)
    elif page == "Submission List":
        page_submission_list(docket_id)
    elif page == "Submission Detail":
        page_submission_detail(docket_id)
    elif page == "Dedupe Families":
        page_dedupe_families(docket_id)
    elif page == "Theme Clusters":
        page_theme_clusters(docket_id)
    elif page == "Report":
        page_report(docket_id)
    elif page == "Pipeline Status":
        page_pipeline_status(docket_id)


if __name__ == "__main__":
    main()
