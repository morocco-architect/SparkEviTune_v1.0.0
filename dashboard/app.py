from __future__ import annotations

import json
import os

import httpx
import streamlit as st

API_URL = os.getenv("SPARKEVITUNE_API_URL", "http://localhost:8000")

st.set_page_config(page_title="SparkEviTune", layout="wide")
st.title("SparkEviTune")
st.caption("Rules + ML anomaly detection + performance prediction + constrained optimization + optional LLM explanation")
st.warning("The project never auto-applies a configuration. Review and benchmark every proposed change.")

with st.sidebar:
    st.header("Cluster profile")
    workers = st.number_input("Workers", min_value=1, value=1)
    cores = st.number_input("Cores per worker", min_value=1, value=4)
    memory = st.number_input("Memory per worker (GB)", min_value=1.0, value=4.0)
    input_size = st.number_input("Input size (GB)", min_value=0.0, value=0.5)
    joins = st.number_input("Estimated joins", min_value=0, value=0)
    aggregations = st.number_input("Estimated aggregations", min_value=0, value=1)

uploaded = st.file_uploader("Upload a Spark event log", type=["json", "jsonl", "log"])
if uploaded and st.button("Analyze"):
    with st.spinner("Analyzing event log..."):
        response = httpx.post(
            f"{API_URL}/analyze/upload",
            params={
                "workers": workers,
                "cores_per_worker": cores,
                "memory_per_worker_gb": memory,
                "input_size_gb": input_size,
                "num_joins": joins,
                "num_aggregations": aggregations,
            },
            files={"file": (uploaded.name, uploaded.getvalue(), "application/json")},
            timeout=180,
        )
    if response.is_error:
        st.error(response.text)
    else:
        st.session_state["report"] = response.json()

report = st.session_state.get("report")
if report:
    rule = report["rule_report"]
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Rule-compliance score", f"{rule['rule_compliance_score']}/100")
    col2.metric("Observed duration", f"{rule['duration_s']:.2f}s")
    col3.metric("ML anomaly score", f"{report['anomaly']['score']:.3f}")
    predicted = report["baseline_prediction"].get("duration_s")
    col4.metric("Predicted duration", f"{predicted:.2f}s" if predicted is not None else "Model unavailable")

    st.subheader("Explanation")
    st.write(report.get("explanation", ""))

    st.subheader("Recommendations")
    st.dataframe(report["fused_recommendations"], use_container_width=True)

    st.subheader("Validated configuration")
    st.code("\n".join(f"{key} {value}" for key, value in report["validation"]["configuration"].items()))
    if report["validation"]["adjustments"]:
        st.info("\n".join(report["validation"]["adjustments"]))
    if report["validation"]["violations"]:
        st.error("\n".join(report["validation"]["violations"]))

    st.download_button(
        "Download JSON report",
        data=json.dumps(report, indent=2),
        file_name=f"sparkevitune-{report['run_id']}.json",
        mime="application/json",
    )
