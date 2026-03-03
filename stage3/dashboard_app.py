from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st


DEFAULT_INPUT = "UVH26_Project/outputs/stage2/viewpoint_analytics_full_4780vp.jsonl"


@st.cache_data(show_spinner=False)
def load_analytics(path_text: str) -> pd.DataFrame:
    path = Path(path_text)
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")

    rows: list[dict] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))

    if not rows:
        raise ValueError("No rows found in analytics JSONL.")

    df = pd.DataFrame(rows)
    numeric_cols = [
        "viewpoint_id",
        "total_images",
        "total_vehicles",
        "avg_vehicle_count",
        "per_vehicle_count",
        "avg_bbox_density",
        "heavy_vehicle_ratio",
        "two_wheeler_ratio",
        "entropy",
        "congestion_index",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "class_distribution_vector" in df.columns:
        df["class_distribution_vector"] = df["class_distribution_vector"].fillna("{}")

    return df


def parse_distribution(text: str) -> pd.DataFrame:
    try:
        payload = json.loads(text) if text else {}
    except json.JSONDecodeError:
        payload = {}

    rows = [{"class_name": str(k), "share": float(v)} for k, v in payload.items()]
    return pd.DataFrame(rows).sort_values("share", ascending=False) if rows else pd.DataFrame(columns=["class_name", "share"])


def main() -> None:
    st.set_page_config(page_title="DS-City Stage 3 Dashboard", layout="wide")
    st.title("DS-City Analytics — Stage 3 Dashboard")
    st.caption("Interactive view over Stage 2 inferred viewpoint statistics.")

    with st.sidebar:
        st.header("Inputs")
        input_path = st.text_input("Analytics JSONL", value=DEFAULT_INPUT)
        top_n = st.slider("Top viewpoints (ranked views)", min_value=5, max_value=100, value=20, step=5)
        min_images = st.slider("Min images per viewpoint", min_value=1, max_value=200, value=1)
        max_entropy = st.slider("Max entropy filter", min_value=0.0, max_value=5.0, value=5.0, step=0.1)

    try:
        df = load_analytics(input_path)
    except Exception as exc:
        st.error(str(exc))
        return

    filtered = df[(df["total_images"] >= min_images) & (df["entropy"] <= max_entropy)].copy()
    if filtered.empty:
        st.warning("No viewpoints match current filters.")
        return

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Viewpoints", f"{len(filtered):,}")
    c2.metric("Total Vehicles", f"{int(filtered['total_vehicles'].sum()):,}")
    c3.metric("Mean Congestion", f"{filtered['congestion_index'].mean():.4f}")
    c4.metric("Mean BBox Density", f"{filtered['avg_bbox_density'].mean():.4f}")

    top_congestion = filtered.nlargest(top_n, "congestion_index").sort_values("congestion_index", ascending=True)
    fig_top_congestion = px.bar(
        top_congestion,
        x="congestion_index",
        y=top_congestion["viewpoint_id"].astype(str),
        orientation="h",
        title=f"Top {top_n} Viewpoints by Congestion Index",
        labels={"y": "viewpoint_id"},
    )

    fig_scatter = px.scatter(
        filtered,
        x="avg_vehicle_count",
        y="avg_bbox_density",
        size="total_images",
        color="entropy",
        hover_data=["viewpoint_id", "total_vehicles", "congestion_index"],
        title="Vehicle Count vs BBox Density (size = total_images, color = entropy)",
    )

    left, right = st.columns(2)
    left.plotly_chart(fig_top_congestion, use_container_width=True)
    right.plotly_chart(fig_scatter, use_container_width=True)

    fig_ratio = px.scatter(
        filtered,
        x="heavy_vehicle_ratio",
        y="two_wheeler_ratio",
        hover_data=["viewpoint_id", "total_images", "total_vehicles"],
        title="Composition View: Heavy Vehicle Ratio vs Two-Wheeler Ratio",
    )
    fig_hist = px.histogram(
        filtered,
        x="congestion_index",
        nbins=40,
        title="Congestion Index Distribution",
    )

    left2, right2 = st.columns(2)
    left2.plotly_chart(fig_ratio, use_container_width=True)
    right2.plotly_chart(fig_hist, use_container_width=True)

    st.subheader("Top Viewpoints Table")
    show_cols = [
        "viewpoint_id",
        "total_images",
        "total_vehicles",
        "avg_vehicle_count",
        "per_vehicle_count",
        "avg_bbox_density",
        "heavy_vehicle_ratio",
        "two_wheeler_ratio",
        "entropy",
        "congestion_index",
    ]
    st.dataframe(
        filtered.sort_values("congestion_index", ascending=False)[show_cols].head(top_n),
        use_container_width=True,
        hide_index=True,
    )

    st.subheader("Single Viewpoint Deep Dive")
    vp_ids = filtered["viewpoint_id"].astype(int).sort_values().tolist()
    selected_vp = st.selectbox("Select viewpoint_id", vp_ids)
    vp_row = filtered.loc[filtered["viewpoint_id"] == selected_vp].iloc[0]

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Images", int(vp_row["total_images"]))
    m2.metric("Total Vehicles", int(vp_row["total_vehicles"]))
    m3.metric("Avg Vehicle Count", f"{vp_row['avg_vehicle_count']:.4f}")
    m4.metric("Congestion Index", f"{vp_row['congestion_index']:.4f}")

    dist_df = parse_distribution(vp_row.get("class_distribution_vector", "{}"))
    if dist_df.empty:
        st.info("No class distribution data available for this viewpoint.")
    else:
        fig_dist = px.bar(
            dist_df,
            x="class_name",
            y="share",
            title=f"Class Distribution — Viewpoint {selected_vp}",
        )
        st.plotly_chart(fig_dist, use_container_width=True)


if __name__ == "__main__":
    main()
