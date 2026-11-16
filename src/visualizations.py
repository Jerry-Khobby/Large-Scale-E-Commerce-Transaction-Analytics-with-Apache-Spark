"""
Stage 8 - Visualisations of the analytics results.

Reads the small aggregated CSVs written by ``spark_analytics.py`` from
``data/analytics/`` (Pandas is fine here: each result is at most a few
thousand rows) and saves PNG charts to ``outputs/visualizations/``.

Usage:
    python src/visualizations.py
"""

from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # headless: write files, never open windows
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402
import psutil  # noqa: E402
from matplotlib.colors import LinearSegmentedColormap  # noqa: E402
from matplotlib.ticker import FuncFormatter  # noqa: E402

import config  # noqa: E402
from utils import ensure_dirs, get_logger  # noqa: E402

logger = get_logger("Visualizations", "visualizations.log")

# Validated default palette (light mode)
SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_SECONDARY = "#52514e"
MUTED = "#898781"
GRID = "#e1e0d9"
BASELINE = "#c3c2b7"
PRIMARY = "#2a78d6"
CATEGORICAL = [
    "#2a78d6",
    "#eb6834",
    "#1baf7a",
    "#eda100",
    "#e87ba4",
    "#008300",
    "#6250d6",
    "#e34948",
]
SEQUENTIAL = LinearSegmentedColormap.from_list(
    "blues", ["#cde2fb", "#86b6ef", "#3987e5", "#256abf", "#184f95", "#0d366b"]
)

plt.rcParams.update(
    {
        "figure.facecolor": SURFACE,
        "axes.facecolor": SURFACE,
        "savefig.facecolor": SURFACE,
        "axes.edgecolor": BASELINE,
        "axes.labelcolor": INK_SECONDARY,
        "axes.titlecolor": INK,
        "axes.titlesize": 14,
        "axes.titleweight": "bold",
        "axes.titlelocation": "left",
        "axes.spines.top": False,
        "axes.spines.right": False,
        "xtick.color": MUTED,
        "ytick.color": MUTED,
        "text.color": INK,
        "axes.grid": True,
        "grid.color": GRID,
        "grid.linewidth": 0.6,
        "axes.axisbelow": True,
        "font.size": 10,
    }
)


def _money(v: float, _=None) -> str:
    if abs(v) >= 1e6:
        return f"${v / 1e6:,.1f}M"
    if abs(v) >= 1e3:
        return f"${v / 1e3:,.0f}K"
    return f"${v:,.0f}"


money = FuncFormatter(_money)


def read_result(name: str) -> pd.DataFrame:
    """Read the single part-*.csv file Spark wrote for an analytics result."""
    parts = sorted((config.ANALYTICS_DIR / name).glob("part-*.csv"))
    if not parts:
        raise FileNotFoundError(
            f"No CSV for '{name}' - run spark_analytics.py first"
        )
    return pd.read_csv(parts[0])


def save(fig: plt.Figure, filename: str) -> Path:
    out = config.VISUALIZATIONS_DIR / filename
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)
    logger.info(f"Saved {out}")
    return out


def hbar(
    df: pd.DataFrame, label_col: str, value_col: str, title: str, filename: str
) -> None:
    """Horizontal bar chart, largest at the top, values at the bar ends."""
    df = df.sort_values(value_col)
    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.barh(
        df[label_col].astype(str), df[value_col], color=PRIMARY, height=0.7
    )
    ax.xaxis.set_major_formatter(money)
    ax.grid(axis="y", visible=False)
    ax.bar_label(
        bars,
        labels=[money(v, None) for v in df[value_col]],
        padding=4,
        color=INK_SECONDARY,
        fontsize=8,
    )
    ax.set_title(title)
    ax.set_xlabel("Revenue (successful transactions)")
    save(fig, filename)


def top_countries() -> None:
    hbar(
        read_result("revenue_by_country").nlargest(10, "total_revenue"),
        "country",
        "total_revenue",
        "Top 10 countries by revenue",
        "01_top_countries_revenue.png",
    )


def monthly_trend() -> None:
    df = read_result("monthly_revenue_trend").sort_values("year_month")
    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(
        df["year_month"],
        df["monthly_revenue"],
        color=PRIMARY,
        linewidth=2,
        marker="o",
        markersize=4,
    )
    ax.yaxis.set_major_formatter(money)
    ax.set_ylim(bottom=0)
    ax.set_xticks(df["year_month"][::3])
    ax.tick_params(axis="x", rotation=45)
    ax.grid(axis="x", visible=False)
    ax.set_title("Monthly revenue, 2022-2024")
    ax.set_ylabel("Revenue")
    save(fig, "02_monthly_revenue_trend.png")


def payment_methods() -> None:
    df = read_result("revenue_by_payment").sort_values(
        "total_revenue", ascending=False
    )
    fig, ax = plt.subplots(figsize=(7, 7))
    wedges, *_ = ax.pie(
        df["total_revenue"],
        colors=CATEGORICAL[: len(df)],
        startangle=90,
        counterclock=False,
        wedgeprops={"width": 0.4, "edgecolor": SURFACE, "linewidth": 2},
        autopct="%1.1f%%",
        pctdistance=0.8,
        textprops={"color": "#ffffff", "fontsize": 10},
    )
    ax.legend(
        wedges,
        df["payment_method"].str.replace("_", " ").str.title(),
        loc="center",
        frameon=False,
    )
    ax.set_title("Revenue share by payment method")
    save(fig, "03_revenue_by_payment_method.png")


def top_products() -> None:
    hbar(
        read_result("top_products_revenue").nlargest(10, "total_revenue"),
        "product_name",
        "total_revenue",
        "Top 10 products by revenue",
        "04_top_products.png",
    )


def order_value_histogram() -> None:
    df = read_result("order_value_distribution").sort_values(
        "order_value_bucket"
    )
    fig, ax = plt.subplots(figsize=(12, 5))
    ax.bar(
        df["order_value_bucket"],
        df["orders"],
        width=46,
        align="edge",
        color=PRIMARY,
    )
    ax.xaxis.set_major_formatter(money)
    ax.yaxis.set_major_formatter(
        FuncFormatter(
            lambda v, _: f"{v / 1e6:,.1f}M" if v >= 1e6 else f"{v / 1e3:,.0f}K"
        )
    )
    ax.grid(axis="x", visible=False)
    ax.set_title("Order value distribution ($50 buckets)")
    ax.set_xlabel("Order value")
    ax.set_ylabel("Successful orders")
    save(fig, "05_order_value_distribution.png")


def hour_day_heatmap() -> None:
    df = read_result("hour_day_heatmap")
    grid = df.pivot_table(
        index=["day_of_week_num", "day_of_week"],
        columns="transaction_hour",
        values="transactions",
    ).sort_index()
    fig, ax = plt.subplots(figsize=(14, 4.5))
    image = ax.imshow(grid.to_numpy(), aspect="auto", cmap=SEQUENTIAL)
    ax.set_yticks(range(len(grid)), grid.index.get_level_values("day_of_week"))
    ax.set_xticks(range(24), [f"{h:02d}" for h in range(24)])
    ax.grid(False)
    ax.set_xlabel("Hour of day")
    ax.set_title("Transactions by day and hour")
    cbar = fig.colorbar(image, ax=ax, pad=0.01)
    cbar.outline.set_visible(False)
    cbar.set_label("Transactions", color=INK_SECONDARY)
    save(fig, "06_hour_day_heatmap.png")


def pandas_memory_trace() -> None:
    """Memory of the Pandas full-load attempt over time (pandas_test.py)."""
    trace_file = config.REPORTS_DIR / "pandas_memory_trace.csv"
    if not trace_file.exists():
        raise FileNotFoundError("No memory trace - run pandas_test.py first")
    df = pd.read_csv(trace_file)
    # RAM of the machine the test ran on (recorded in the trace),
    # not of this one
    total_gb = (
        df["machine_ram_mb"].iloc[0] / 1024
        if "machine_ram_mb" in df
        else psutil.virtual_memory().total / 1024**3
    )
    # Projected size of the full DataFrame, measured from the 1M-row sample
    results_file = config.REPORTS_DIR / "pandas_test_results.csv"
    projected_gb = None
    if results_file.exists():
        res = pd.read_csv(results_file).set_index("test")
        if "pandas_full_dataset_estimate" in res.index:
            projected_gb = (
                res.loc["pandas_full_dataset_estimate", "dataframe_mb"] / 1024
            )

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(
        df["seconds"],
        df["pandas_process_mb"] / 1024,
        color=PRIMARY,
        linewidth=2,
    )
    last = df.iloc[-1]
    ax.annotate(
        f"Load aborted at {last['system_memory_pct']:.0f}% system RAM",
        (last["seconds"], last["pandas_process_mb"] / 1024),
        xytext=(-8, 10),
        textcoords="offset points",
        ha="right",
        color=INK_SECONDARY,
        fontsize=9,
    )

    lines = [(total_gb, f"Machine RAM ({total_gb:.1f} GB)")]
    if projected_gb:
        lines.append(
            (projected_gb, f"Projected full DataFrame ({projected_gb:.1f} GB)")
        )
    for y, label in lines:
        ax.axhline(y, color=MUTED, linewidth=1, linestyle="--")
        ax.annotate(
            label,
            (df["seconds"].iloc[0], y),
            xytext=(0, 5),
            textcoords="offset points",
            color=INK_SECONDARY,
            fontsize=9,
        )
    ax.set_ylim(0, max(y for y, _ in lines) * 1.12)
    ax.grid(axis="x", visible=False)
    ax.set_title("Pandas full-dataset load: process memory over time")
    ax.set_xlabel("Seconds since load started")
    ax.set_ylabel("Pandas process memory (GB)")
    save(fig, "07_pandas_memory_trace.png")


def main() -> None:
    ensure_dirs()
    charts = [
        top_countries,
        monthly_trend,
        payment_methods,
        top_products,
        order_value_histogram,
        hour_day_heatmap,
        pandas_memory_trace,
    ]
    failed = 0
    for chart in charts:
        try:
            chart()
        except Exception:
            failed += 1
            logger.error(f"Chart {chart.__name__} failed", exc_info=True)
    logger.info(f"{len(charts) - failed}/{len(charts)} charts created")


if __name__ == "__main__":
    main()
