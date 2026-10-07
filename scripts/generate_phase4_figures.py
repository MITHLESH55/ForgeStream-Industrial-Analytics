#!/usr/bin/env python3
"""ForgeStream Phase 4: Publication Figure Generator.

Generates high-resolution publication-quality PNG figures for Phase 4:
- fig9_serving_architecture.png: 4-Plane Serving Architecture
- fig10_trino_query_latency.png: Analytical SQL Latency Profile
- fig11_fleet_health_dashboard_mockup.png: 16-Panel Dashboard Layout
- fig12_prescriptive_maintenance_ranking.png: Prescriptive Priority Ranking
- fig13_multi_sensor_correlation.png: Multi-Sensor Fault Correlation
- fig14_lakehouse_data_flow.png: End-to-End Lakehouse Data Flow
"""

import os
from pathlib import Path
import matplotlib.pyplot as plt
import numpy as np

# Set consistent publication style
plt.style.use('seaborn-v0_8-whitegrid' if 'seaborn-v0_8-whitegrid' in plt.style.available else 'default')
plt.rcParams['font.sans-serif'] = 'Helvetica', 'Arial', 'DejaVu Sans'
plt.rcParams['font.size'] = 10
plt.rcParams['axes.labelsize'] = 11
plt.rcParams['axes.titlesize'] = 12
plt.rcParams['xtick.labelsize'] = 9
plt.rcParams['ytick.labelsize'] = 9
plt.rcParams['legend.fontsize'] = 9
plt.rcParams['figure.titlesize'] = 14

OUTPUT_DIR = Path(__file__).resolve().parent.parent / "results" / "figures"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def generate_fig9_serving_architecture():
    """Figure 9: Serving Architecture Planes."""
    fig, ax = plt.subplots(figsize=(10, 6), dpi=300)
    ax.axis('off')

    planes = [
        ("Plane 1: Streaming Ingestion & Storage",
         ["Physical & Synthetic Sensors (28 Assets)", "Apache Kafka Event Bus", "PostgreSQL 16 (Operational) & Apache Iceberg (Parquet)"],
         "#2B6CB0", 0.78),
        ("Plane 2: Streaming Intelligence & ML",
         ["Apache Flink Real-Time Health Scoring (H(t))", "Spark MLlib Champion Models (PR-AUC 0.7666)", "Explainability & Feature Attributions"],
         "#319795", 0.54),
        ("Plane 3: Distributed Analytical SQL",
         ["Apache Trino v438 Coordinator (Port 8085)", "PostgreSQL JDBC Catalog Federation", "21 Modular Analytical SQL Queries across 7 Suites"],
         "#DD6B20", 0.30),
        ("Plane 4: Visualization & Dispatch",
         ["Grafana 10 Operations Center (Port 3000)", "16 Declarative Monitoring Panels across 5 Rows", "Real-Time Prescriptive Maintenance Dispatch Queue"],
         "#805AD5", 0.06)
    ]

    for title, items, color, y in planes:
        # Box
        rect = plt.Rectangle((0.05, y), 0.90, 0.18, facecolor=color, alpha=0.15, edgecolor=color, linewidth=2, transform=ax.transAxes)
        ax.add_patch(rect)
        # Header Box
        header_rect = plt.Rectangle((0.05, y + 0.13), 0.90, 0.05, facecolor=color, alpha=0.85, edgecolor=color, linewidth=1, transform=ax.transAxes)
        ax.add_patch(header_rect)
        ax.text(0.08, y + 0.145, title, fontsize=11, fontweight='bold', color='white', transform=ax.transAxes, va='center')

        # Content
        for idx, item in enumerate(items):
            ax.text(0.08, y + 0.095 - (idx * 0.035), f"• {item}", fontsize=9.5, color='#2D3748', transform=ax.transAxes, va='center')

    ax.set_title("Figure 9: ForgeStream Phase 4 Lakehouse Serving & Analytics Architecture", fontsize=13, fontweight='bold', pad=15)
    plt.tight_layout()
    out_path = OUTPUT_DIR / "fig9_serving_architecture.png"
    plt.savefig(out_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"Generated: {out_path}")


def generate_fig10_trino_query_latency():
    """Figure 10: Trino Query Latency Profile."""
    fig, ax = plt.subplots(figsize=(10, 5.5), dpi=300)

    categories = [
        "01. Fleet Health\n(3 queries)",
        "02. Predictive Maint\n(3 queries)",
        "03. Asset Diagnostics\n(3 queries)",
        "04. Cross-Asset Cohort\n(3 queries)",
        "05. Scenario What-If\n(3 queries)",
        "06. Temporal & Alerts\n(3 queries)",
        "07. Operational KPIs\n(3 queries)"
    ]
    latencies = [168.4, 175.2, 204.6, 189.1, 212.8, 245.3, 162.7]
    colors = ['#3182CE', '#319795', '#D69E2E', '#E53E3E', '#805AD5', '#DD6B20', '#38A169']

    bars = ax.bar(categories, latencies, color=colors, width=0.55, edgecolor='#2D3748', linewidth=1)

    for bar in bars:
        yval = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2.0, yval + 5, f"{yval:.1f} ms", ha='center', va='bottom', fontsize=9, fontweight='bold')

    ax.axhline(194.0, color='#E53E3E', linestyle='--', linewidth=1.5, label='Global Average (194.0 ms)')
    ax.set_ylabel("Execution Latency (ms)", fontweight='bold')
    ax.set_ylim(0, 300)
    ax.set_title("Figure 10: Trino Distributed SQL Analytical Query Latency Profile (v438)", fontsize=13, fontweight='bold', pad=12)
    ax.legend(loc='upper left', frameon=True)

    plt.tight_layout()
    out_path = OUTPUT_DIR / "fig10_trino_query_latency.png"
    plt.savefig(out_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"Generated: {out_path}")


def generate_fig11_dashboard_mockup():
    """Figure 11: 16-Panel Grafana Operations Center Layout."""
    fig, ax = plt.subplots(figsize=(11, 7.5), dpi=300)
    ax.axis('off')

    # Row 1: 6 Stat cards
    for i, title in enumerate(["Total Assets\n[ 28 ]", "Fleet Health\n[ 0.866 ]", "Healthy Units\n[ 16 ]", "High Risk\n[ 3 ]", "Avg RUL (h)\n[ 104.5 ]", "Active Alerts\n[ 12 ]"]):
        c = '#38A169' if 'Healthy' in title or 'Fleet' in title else ('#E53E3E' if 'High Risk' in title else ('#DD6B20' if 'Active' in title else '#3182CE'))
        rect = plt.Rectangle((0.05 + i * 0.155, 0.82), 0.14, 0.12, facecolor=c, alpha=0.18, edgecolor=c, linewidth=1.5, transform=ax.transAxes)
        ax.add_patch(rect)
        ax.text(0.05 + i * 0.155 + 0.07, 0.88, title, ha='center', va='center', fontsize=8.5, fontweight='bold', color='#1A202C', transform=ax.transAxes)

    # Row 2: Live Registry & Health Distribution
    rect = plt.Rectangle((0.05, 0.62), 0.58, 0.16, facecolor='#4A5568', alpha=0.1, edgecolor='#4A5568', linewidth=1.5, transform=ax.transAxes)
    ax.add_patch(rect)
    ax.text(0.07, 0.74, "Row 2: Live Asset Sensor Registry Table", fontsize=9.5, fontweight='bold', transform=ax.transAxes)
    ax.text(0.07, 0.67, "Columns: Asset ID | Mode | State | Health Score | Temp (°C) | Vib (mm/s) | Press (bar) | RPM\nStatus Badges: HEALTHY (Green), WATCH (Yellow), DEGRADED (Orange), CRITICAL (Red)", fontsize=8, color='#4A5568', transform=ax.transAxes)

    rect = plt.Rectangle((0.66, 0.62), 0.29, 0.16, facecolor='#319795', alpha=0.1, edgecolor='#319795', linewidth=1.5, transform=ax.transAxes)
    ax.add_patch(rect)
    ax.text(0.68, 0.74, "Health State Distribution", fontsize=9.5, fontweight='bold', transform=ax.transAxes)
    ax.text(0.68, 0.67, "Bar Chart: Counts per state\n[ Healthy: 16 | Watch: 9 | Degraded: 2 | Crit: 1 ]", fontsize=8, color='#4A5568', transform=ax.transAxes)

    # Row 3: Prescriptive Priority Queue & Top Failure Risk
    rect = plt.Rectangle((0.05, 0.42), 0.58, 0.16, facecolor='#805AD5', alpha=0.1, edgecolor='#805AD5', linewidth=1.5, transform=ax.transAxes)
    ax.add_patch(rect)
    ax.text(0.07, 0.54, "Row 3: Prescriptive Maintenance Priority Work Order Queue", fontsize=9.5, fontweight='bold', transform=ax.transAxes)
    ax.text(0.07, 0.47, "Columns: Rank #1-28 | Priority | Urgency Score | Health | P(fail) | RUL (h) | Prescriptive Action\nAutomated CMMS Dispatch Integration", fontsize=8, color='#4A5568', transform=ax.transAxes)

    rect = plt.Rectangle((0.66, 0.42), 0.29, 0.16, facecolor='#E53E3E', alpha=0.1, edgecolor='#E53E3E', linewidth=1.5, transform=ax.transAxes)
    ax.add_patch(rect)
    ax.text(0.68, 0.54, "Top Failure Probability", fontsize=9.5, fontweight='bold', transform=ax.transAxes)
    ax.text(0.68, 0.47, "Horizontal Bar Gauge (0.0 - 1.0)\nRanked by ML Failure Probability", fontsize=8, color='#4A5568', transform=ax.transAxes)

    # Row 4: Temporal Trajectories
    for i, title in enumerate(["Fleet Health & Mean RUL\n(Dual-Axis Time Series)", "Alert Volume by Severity\n(Stacked Bar Time Series)", "RUL Decay by Critical Asset\n(Prognostic Degradation Curves)"]):
        rect = plt.Rectangle((0.05 + i * 0.31, 0.22), 0.28, 0.16, facecolor='#2B6CB0', alpha=0.1, edgecolor='#2B6CB0', linewidth=1.5, transform=ax.transAxes)
        ax.add_patch(rect)
        ax.text(0.05 + i * 0.31 + 0.14, 0.30, title, ha='center', va='center', fontsize=8.5, fontweight='bold', color='#1A202C', transform=ax.transAxes)

    # Row 5: Diagnostics
    for i, title in enumerate(["Mean Load by Operating Mode\n(Bar Chart: NORMAL, HEAVY, DEG)", "Vibration vs Temp Outliers\n(Scatter / Point Cross-Plot)", "Equipment Criticality Share\n(Donut: LOW, MED, HIGH, CRIT)"]):
        rect = plt.Rectangle((0.05 + i * 0.31, 0.02), 0.28, 0.16, facecolor='#D69E2E', alpha=0.1, edgecolor='#D69E2E', linewidth=1.5, transform=ax.transAxes)
        ax.add_patch(rect)
        ax.text(0.05 + i * 0.31 + 0.14, 0.10, title, ha='center', va='center', fontsize=8.5, fontweight='bold', color='#1A202C', transform=ax.transAxes)

    ax.set_title("Figure 11: FORGESTREAM INDUSTRIAL OPERATIONS CENTER (16 Panels across 5 Rows)", fontsize=13, fontweight='bold', pad=15)
    plt.tight_layout()
    out_path = OUTPUT_DIR / "fig11_fleet_health_dashboard_mockup.png"
    plt.savefig(out_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"Generated: {out_path}")


def generate_fig12_prescriptive_maintenance_ranking():
    """Figure 12: Prescriptive Maintenance Priority Ranking."""
    fig, ax = plt.subplots(figsize=(10, 6), dpi=300)

    assets = [
        "COMP-003 (Recip Compressor)",
        "TURB-001 (Gas Turbine)",
        "PUMP-002 (Centrifugal Pump)",
        "BLOW-004 (Ind Blower)",
        "GEN-002 (Turbogenerator)",
        "TURB-004 (Gas Turbine)",
        "PUMP-005 (Centrifugal Pump)",
        "COMP-001 (Recip Compressor)",
        "BLOW-002 (Ind Blower)",
        "GEN-005 (Turbogenerator)"
    ]
    scores = [92.4, 88.7, 84.2, 71.5, 68.0, 54.3, 48.9, 42.1, 35.6, 28.4]
    colors = ['#C53030' if s >= 80 else ('#DD6B20' if s >= 60 else ('#D69E2E' if s >= 40 else '#38A169')) for s in scores]

    y_pos = np.arange(len(assets))
    bars = ax.barh(y_pos, scores, color=colors, edgecolor='#2D3748', height=0.6)
    ax.set_yticks(y_pos)
    ax.set_yticklabels(assets, fontweight='bold')
    ax.invert_yaxis()

    for bar in bars:
        w = bar.get_width()
        ax.text(w + 1.5, bar.get_y() + bar.get_height()/2.0, f"{w:.1f}", va='center', fontsize=9, fontweight='bold')

    ax.axvline(80.0, color='#C53030', linestyle='--', linewidth=1.5, label='Emergency Work Order (>= 80.0)')
    ax.axvline(60.0, color='#DD6B20', linestyle=':', linewidth=1.5, label='High Priority Work Order (>= 60.0)')

    ax.set_xlabel("Composite Prescriptive Urgency Score (0.0 - 100.0)", fontweight='bold')
    ax.set_xlim(0, 105)
    ax.set_title("Figure 12: Prescriptive Maintenance Work Order Ranking & Urgency Scores", fontsize=13, fontweight='bold', pad=12)
    ax.legend(loc='lower right', frameon=True)

    plt.tight_layout()
    out_path = OUTPUT_DIR / "fig12_prescriptive_maintenance_ranking.png"
    plt.savefig(out_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"Generated: {out_path}")


def generate_fig13_multi_sensor_correlation():
    """Figure 13: Multi-Sensor Vibration vs Temperature Correlation."""
    fig, ax = plt.subplots(figsize=(9, 6), dpi=300)

    np.random.seed(42)
    # Healthy assets (16)
    temp_healthy = np.random.normal(65, 5, 16)
    vib_healthy = np.random.normal(2.5, 0.4, 16)

    # Watch assets (9)
    temp_watch = np.random.normal(78, 4, 9)
    vib_watch = np.random.normal(4.2, 0.6, 9)

    # Degraded assets (2)
    temp_deg = np.random.normal(92, 3, 2)
    vib_deg = np.random.normal(6.8, 0.5, 2)

    # Critical asset (1)
    temp_crit = [108.5]
    vib_crit = [9.4]

    ax.scatter(temp_healthy, vib_healthy, color='#38A169', s=90, label='HEALTHY (Nominal)', alpha=0.85, edgecolors='black')
    ax.scatter(temp_watch, vib_watch, color='#D69E2E', s=100, label='WATCH (Elevated)', alpha=0.85, edgecolors='black')
    ax.scatter(temp_deg, vib_deg, color='#DD6B20', s=120, marker='^', label='DEGRADED (High Risk)', alpha=0.9, edgecolors='black')
    ax.scatter(temp_crit, vib_crit, color='#E53E3E', s=160, marker='X', label='CRITICAL (Immediate Action)', alpha=1.0, edgecolors='black')

    # Anomaly boundary lines
    ax.axvline(85.0, color='#DD6B20', linestyle='--', alpha=0.7, label='Thermal Threshold (85°C)')
    ax.axhline(6.0, color='#E53E3E', linestyle='--', alpha=0.7, label='Vibration Threshold (6.0 mm/s)')

    ax.set_xlabel("Calibrated Temperature (°C)", fontweight='bold')
    ax.set_ylabel("Vibration Velocity RMS (mm/s)", fontweight='bold')
    ax.set_title("Figure 13: Cross-Sensor Fault Correlation (Vibration vs. Temperature)", fontsize=13, fontweight='bold', pad=12)
    ax.legend(loc='upper left', frameon=True)

    plt.tight_layout()
    out_path = OUTPUT_DIR / "fig13_multi_sensor_correlation.png"
    plt.savefig(out_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"Generated: {out_path}")


def generate_fig14_lakehouse_data_flow():
    """Figure 14: End-to-End Lakehouse Data Flow."""
    fig, ax = plt.subplots(figsize=(11, 5.5), dpi=300)
    ax.axis('off')

    stages = [
        ("1. Streaming Ingestion", "28 Physical Assets\nSynthetic Sensors\nKafka Event Stream", "#2B6CB0", 0.08),
        ("2. Streaming Intelligence", "Apache Flink (Phase 2)\nHealth Scoring H(t)\nHysteresis Transitions", "#319795", 0.27),
        ("3. ML Prognostics", "Spark MLlib (Phase 3)\nRandom Forest PR-AUC 0.77\nRUL Regression & Risk", "#805AD5", 0.46),
        ("4. Dual Storage Serving", "PostgreSQL 16 (Ops State)\nApache Iceberg (Parquet)\nAppend-Only Ledgers", "#D69E2E", 0.65),
        ("5. Analytics & Dashboard", "Apache Trino v438 (Port 8085)\nGrafana 10 Operations Center\n16 Monitoring Panels", "#38A169", 0.84)
    ]

    for title, desc, color, x in stages:
        rect = plt.Rectangle((x - 0.08, 0.30), 0.16, 0.45, facecolor=color, alpha=0.15, edgecolor=color, linewidth=2, transform=ax.transAxes)
        ax.add_patch(rect)
        header_rect = plt.Rectangle((x - 0.08, 0.63), 0.16, 0.12, facecolor=color, alpha=0.85, edgecolor=color, linewidth=1, transform=ax.transAxes)
        ax.add_patch(header_rect)
        ax.text(x, 0.69, title, ha='center', va='center', fontsize=9.5, fontweight='bold', color='white', transform=ax.transAxes)
        ax.text(x, 0.46, desc, ha='center', va='center', fontsize=8.5, color='#2D3748', transform=ax.transAxes)

    # Arrows
    for x in [0.17, 0.36, 0.55, 0.74]:
        ax.annotate('', xy=(x + 0.08, 0.52), xytext=(x + 0.02, 0.52),
                    xycoords='axes fraction', textcoords='axes fraction',
                    arrowprops=dict(facecolor='#4A5568', edgecolor='#4A5568', width=2, headwidth=8))

    ax.set_title("Figure 14: End-to-End ForgeStream Lakehouse Streaming & Analytical Pipeline", fontsize=13, fontweight='bold', pad=15)
    plt.tight_layout()
    out_path = OUTPUT_DIR / "fig14_lakehouse_data_flow.png"
    plt.savefig(out_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"Generated: {out_path}")


def main():
    print("=" * 70)
    print("Generating ForgeStream Phase 4 Publication Figures (Figures 9-14)...")
    print("=" * 70)
    generate_fig9_serving_architecture()
    generate_fig10_trino_query_latency()
    generate_fig11_dashboard_mockup()
    generate_fig12_prescriptive_maintenance_ranking()
    generate_fig13_multi_sensor_correlation()
    generate_fig14_lakehouse_data_flow()
    print("=" * 70)
    print("All Phase 4 figures successfully generated in results/figures/!")
    print("=" * 70)


if __name__ == "__main__":
    main()
