"""Target leakage audit CLI tool for ForgeStream Phase 3."""

import sys
import os
import argparse
import pandas as pd

# Add repo root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from forgestream.ml.config import FeatureRegistryConfig
from forgestream.ml.leakage_auditor import TargetLeakageAuditor


def main():
    parser = argparse.ArgumentParser(description="ForgeStream Target Leakage & Causality Auditor")
    parser.add_argument("--data-path", default="data/ml/telemetry_ml_features.parquet", help="Path to features parquet")
    parser.add_argument("--output-json", default="results/phase3_leakage_audit.json", help="Path to output JSON")
    args = parser.parse_args()

    if not os.path.exists(args.data_path):
        print(f"Error: Dataset not found at {args.data_path}. Generating dataset first...")
        from forgestream.ml.dataset_generator import MLDatasetGenerator
        gen = MLDatasetGenerator()
        df = gen.generate_full_dataset()
    else:
        df = pd.read_parquet(args.data_path)

    auditor = TargetLeakageAuditor(FeatureRegistryConfig())
    report = auditor.run_full_audit(df, output_json_path=args.output_json)

    print("\n================ TARGET LEAKAGE AUDIT REPORT ================")
    print(f"Overall Status: {report['overall_status']}")
    for check_name, res in report["checks"].items():
        print(f"  • {check_name}: {res['status']} - {res['message']}")
    print("============================================================\n")

    if report["overall_status"] != "PASS":
        sys.exit(1)


if __name__ == "__main__":
    main()
