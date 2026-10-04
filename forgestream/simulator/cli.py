"""
Standalone CLI entry point for the ForgeStream Industrial Simulator.
"""

import argparse
import json
import sys
from forgestream.simulator.generator import TelemetryGenerator
from forgestream.schemas.telemetry_schema import ScenarioID
from forgestream.observability.logging import setup_logging, get_logger

logger = get_logger("simulator.cli")


def main():
    parser = argparse.ArgumentParser(description="ForgeStream Industrial Telemetry Simulator")
    parser.add_argument("--seed", type=int, default=42, help="PRNG random seed for determinism")
    parser.add_argument("--duration", type=int, default=10, help="Simulation duration in seconds")
    parser.add_argument("--rate", type=float, default=10.0, help="Event rate per asset (Hz)")
    parser.add_argument("--scenario", type=str, default="SCENARIO_001_NORMAL_OPERATION", help="Scenario ID")
    parser.add_argument("--format", choices=["json", "summary"], default="summary", help="Output format")
    args = parser.parse_args()

    setup_logging(level="INFO", log_format="text")
    logger.info("Initializing simulator", extra={"seed": args.seed, "duration": args.duration})

    generator = TelemetryGenerator(seed=args.seed, event_rate_hz=args.rate)
    try:
        scenario_enum = ScenarioID(args.scenario)
        for asset_id in generator.states:
            generator.set_scenario(asset_id, scenario_enum)
    except ValueError:
        logger.error(f"Unknown scenario: {args.scenario}")
        sys.exit(1)

    count = 0
    sample_events = []
    for event in generator.generate_events(duration_sec=args.duration):
        count += 1
        if count <= 5:
            sample_events.append(event.model_dump(mode="json"))
        if args.format == "json":
            print(json.dumps(event.model_dump(mode="json")))

    if args.format == "summary":
        print("=" * 70)
        print(f"FORGESTREAM SIMULATOR RUN COMPLETE")
        print(f"Total Events Generated: {count}")
        print(f"Seed: {args.seed} | Duration: {args.duration}s | Rate: {args.rate} Hz")
        print(f"Active Assets: {list(generator.states.keys())}")
        print("=" * 70)
        print("First 3 Sample Events:")
        for idx, s in enumerate(sample_events[:3]):
            print(f"  [{idx+1}] Asset: {s['asset_id']} | Mode: {s['operating_mode']} | "
                  f"Temp: {s['temperature']}°C | Vibe: {s['vibration']} mm/s | Power: {s['power']} kW")


if __name__ == "__main__":
    main()
