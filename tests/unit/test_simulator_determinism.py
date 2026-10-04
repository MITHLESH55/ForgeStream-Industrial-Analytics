"""
Unit tests for deterministic telemetry simulation.
Verifies seed repeatability and replayability.
"""

import pytest
from forgestream.simulator.generator import TelemetryGenerator


def test_seed_determinism_identical_runs():
    """Proves that two independent generator runs with the same seed produce identical event sequences."""
    gen1 = TelemetryGenerator(seed=12345, event_rate_hz=5.0)
    gen2 = TelemetryGenerator(seed=12345, event_rate_hz=5.0)

    events1 = list(gen1.generate_events(duration_sec=2.0))
    events2 = list(gen2.generate_events(duration_sec=2.0))

    assert len(events1) == len(events2)
    assert len(events1) > 0

    for e1, e2 in zip(events1, events2):
        assert e1.event_id == e2.event_id
        assert e1.asset_id == e2.asset_id
        assert e1.timestamp == e2.timestamp
        assert e1.temperature == e2.temperature
        assert e1.vibration == e2.vibration
        assert e1.pressure == e2.pressure
        assert e1.power == e2.power
        assert e1.current == e2.current
        assert e1.voltage == e2.voltage
        assert e1.load == e2.load
        assert e1.sequence_number == e2.sequence_number


def test_different_seeds_produce_different_sequences():
    """Proves that changing the PRNG seed generates distinct values."""
    gen_a = TelemetryGenerator(seed=100, event_rate_hz=5.0)
    gen_b = TelemetryGenerator(seed=200, event_rate_hz=5.0)

    events_a = list(gen_a.generate_events(duration_sec=1.0))
    events_b = list(gen_b.generate_events(duration_sec=1.0))

    temperatures_a = [e.temperature for e in events_a]
    temperatures_b = [e.temperature for e in events_b]

    assert temperatures_a != temperatures_b


def test_monotonic_sequence_numbers():
    """Verifies that sequence numbers increase monotonically for each asset."""
    gen = TelemetryGenerator(seed=42, event_rate_hz=5.0)
    events = list(gen.generate_events(duration_sec=2.0))

    per_asset_seqs = {}
    for e in events:
        if e.asset_id not in per_asset_seqs:
            per_asset_seqs[e.asset_id] = []
        per_asset_seqs[e.asset_id].append(e.sequence_number)

    for asset_id, seqs in per_asset_seqs.items():
        assert seqs == list(range(len(seqs))), f"Sequence numbers non-monotonic for {asset_id}"
