"""
Pytest fixtures for ForgeStream test suite.
Provides isolated temporary directories, seeds, sample events, and catalog fixtures.
"""

import os
import tempfile
import pytest
from pathlib import Path
from forgestream.simulator.generator import TelemetryGenerator
from forgestream.simulator.asset_models import get_default_asset_list
from forgestream.schemas.telemetry_schema import ScenarioID, TelemetryEvent


@pytest.fixture
def temp_data_dir():
    """Provides a clean temporary directory for test storage artifacts."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        yield Path(tmp_dir)


@pytest.fixture
def sample_generator():
    """Provides a deterministic generator with seed 42."""
    return TelemetryGenerator(seed=42, event_rate_hz=5.0)


@pytest.fixture
def sample_events(sample_generator):
    """Generates a batch of 25 deterministic events across all 5 assets."""
    return list(sample_generator.generate_events(duration_sec=1.0))
