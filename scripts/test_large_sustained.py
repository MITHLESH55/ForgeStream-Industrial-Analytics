"""Measure sustained large-volume throughput."""

import time
import json
import numpy as np
import sys
import os

sys.path.insert(0, "/opt/forgestream")
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from scripts.test_state_update_freq import run_test

if __name__ == "__main__":
    print("Testing 20,000 events with parallelism=4:")
    run_test(update_interval=10, num_events=20000, parallelism=4)
