"""Benchmark optimization on CircularSensorBuffer and State serialization."""

import time
import json
from collections import deque
import numpy as np


class OldCircularBuffer:
    def __init__(self, capacity: int = 120):
        self.capacity = capacity
        self.buffer = deque(maxlen=capacity)

    def append(self, timestamp: float, value: float) -> None:
        self.buffer.append((timestamp, float(value)))

    def to_dict(self):
        return {"capacity": self.capacity, "samples": list(self.buffer)}

    @classmethod
    def from_dict(cls, data):
        buf = cls(capacity=data.get("capacity", 120))
        for ts, val in data.get("samples", []):
            buf.append(ts, val)
        return buf


class OptimizedCircularBuffer:
    __slots__ = ("capacity", "buffer")

    def __init__(self, capacity: int = 120):
        self.capacity = capacity
        self.buffer = deque(maxlen=capacity)

    def append(self, timestamp: float, value: float) -> None:
        self.buffer.append((timestamp, float(value)))

    def get_values(self):
        return [val for _, val in self.buffer]

    def get_time_series(self):
        return list(self.buffer)

    def get_window(self, window_sec: float, current_timestamp: float):
        cutoff = current_timestamp - window_sec
        return [(ts, val) for ts, val in self.buffer if ts >= cutoff]

    def __len__(self) -> int:
        return len(self.buffer)

    def to_dict(self):
        return {"capacity": self.capacity, "samples": list(self.buffer)}

    @classmethod
    def from_dict(cls, data):
        buf = cls.__new__(cls)
        buf.capacity = data.get("capacity", 120)
        buf.buffer = deque(data.get("samples", ()), maxlen=buf.capacity)
        return buf


def test_buffer_perf():
    b_old = OldCircularBuffer(120)
    for i in range(120):
        b_old.append(100.0 + i, 50.0 + i)
    d = b_old.to_dict()

    t0 = time.perf_counter()
    for _ in range(50000):
        _ = OldCircularBuffer.from_dict(d)
    t1 = time.perf_counter()
    print(f"Old from_dict 50k calls: {t1 - t0:.4f}s")

    t0 = time.perf_counter()
    for _ in range(50000):
        _ = OptimizedCircularBuffer.from_dict(d)
    t1 = time.perf_counter()
    print(f"Optimized from_dict 50k calls: {t1 - t0:.4f}s")


if __name__ == "__main__":
    test_buffer_perf()
