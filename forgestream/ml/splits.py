"""Chronological dataset splitting and class imbalance mitigation."""

from typing import Tuple, Dict, Any, List, Optional
import numpy as np
import pandas as pd
from forgestream.ml.config import MLDatasetConfig


def split_by_complete_runs(
    df: pd.DataFrame,
    train_ratio: float = 0.70,
    val_ratio: float = 0.15,
    test_ratio: float = 0.15,
    run_col: str = "run_id",
    random_seed: int = 42,
) -> pd.DataFrame:
    """Split dataset at the complete asset lifecycle/run level.

    Assigns whole runs to train (70%), val (15%), or test (15%) partitions.
    Ensures held-out evaluation on completely unseen asset trajectories while
    providing training partitions with complete degradation-to-failure lifecycles.
    """
    df = df.copy()
    unique_runs = list(df[run_col].unique())
    n_runs = len(unique_runs)

    # Deterministic run assignment ensuring balanced representation
    rng = np.random.default_rng(random_seed)
    shuffled_runs = list(unique_runs)
    rng.shuffle(shuffled_runs)

    n_train = int(np.round(n_runs * train_ratio))
    n_val = int(np.round(n_runs * val_ratio))
    n_test = n_runs - n_train - n_val

    train_runs = set(shuffled_runs[:n_train])
    val_runs = set(shuffled_runs[n_train:n_train + n_val])
    test_runs = set(shuffled_runs[n_train + n_val:])

    def get_split(run_id: str) -> str:
        if run_id in train_runs:
            return "train"
        elif run_id in val_runs:
            return "val"
        else:
            return "test"

    df["split"] = df[run_col].map(get_split)
    return df


def split_chronologically_by_run(
    df: pd.DataFrame,
    train_ratio: float = 0.70,
    val_ratio: float = 0.15,
    test_ratio: float = 0.15,
    run_col: str = "run_id",
    time_col: str = "timestamp",
) -> pd.DataFrame:
    """Assign chronological train/validation/test split tags per asset run.

    For each unique run_id, events are sorted monotonically by time.
    The first train_ratio (70%) are tagged 'train', the subsequent val_ratio (15%)
    are tagged 'val', and the final test_ratio (15%) are tagged 'test'.
    """
    df = df.copy()
    df["split"] = "train"

    for run_id, group in df.groupby(run_col):
        indices = group.sort_values(time_col).index
        n = len(indices)
        train_end = int(n * train_ratio)
        val_end = int(n * (train_ratio + val_ratio))

        train_idx = indices[:train_end]
        val_idx = indices[train_end:val_end]
        test_idx = indices[val_end:]

        df.loc[train_idx, "split"] = "train"
        df.loc[val_idx, "split"] = "val"
        df.loc[test_idx, "split"] = "test"

    return df


def calculate_train_class_weights(
    train_df: pd.DataFrame,
    label_col: str = "label_failure",
) -> Dict[int, float]:
    """Calculate balanced class weights strictly on the training partition.

    Formula:
      w_k = N_train / (2 * N_k)
    """
    labels = train_df[label_col].values
    total_samples = len(labels)
    n_pos = int(np.sum(labels == 1.0))
    n_neg = total_samples - n_pos

    if n_pos == 0:
        w1 = 1.0
        w0 = 1.0
    else:
        w0 = total_samples / (2.0 * max(1, n_neg))
        w1 = total_samples / (2.0 * max(1, n_pos))

    return {0: float(w0), 1: float(w1)}


def add_sample_weights(
    df: pd.DataFrame,
    class_weights: Dict[int, float],
    label_col: str = "label_failure",
    weight_col: str = "sample_weight",
) -> pd.DataFrame:
    """Attach sample weight column based on class weights computed on train split."""
    df = df.copy()
    df[weight_col] = df[label_col].map(lambda x: class_weights.get(int(x), 1.0))
    return df
