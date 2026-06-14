import argparse
import csv
import itertools
import json
import sys
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Callable

import joblib
import pandas as pd
from sklearn.ensemble import ExtraTreesClassifier, HistGradientBoostingClassifier, RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    balanced_accuracy_score,
    brier_score_loss,
    f1_score,
    roc_auc_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "backend"))

from app.services.historical_features import (  # noqa: E402
    HISTORICAL_ML_FEATURE_COLUMNS,
    HistoricalFlightDay,
    build_historical_ml_features,
    load_historical_flight_days,
)


MODEL_VERSION = "historical-ml-v2-2026-06-14"
DATA_VERSION = "historical-only-as-of-backtest-v2-2026-06-14"
DEFAULT_HORIZONS = [16, 21, 30, 45, 60, 90, 180, 365]
DEFAULT_SEGMENTS = [
    {"name": "short_16_30", "min_horizon": 16, "max_horizon": 30},
    {"name": "medium_31_90", "min_horizon": 31, "max_horizon": 90},
    {"name": "long_91_365", "min_horizon": 91, "max_horizon": 365},
]
CAT_FEATURE_COLUMNS = ["month", "day", "month_decade", "season_index", "day_of_week", "is_weekend"]


@dataclass(frozen=True)
class ModelSpec:
    name: str
    factory: Callable[[], object]
    family: str
    fit_params: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SelectedModel:
    segment_name: str
    min_horizon: int
    max_horizon: int
    spec: ModelSpec
    threshold: float
    cv_score: float
    valid_metrics: dict[str, float | int]
    test_metrics: dict[str, float | int]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train the production climate-historical model.")
    parser.add_argument("--dataset", default="data/processed/dataset_daily_flights_historical_only.csv")
    parser.add_argument("--model-output", default="backend/app/model_artifacts/historical_model_v1.joblib")
    parser.add_argument("--metadata-output", default="backend/app/model_artifacts/historical_model_v1.json")
    parser.add_argument(
        "--training-output",
        default="data/interim/training/historical_ml_production/historical_ml_training_rows.csv",
    )
    parser.add_argument(
        "--metrics-output",
        default="data/interim/training/historical_ml_production/historical_ml_metrics.csv",
    )
    parser.add_argument("--min-past-rows", type=int, default=60)
    parser.add_argument("--horizons", default=",".join(str(item) for item in DEFAULT_HORIZONS))
    parser.add_argument(
        "--families",
        default="sklearn,catboost,lightgbm",
        help="Comma-separated families: sklearn, catboost, lightgbm.",
    )
    parser.add_argument(
        "--search-level",
        choices=("quick", "standard", "wide"),
        default="standard",
        help="Controls the amount of hyperparameter candidates.",
    )
    parser.add_argument(
        "--deployment-mode",
        choices=("global", "segmented", "auto"),
        default="auto",
        help="global = one model; segmented = separate horizon models; auto = choose by temporal CV.",
    )
    parser.add_argument("--temporal-folds", type=int, default=4)
    parser.add_argument(
        "--max-candidates-per-family",
        type=int,
        default=40,
        help="Deterministic cap per model family, useful for CatBoost/LightGBM grids.",
    )
    return parser.parse_args()


def parse_horizons(value: str) -> list[int]:
    horizons = sorted({int(part.strip()) for part in value.split(",") if part.strip()})
    return [item for item in horizons if item > 0]


def parse_families(value: str) -> set[str]:
    return {part.strip().lower() for part in value.split(",") if part.strip()}


def flight_day_value(row: HistoricalFlightDay) -> int:
    value = row.is_completed
    if value is None:
        raise ValueError(f"Unsupported flight day status: {row}")
    return value


def split_for_target_date(target_date: date, sorted_dates: list[date]) -> str:
    index = sorted_dates.index(target_date)
    train_end = int(len(sorted_dates) * 0.70)
    valid_end = int(len(sorted_dates) * 0.85)
    if index < train_end:
        return "train"
    if index < valid_end:
        return "valid"
    return "test"


def build_training_rows(
    rows: list[HistoricalFlightDay],
    horizons: list[int],
    min_past_rows: int,
) -> list[dict[str, object]]:
    binary_rows = [row for row in rows if row.is_completed is not None]
    sorted_dates = sorted(row.date for row in binary_rows)
    rows_by_date = {row.date: row for row in binary_rows}
    result: list[dict[str, object]] = []

    for target_date in sorted_dates:
        target_value = flight_day_value(rows_by_date[target_date])
        for horizon_days in horizons:
            as_of_date = target_date - timedelta(days=horizon_days)
            past_rows = [row for row in binary_rows if row.date <= as_of_date]
            if len(past_rows) < min_past_rows:
                continue

            features = build_historical_ml_features(
                target_date=target_date,
                as_of_date=as_of_date,
                rows=binary_rows,
            )
            result.append(
                {
                    "as_of_date": as_of_date.isoformat(),
                    "target_date": target_date.isoformat(),
                    "dataset_split": split_for_target_date(target_date, sorted_dates),
                    "is_flight_completed": target_value,
                    **features,
                }
            )

    return result


def product_score(y_true: list[int], y_probability: list[float], threshold: float) -> dict[str, float | int]:
    y_pred = [1 if probability >= threshold else 0 for probability in y_probability]
    try:
        roc_auc = roc_auc_score(y_true, y_probability)
    except ValueError:
        roc_auc = 0.5

    brier = brier_score_loss(y_true, y_probability)
    f1_cancelled = f1_score(y_true, y_pred, pos_label=0, zero_division=0)
    f1_completed = f1_score(y_true, y_pred, pos_label=1, zero_division=0)
    balanced = balanced_accuracy_score(y_true, y_pred)
    score = (
        0.30 * roc_auc
        + 0.25 * balanced
        + 0.15 * f1_cancelled
        + 0.15 * f1_completed
        + 0.15 * (1 - brier)
    )
    false_yes = sum(1 for truth, pred in zip(y_true, y_pred) if truth == 0 and pred == 1)
    false_no = sum(1 for truth, pred in zip(y_true, y_pred) if truth == 1 and pred == 0)

    return {
        "threshold": round(threshold, 4),
        "score": round(score, 6),
        "accuracy": round(accuracy_score(y_true, y_pred), 6),
        "balanced_accuracy": round(balanced, 6),
        "roc_auc": round(roc_auc, 6),
        "brier": round(brier, 6),
        "f1_cancelled": round(f1_cancelled, 6),
        "f1_completed": round(f1_completed, 6),
        "false_yes": false_yes,
        "false_no": false_no,
    }


def tune_threshold(y_true: list[int], y_probability: list[float]) -> tuple[float, dict[str, float | int]]:
    best_threshold = 0.5
    best_metrics: dict[str, float | int] | None = None
    for value in range(20, 81):
        threshold = value / 100
        metrics = product_score(y_true, y_probability, threshold)
        if best_metrics is None or float(metrics["score"]) > float(best_metrics["score"]):
            best_threshold = threshold
            best_metrics = metrics
    if best_metrics is None:
        raise ValueError("No threshold candidates were evaluated.")
    return best_threshold, best_metrics


def limited_grid(
    grid: dict[str, list[Any]],
    max_candidates: int,
) -> list[dict[str, Any]]:
    keys = list(grid)
    all_params = [dict(zip(keys, values)) for values in itertools.product(*(grid[key] for key in keys))]
    if len(all_params) <= max_candidates:
        return all_params

    step = max(len(all_params) / max_candidates, 1)
    selected_indexes = sorted({min(int(index * step), len(all_params) - 1) for index in range(max_candidates)})
    return [all_params[index] for index in selected_indexes]


def sklearn_specs(search_level: str, max_candidates: int) -> list[ModelSpec]:
    specs: list[ModelSpec] = [
        ModelSpec(
            name="logistic_regression",
            family="sklearn",
            factory=lambda: Pipeline(
                steps=[
                    ("scale", StandardScaler()),
                    (
                        "model",
                        LogisticRegression(
                            max_iter=2000,
                            class_weight="balanced",
                            random_state=42,
                        ),
                    ),
                ]
            ),
        ),
        ModelSpec(
            name="hist_gradient_boosting_base",
            family="sklearn",
            factory=lambda: HistGradientBoostingClassifier(
                max_iter=200,
                learning_rate=0.04,
                max_leaf_nodes=15,
                l2_regularization=0.1,
                random_state=42,
            ),
        ),
    ]

    extra_trees_grid = {
        "n_estimators": [400, 800] if search_level == "quick" else [400, 800, 1200],
        "max_depth": [4, 6, 8, 10] if search_level == "quick" else [4, 6, 8, 10, None],
        "min_samples_leaf": [5, 10, 15, 25] if search_level != "wide" else [5, 10, 15, 25, 40],
        "max_features": ["sqrt", 0.5, 0.8, None],
        "class_weight": ["balanced", "balanced_subsample"],
    }
    for params in limited_grid(extra_trees_grid, max_candidates=max_candidates):
        name = "extra_trees_" + "_".join(f"{key}-{value}" for key, value in params.items())
        specs.append(
            ModelSpec(
                name=name,
                family="sklearn",
                factory=lambda params=params: ExtraTreesClassifier(
                    **params,
                    random_state=42,
                    n_jobs=-1,
                ),
            )
        )

    rf_grid = {
        "n_estimators": [400, 800],
        "max_depth": [4, 6, 8, 10, None],
        "min_samples_leaf": [5, 10, 15, 25],
        "max_features": ["sqrt", 0.5, 0.8],
        "class_weight": ["balanced_subsample"],
    }
    rf_limit = max(8, max_candidates // 2) if search_level != "wide" else max_candidates
    for params in limited_grid(rf_grid, max_candidates=rf_limit):
        name = "random_forest_" + "_".join(f"{key}-{value}" for key, value in params.items())
        specs.append(
            ModelSpec(
                name=name,
                family="sklearn",
                factory=lambda params=params: RandomForestClassifier(
                    **params,
                    random_state=42,
                    n_jobs=-1,
                ),
            )
        )

    return specs


def catboost_specs(search_level: str, max_candidates: int) -> list[ModelSpec]:
    try:
        from catboost import CatBoostClassifier
    except ImportError:
        print("CatBoost is not installed; skipping catboost family.")
        return []

    grid = {
        "iterations": [300, 600] if search_level == "quick" else [300, 600, 900],
        "learning_rate": [0.02, 0.04, 0.06, 0.08],
        "depth": [3, 4, 5, 6] if search_level != "wide" else [3, 4, 5, 6, 7, 8],
        "l2_leaf_reg": [1, 3, 10],
        "class_weights": [None, [1.3, 1.0], [1.0, 1.3]],
    }
    specs: list[ModelSpec] = []
    for params in limited_grid(grid, max_candidates=max_candidates):
        compact = {key: value for key, value in params.items() if value is not None}
        name = "catboost_" + "_".join(f"{key}-{value}" for key, value in compact.items())
        specs.append(
            ModelSpec(
                name=name,
                family="catboost",
                factory=lambda compact=compact: CatBoostClassifier(
                    **compact,
                    loss_function="Logloss",
                    eval_metric="Logloss",
                    random_seed=42,
                    verbose=False,
                    allow_writing_files=False,
                ),
                fit_params={"cat_features": CAT_FEATURE_COLUMNS},
            )
        )
    return specs


def lightgbm_specs(search_level: str, max_candidates: int) -> list[ModelSpec]:
    try:
        from lightgbm import LGBMClassifier
    except ImportError:
        print("LightGBM is not installed; skipping lightgbm family.")
        return []

    grid = {
        "n_estimators": [300, 600] if search_level == "quick" else [300, 600, 900],
        "learning_rate": [0.02, 0.04, 0.06, 0.08],
        "max_depth": [3, 4, 5, 6, -1],
        "num_leaves": [7, 15, 31],
        "min_child_samples": [10, 20, 40],
        "subsample": [0.8, 1.0],
        "colsample_bytree": [0.8, 1.0],
        "class_weight": [None, "balanced"],
    }
    specs: list[ModelSpec] = []
    for params in limited_grid(grid, max_candidates=max_candidates):
        compact = {key: value for key, value in params.items() if value is not None}
        name = "lightgbm_" + "_".join(f"{key}-{value}" for key, value in compact.items())
        specs.append(
            ModelSpec(
                name=name,
                family="lightgbm",
                factory=lambda compact=compact: LGBMClassifier(
                    **compact,
                    random_state=42,
                    n_jobs=-1,
                    verbosity=-1,
                ),
            )
        )
    return specs


def model_specs(families: set[str], search_level: str, max_candidates: int) -> list[ModelSpec]:
    specs: list[ModelSpec] = []
    if "sklearn" in families:
        specs.extend(sklearn_specs(search_level, max_candidates=max_candidates))
    if "catboost" in families:
        specs.extend(catboost_specs(search_level, max_candidates=max_candidates))
    if "lightgbm" in families:
        specs.extend(lightgbm_specs(search_level, max_candidates=max_candidates))
    if not specs:
        raise RuntimeError("No model candidates are available. Check --families and installed packages.")
    return specs


def fit_model(spec: ModelSpec, x_train: pd.DataFrame, y_train: list[int]) -> object:
    model = spec.factory()
    model.fit(x_train, y_train, **spec.fit_params)
    return model


def probability_from_model(model: object, frame: pd.DataFrame) -> list[float]:
    if hasattr(model, "predict_proba"):
        return [float(item) for item in model.predict_proba(frame)[:, 1]]
    decision = model.decision_function(frame)
    return [float(1 / (1 + pow(2.718281828, -item))) for item in decision]


def temporal_folds(data: pd.DataFrame, fold_count: int) -> list[tuple[list[str], list[str]]]:
    dates = sorted(data["target_date"].unique())
    if len(dates) < 20:
        return []

    min_train_size = max(int(len(dates) * 0.45), 10)
    validation_dates = dates[min_train_size:]
    if not validation_dates:
        return []

    fold_count = max(1, min(fold_count, len(validation_dates)))
    fold_size = max(len(validation_dates) // fold_count, 1)
    folds: list[tuple[list[str], list[str]]] = []
    for fold_index in range(fold_count):
        start = fold_index * fold_size
        end = len(validation_dates) if fold_index == fold_count - 1 else (fold_index + 1) * fold_size
        valid_dates = validation_dates[start:end]
        if not valid_dates:
            continue
        first_valid = valid_dates[0]
        train_dates = [item for item in dates if item < first_valid]
        if train_dates:
            folds.append((train_dates, list(valid_dates)))
    return folds


def evaluate_temporal_cv(
    data: pd.DataFrame,
    spec: ModelSpec,
    feature_columns: list[str],
    fold_count: int,
) -> dict[str, float | int]:
    folds = temporal_folds(data, fold_count=fold_count)
    if not folds:
        return {"cv_score": 0.0, "cv_folds": 0, "cv_threshold_mean": 0.5}

    scores: list[float] = []
    thresholds: list[float] = []
    metrics_by_key: dict[str, list[float]] = {}
    for train_dates, valid_dates in folds:
        train = data[data["target_date"].isin(train_dates)]
        valid = data[data["target_date"].isin(valid_dates)]
        if train["is_flight_completed"].nunique() < 2 or valid["is_flight_completed"].nunique() < 2:
            continue

        model = fit_model(
            spec,
            train[feature_columns],
            train["is_flight_completed"].astype(int).tolist(),
        )
        probabilities = probability_from_model(model, valid[feature_columns])
        threshold, metrics = tune_threshold(valid["is_flight_completed"].astype(int).tolist(), probabilities)
        scores.append(float(metrics["score"]))
        thresholds.append(threshold)
        for key, value in metrics.items():
            if isinstance(value, (int, float)):
                metrics_by_key.setdefault(key, []).append(float(value))

    if not scores:
        return {"cv_score": 0.0, "cv_folds": 0, "cv_threshold_mean": 0.5}

    averaged = {
        f"cv_{key}_mean": round(sum(values) / len(values), 6)
        for key, values in metrics_by_key.items()
        if values
    }
    return {
        "cv_score": round(sum(scores) / len(scores), 6),
        "cv_folds": len(scores),
        "cv_threshold_mean": round(sum(thresholds) / len(thresholds), 4),
        **averaged,
    }


def evaluate_on_split(
    spec: ModelSpec,
    train: pd.DataFrame,
    valid: pd.DataFrame,
    test: pd.DataFrame,
    feature_columns: list[str],
) -> tuple[float, dict[str, float | int], dict[str, float | int]]:
    model = fit_model(spec, train[feature_columns], train["is_flight_completed"].astype(int).tolist())
    valid_probability = probability_from_model(model, valid[feature_columns])
    threshold, valid_metrics = tune_threshold(valid["is_flight_completed"].astype(int).tolist(), valid_probability)
    test_probability = probability_from_model(model, test[feature_columns])
    test_metrics = product_score(test["is_flight_completed"].astype(int).tolist(), test_probability, threshold)
    return threshold, valid_metrics, test_metrics


def filter_horizon(data: pd.DataFrame, min_horizon: int, max_horizon: int) -> pd.DataFrame:
    return data[(data["horizon_days"] >= min_horizon) & (data["horizon_days"] <= max_horizon)].copy()


def select_model_for_segment(
    data: pd.DataFrame,
    specs: list[ModelSpec],
    feature_columns: list[str],
    segment_name: str,
    min_horizon: int,
    max_horizon: int,
    fold_count: int,
) -> tuple[SelectedModel, list[dict[str, object]]]:
    segment_data = filter_horizon(data, min_horizon=min_horizon, max_horizon=max_horizon)
    train = segment_data[segment_data["dataset_split"] == "train"].copy()
    valid = segment_data[segment_data["dataset_split"] == "valid"].copy()
    test = segment_data[segment_data["dataset_split"] == "test"].copy()
    if train.empty or valid.empty or test.empty:
        raise RuntimeError(f"Segment {segment_name} has empty train/valid/test data.")

    selection_data = segment_data[segment_data["dataset_split"].isin({"train", "valid"})].copy()
    metric_rows: list[dict[str, object]] = []
    for spec in specs:
        cv_metrics = evaluate_temporal_cv(
            selection_data,
            spec=spec,
            feature_columns=feature_columns,
            fold_count=fold_count,
        )
        threshold, valid_metrics, test_metrics = evaluate_on_split(
            spec=spec,
            train=train,
            valid=valid,
            test=test,
            feature_columns=feature_columns,
        )
        metric_rows.append(
            {
                "strategy": "candidate",
                "segment": segment_name,
                "min_horizon": min_horizon,
                "max_horizon": max_horizon,
                "model_name": spec.name,
                "family": spec.family,
                "selected_threshold": threshold,
                **cv_metrics,
                **{f"valid_{key}": value for key, value in valid_metrics.items()},
                **{f"test_{key}": value for key, value in test_metrics.items()},
            }
        )

    selected_row = max(metric_rows, key=lambda row: (float(row["cv_score"]), float(row["valid_score"])))
    selected_spec = next(spec for spec in specs if spec.name == selected_row["model_name"])
    selected = SelectedModel(
        segment_name=segment_name,
        min_horizon=min_horizon,
        max_horizon=max_horizon,
        spec=selected_spec,
        threshold=float(selected_row["selected_threshold"]),
        cv_score=float(selected_row["cv_score"]),
        valid_metrics={key.replace("valid_", ""): value for key, value in selected_row.items() if key.startswith("valid_")},
        test_metrics={key.replace("test_", ""): value for key, value in selected_row.items() if key.startswith("test_")},
    )
    return selected, metric_rows


def baseline_metric_rows(data: pd.DataFrame) -> list[dict[str, object]]:
    valid = data[data["dataset_split"] == "valid"].copy()
    test = data[data["dataset_split"] == "test"].copy()
    y_valid = valid["is_flight_completed"].astype(int).tolist()
    y_test = test["is_flight_completed"].astype(int).tolist()
    old_valid_probability = valid["history_combined_probability"].astype(float).tolist()
    old_test_probability = test["history_combined_probability"].astype(float).tolist()
    old_threshold, old_valid_metrics = tune_threshold(y_valid, old_valid_probability)

    return [
        {
            "strategy": "baseline",
            "segment": "all",
            "model_name": "current_history_formula_threshold_45",
            "family": "formula",
            "selected_threshold": 0.45,
            **{f"test_{key}": value for key, value in product_score(y_test, old_test_probability, 0.45).items()},
        },
        {
            "strategy": "baseline",
            "segment": "all",
            "model_name": "current_history_formula_tuned_threshold",
            "family": "formula",
            "selected_threshold": old_threshold,
            **{f"valid_{key}": value for key, value in old_valid_metrics.items()},
            **{f"test_{key}": value for key, value in product_score(y_test, old_test_probability, old_threshold).items()},
        },
    ]


def aggregate_segmented_test_metrics(
    data: pd.DataFrame,
    selected_segments: list[SelectedModel],
    feature_columns: list[str],
) -> dict[str, float | int]:
    probabilities: list[float] = []
    truths: list[int] = []
    decisions: list[int] = []

    for selected in selected_segments:
        segment_data = filter_horizon(data, selected.min_horizon, selected.max_horizon)
        train = segment_data[segment_data["dataset_split"].isin({"train", "valid"})].copy()
        test = segment_data[segment_data["dataset_split"] == "test"].copy()
        if test.empty:
            continue

        model = fit_model(
            selected.spec,
            train[feature_columns],
            train["is_flight_completed"].astype(int).tolist(),
        )
        segment_probability = probability_from_model(model, test[feature_columns])
        probabilities.extend(segment_probability)
        segment_truth = test["is_flight_completed"].astype(int).tolist()
        truths.extend(segment_truth)
        decisions.extend([1 if probability >= selected.threshold else 0 for probability in segment_probability])

    if not truths:
        return {"score": 0.0}

    try:
        roc_auc = roc_auc_score(truths, probabilities)
    except ValueError:
        roc_auc = 0.5
    brier = brier_score_loss(truths, probabilities)
    f1_cancelled = f1_score(truths, decisions, pos_label=0, zero_division=0)
    f1_completed = f1_score(truths, decisions, pos_label=1, zero_division=0)
    balanced = balanced_accuracy_score(truths, decisions)
    score = (
        0.30 * roc_auc
        + 0.25 * balanced
        + 0.15 * f1_cancelled
        + 0.15 * f1_completed
        + 0.15 * (1 - brier)
    )
    return {
        "score": round(score, 6),
        "accuracy": round(accuracy_score(truths, decisions), 6),
        "balanced_accuracy": round(balanced, 6),
        "roc_auc": round(roc_auc, 6),
        "brier": round(brier, 6),
        "f1_cancelled": round(f1_cancelled, 6),
        "f1_completed": round(f1_completed, 6),
        "false_yes": sum(1 for truth, pred in zip(truths, decisions) if truth == 0 and pred == 1),
        "false_no": sum(1 for truth, pred in zip(truths, decisions) if truth == 1 and pred == 0),
    }


def train_deployment_artifact(
    data: pd.DataFrame,
    selected_segments: list[SelectedModel],
    feature_columns: list[str],
    mode: str,
) -> dict[str, Any]:
    if mode == "global":
        selected = selected_segments[0]
        model = fit_model(
            selected.spec,
            data[feature_columns],
            data["is_flight_completed"].astype(int).tolist(),
        )
        return {
            "mode": "global",
            "model": model,
            "model_name": selected.spec.name,
            "family": selected.spec.family,
            "threshold": selected.threshold,
            "feature_columns": feature_columns,
        }

    segment_artifacts: list[dict[str, Any]] = []
    for selected in selected_segments:
        segment_data = filter_horizon(data, selected.min_horizon, selected.max_horizon)
        model = fit_model(
            selected.spec,
            segment_data[feature_columns],
            segment_data["is_flight_completed"].astype(int).tolist(),
        )
        segment_artifacts.append(
            {
                "name": selected.segment_name,
                "min_horizon": selected.min_horizon,
                "max_horizon": selected.max_horizon,
                "model": model,
                "model_name": selected.spec.name,
                "family": selected.spec.family,
                "threshold": selected.threshold,
                "feature_columns": feature_columns,
            }
        )
    return {"mode": "segmented", "segments": segment_artifacts}


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = sorted({key for row in rows for key in row})
    with path.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> None:
    args = parse_args()
    horizons = parse_horizons(args.horizons)
    families = parse_families(args.families)
    rows = load_historical_flight_days(args.dataset)
    training_rows = build_training_rows(rows, horizons=horizons, min_past_rows=args.min_past_rows)
    if not training_rows:
        raise RuntimeError("No training rows were built.")

    training_output = Path(args.training_output)
    write_csv(training_output, training_rows)

    data = pd.DataFrame(training_rows)
    feature_columns = list(HISTORICAL_ML_FEATURE_COLUMNS)
    specs = model_specs(
        families=families,
        search_level=args.search_level,
        max_candidates=args.max_candidates_per_family,
    )

    metric_rows: list[dict[str, object]] = baseline_metric_rows(data)

    global_selected, global_rows = select_model_for_segment(
        data=data,
        specs=specs,
        feature_columns=feature_columns,
        segment_name="all",
        min_horizon=min(horizons),
        max_horizon=max(horizons),
        fold_count=args.temporal_folds,
    )
    metric_rows.extend(global_rows)

    segmented_selected: list[SelectedModel] = []
    segmented_cv_score = 0.0
    segmented_test_metrics: dict[str, float | int] = {"score": 0.0}
    if args.deployment_mode in {"segmented", "auto"}:
        for segment in DEFAULT_SEGMENTS:
            selected, rows_for_segment = select_model_for_segment(
                data=data,
                specs=specs,
                feature_columns=feature_columns,
                segment_name=str(segment["name"]),
                min_horizon=int(segment["min_horizon"]),
                max_horizon=int(segment["max_horizon"]),
                fold_count=args.temporal_folds,
            )
            segmented_selected.append(selected)
            metric_rows.extend(rows_for_segment)
        segmented_cv_score = sum(item.cv_score for item in segmented_selected) / len(segmented_selected)
        segmented_test_metrics = aggregate_segmented_test_metrics(
            data=data,
            selected_segments=segmented_selected,
            feature_columns=feature_columns,
        )
        metric_rows.append(
            {
                "strategy": "deployment_strategy",
                "segment": "segmented_all",
                "model_name": "segmented",
                "family": "mixed",
                "cv_score": round(segmented_cv_score, 6),
                **{f"test_{key}": value for key, value in segmented_test_metrics.items()},
            }
        )

    global_cv_score = global_selected.cv_score
    if args.deployment_mode == "global":
        deployment_mode = "global"
        deployment_selected = [global_selected]
    elif args.deployment_mode == "segmented":
        deployment_mode = "segmented"
        deployment_selected = segmented_selected
    elif segmented_selected and segmented_cv_score > global_cv_score:
        deployment_mode = "segmented"
        deployment_selected = segmented_selected
    else:
        deployment_mode = "global"
        deployment_selected = [global_selected]

    artifact = train_deployment_artifact(
        data=data,
        selected_segments=deployment_selected,
        feature_columns=feature_columns,
        mode=deployment_mode,
    )

    model_output = Path(args.model_output)
    model_output.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(artifact, model_output)

    metadata = {
        "model_version": MODEL_VERSION,
        "data_version": DATA_VERSION,
        "deployment_mode": deployment_mode,
        "feature_columns": feature_columns,
        "horizons_days": horizons,
        "segments": [
            {
                "name": selected.segment_name,
                "min_horizon": selected.min_horizon,
                "max_horizon": selected.max_horizon,
                "model_name": selected.spec.name,
                "family": selected.spec.family,
                "threshold": selected.threshold,
                "cv_score": round(selected.cv_score, 6),
                "valid_metrics": selected.valid_metrics,
                "test_metrics": selected.test_metrics,
            }
            for selected in deployment_selected
        ],
        "global_candidate": {
            "model_name": global_selected.spec.name,
            "family": global_selected.spec.family,
            "threshold": global_selected.threshold,
            "cv_score": round(global_selected.cv_score, 6),
            "test_metrics": global_selected.test_metrics,
        },
        "segmented_candidate": {
            "cv_score": round(segmented_cv_score, 6),
            "test_metrics": segmented_test_metrics,
            "segments": [
                {
                    "name": selected.segment_name,
                    "model_name": selected.spec.name,
                    "family": selected.spec.family,
                    "threshold": selected.threshold,
                    "cv_score": round(selected.cv_score, 6),
                    "test_metrics": selected.test_metrics,
                }
                for selected in segmented_selected
            ],
        },
        "min_past_rows": args.min_past_rows,
        "training_rows": int(len(data)),
        "target_rows": int(data["target_date"].nunique()),
        "target_date_min": str(data["target_date"].min()),
        "target_date_max": str(data["target_date"].max()),
        "split_summary": {
            split: {
                "rows": int(len(part)),
                "target_days": int(part["target_date"].nunique()),
                "completed_rate": round(float(part["is_flight_completed"].mean()), 6),
            }
            for split, part in data.groupby("dataset_split")
        },
        "notes": [
            "Feature rows use as_of_date cutoff and do not use facts between request date and target_date.",
            "Model selection uses temporal folds on train+valid dates; test metrics are reported separately.",
            "Deployment artifact is intentionally ignored by git and should be copied to the server directly.",
        ],
    }
    metadata_output = Path(args.metadata_output)
    metadata_output.parent.mkdir(parents=True, exist_ok=True)
    metadata_output.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")

    metrics_output = Path(args.metrics_output)
    write_csv(metrics_output, metric_rows)

    print(f"Built training rows: {len(training_rows)} -> {training_output}")
    print(f"Candidates checked: {len(specs)}")
    print(f"Deployment mode: {deployment_mode}")
    for selected in deployment_selected:
        print(
            "Selected "
            f"{selected.segment_name}: {selected.spec.name}, "
            f"threshold={selected.threshold:.2f}, cv_score={selected.cv_score:.3f}"
        )
    print(f"Model artifact: {model_output}")
    print(f"Metadata: {metadata_output}")
    print(f"Metrics: {metrics_output}")


if __name__ == "__main__":
    main()
