import json
import logging
from dataclasses import dataclass
from datetime import date
from functools import lru_cache
from pathlib import Path
from typing import Any

from app.config import get_settings
from app.services.historical_features import (
    HISTORICAL_ML_FEATURE_COLUMNS,
    build_historical_ml_features,
    load_historical_flight_days,
)
from app.services.historical_probability import user_probability_from_model_score


logger = logging.getLogger("flyforecast.historical_ml")


@dataclass(frozen=True)
class HistoricalMlPrediction:
    available: bool
    probability_flight: float | None = None
    threshold: float | None = None
    raw_probability_flight: float | None = None
    raw_threshold: float | None = None
    model_version: str | None = None
    data_version: str | None = None
    model_name: str | None = None
    reason: str | None = None


@dataclass(frozen=True)
class HistoricalMlBundle:
    model: Any
    metadata: dict[str, Any]


@lru_cache(maxsize=1)
def _load_bundle() -> HistoricalMlBundle | None:
    settings = get_settings()
    if not settings.historical_ml_enabled:
        return None

    model_path = Path(settings.historical_ml_model_path)
    metadata_path = Path(settings.historical_ml_metadata_path)
    if not model_path.exists() or not metadata_path.exists():
        logger.warning(
            "historical_ml_artifact_missing model_path=%s metadata_path=%s",
            model_path,
            metadata_path,
        )
        return None

    try:
        import joblib

        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        model = joblib.load(model_path)
    except Exception as exc:  # pragma: no cover - defensive production fallback
        logger.exception("historical_ml_load_failed error=%s", exc)
        return None

    return HistoricalMlBundle(model=model, metadata=metadata)


def clear_historical_ml_cache() -> None:
    _load_bundle.cache_clear()


def _select_model_artifact(
    artifact: Any,
    metadata: dict[str, Any],
    horizon_days: int,
) -> tuple[Any, list[str], float, str]:
    if isinstance(artifact, dict):
        mode = artifact.get("mode")
        if mode == "segmented":
            segments = artifact.get("segments") or []
            if not segments:
                raise ValueError("Historical ML artifact has no segments.")

            selected = None
            for segment in segments:
                if int(segment["min_horizon"]) <= horizon_days <= int(segment["max_horizon"]):
                    selected = segment
                    break
            if selected is None:
                selected = min(
                    segments,
                    key=lambda segment: min(
                        abs(horizon_days - int(segment["min_horizon"])),
                        abs(horizon_days - int(segment["max_horizon"])),
                    ),
                )

            return (
                selected["model"],
                list(selected.get("feature_columns") or metadata.get("feature_columns") or HISTORICAL_ML_FEATURE_COLUMNS),
                float(selected.get("threshold", 0.5)),
                str(selected.get("model_name") or selected.get("name") or "segmented"),
            )

        if mode == "global":
            return (
                artifact["model"],
                list(artifact.get("feature_columns") or metadata.get("feature_columns") or HISTORICAL_ML_FEATURE_COLUMNS),
                float(artifact.get("threshold", metadata.get("threshold", 0.5))),
                str(artifact.get("model_name") or metadata.get("model_name") or "global"),
            )

    return (
        artifact,
        list(metadata.get("feature_columns") or HISTORICAL_ML_FEATURE_COLUMNS),
        float(metadata.get("threshold", 0.5)),
        str(metadata.get("model_name") or "legacy"),
    )


def predict_historical_ml(target_date: date, as_of_date: date) -> HistoricalMlPrediction:
    settings = get_settings()
    if not settings.historical_ml_enabled:
        return HistoricalMlPrediction(available=False, reason="historical_ml_disabled")

    bundle = _load_bundle()
    if bundle is None:
        return HistoricalMlPrediction(available=False, reason="historical_ml_artifact_unavailable")

    rows = load_historical_flight_days(settings.flyforecast_dataset_path)
    if not rows:
        return HistoricalMlPrediction(available=False, reason="historical_dataset_unavailable")

    horizon_days = max((target_date - as_of_date).days, 0)
    try:
        model, feature_columns, threshold, model_name = _select_model_artifact(
            bundle.model,
            bundle.metadata,
            horizon_days=horizon_days,
        )
    except Exception as exc:  # pragma: no cover - defensive production fallback
        logger.exception("historical_ml_artifact_select_failed error=%s", exc)
        return HistoricalMlPrediction(available=False, reason="historical_ml_artifact_invalid")

    features = build_historical_ml_features(
        target_date=target_date,
        as_of_date=as_of_date,
        rows=rows,
    )
    import pandas as pd

    frame = pd.DataFrame([{column: features[column] for column in feature_columns}])
    raw_probability = float(model.predict_proba(frame)[0, 1])
    probability = user_probability_from_model_score(raw_probability, threshold)

    return HistoricalMlPrediction(
        available=True,
        probability_flight=probability,
        threshold=0.5,
        raw_probability_flight=round(min(max(raw_probability, 0.0), 1.0), 4),
        raw_threshold=threshold,
        model_version=str(bundle.metadata.get("model_version") or "historical-ml"),
        data_version=str(bundle.metadata.get("data_version") or "historical-ml-data"),
        model_name=model_name,
    )
