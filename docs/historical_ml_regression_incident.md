# Historical ML Regression Incident

Date opened: 2026-06-29

Status: root cause identified, production mitigation applied.

## Summary

On 2026-06-29 the long-horizon historical ML path started looking product-broken: manual checks across future dates mostly returned `no`, even when the older calendar/history estimate was neutral or positive. The runtime was not failing technically. The artifact loaded and returned predictions, but its output was much more pessimistic than expected.

Production mitigation: historical ML is disabled by default. Public long-horizon forecasts fall back to the old `climate_history` logic unless `HISTORICAL_ML_ENABLED=true` is explicitly set.

## Impact

- Affected surface: `/predict` for horizons beyond the weather forecast range, especially manual future-date searches.
- Near-horizon weather model and board-status guardrails were not the root issue.
- User-facing symptom: it became hard to find any positive long-horizon forecast, including dates out to late 2026 / early 2027.
- Product risk: the service stopped helping users identify likely good travel dates and instead behaved like a conservative cancellation detector.

## Known Facts

Fresh backup used for diagnosis:

- Backup manifest timestamp: `2026-06-29T12:06:38.221670+00:00`.
- Local archive name: `flyforecast_service_backup_20260629_120638.zip`.

Runtime facts from backup:

- `legacy_jsonl/prediction_logs.csv`: `2962` total rows.
- Long-horizon rows in JSONL (`horizon_days > 15`): `361`.
- Long-horizon forecast modes:
  - `historical_ml`: `224`
  - `climate_history`: `120`
  - blank/older rows: `17`
- Latest 200 long-horizon JSONL rows:
  - `model_version=historical-ml-v2-2026-06-14` for all 200.
  - decisions: `162 no`, `38 yes`.
- `postgres/prediction_events.csv` has a less extreme full-history mix for long horizons:
  - `210` long-horizon rows.
  - decisions: `110 yes`, `100 no`.
  - models: `historical-ml-v2-2026-06-14` for `130` rows, older baseline models for the rest.
- `forecast_monitor/predictions.csv` reached target dates up to `2026-09-27`; latest long monitor rows on 2026-06-29 were all `no` with probabilities around `0.16-0.29`.

Examples from 2026-06-29 manual/date-search attempts:

| target_date | horizon_days | ML probability | decision | old historical probability | similar days |
|---|---:|---:|---|---:|---:|
| 2026-07-16 | 17 | 0.2908 | no | 0.6301 | 71 |
| 2026-08-06 | 38 | 0.3028 | no | 0.5467 | 73 |
| 2026-08-13 | 45 | 0.2143 | no | 0.4744 | 76 |
| 2026-08-27 | 59 | 0.1558 | no | 0.5224 | 65 |
| 2026-09-01 | 64 | 0.2476 | no | 0.5167 | 58 |
| 2026-10-16 | 109 | 0.3109 | no | 0.5652 | 21 |
| 2026-11-18 | 142 | 0.2649 | no | 0.5385 | 37 |
| 2026-12-03 | 157 | 0.2693 | no | 0.5263 | 36 |

The important pattern: the old calendar/history signal often remained near `0.52-0.63`, but the ML output pushed the final user-facing probability below the decision threshold.

## Artifact Details

Local artifact metadata:

- `backend/app/model_artifacts/historical_model_v1.json`
- `model_version`: `historical-ml-v2-2026-06-14`
- `data_version`: `historical-only-as-of-backtest-v2-2026-06-14`
- `deployment_mode`: `segmented`

Segment metrics from metadata:

| segment | horizon | threshold | test accuracy | test balanced accuracy | test f1_completed | false_yes | false_no |
|---|---:|---:|---:|---:|---:|---:|---:|
| short_16_30 | 16-30 | 0.31 | 0.618234 | 0.539527 | 0.336634 | 45 | 89 |
| medium_31_90 | 31-90 | 0.35 | 0.581197 | 0.522251 | 0.352423 | 64 | 83 |
| long_91_365 | 91-365 | 0.57 | 0.572650 | 0.449214 | 0.056604 | 21 | 79 |

The `91-365` segment is the strongest red flag. Its test `f1_completed=0.056604` means it almost never identifies completed/successful days. It can still look superficially acceptable by accuracy if the split has many cancellations or if the selection metric rewards avoiding false positives, but product-wise it is bad for finding good dates.

## Confirmed Root Cause

The deployed historical ML path was scoring with a runtime dataset that did not match the dataset used to train the artifact.

- Training script default dataset: `data/processed/dataset_daily_flights_historical_only.csv`.
- Artifact data version: `historical-only-as-of-backtest-v2-2026-06-14`.
- Runtime/backend dataset default and `.env` value: `/app/data/processed/dataset_daily_flights.csv`.

Local dataset comparison during the investigation:

| dataset | rows | latest date | completed rate |
|---|---:|---|---:|
| `data/processed/dataset_daily_flights.csv` | 699 | 2026-04-07 | 0.545 |
| `data/processed/dataset_daily_flights_v3.csv` | 761 | 2026-05-26 | 0.540 |
| `data/processed/dataset_daily_flights_historical_only.csv` | 779 | 2026-06-13 | 0.546 |

This explains why the production examples looked worse than the old calendar/history estimate. For example, for `target_date=2026-07-16` and `as_of_date=2026-06-29`:

| feature | runtime `dataset_daily_flights.csv` | training-compatible `historical_only` dataset |
|---|---:|---:|
| `prev_1_completed` | 0 | 1 |
| `prev_7_cancelled_count` | 7 | 2 |
| `prev_14_completed_rate` | 0.0000 | 0.7857 |
| `prev_30_completed_rate` | 0.1000 | 0.7667 |
| `cancelled_streak_before` | 14 | 0 |
| `completed_streak_before` | 0 | 4 |
| `history_probability_flight` | 0.6301 | 0.6410 |

The old calendar/history probability was still neutral-positive, but ML also consumed recent-history features. On the stale runtime dataset, those features falsely described the recent period as a long cancellation streak. The fixed model artifact therefore produced much more pessimistic probabilities in production.

Contributing factors:

- Historical ML was enabled by default on 2026-06-15 and disabled by default on 2026-06-29.
- The model did not validate that the runtime dataset matched the artifact data version.
- The long-horizon segment was already product-weak in metadata (`f1_completed=0.056604`, `false_no=79`).
- Model selection optimized CV/validation score rather than hard product gates such as minimum completed-day recall, false-no budget, and positive-rate sanity checks.

Conclusion: the primary failure was a training/runtime data contract mismatch, amplified by a model objective that tolerated too many false `no` predictions.

## Current Hypotheses

### 1. The model was product-bad from the start, but spot checks missed it

This is plausible.

Reasons:

- Artifact metadata already shows weak test behavior, especially `long_91_365`.
- The model selection metrics may have overvalued cancellation detection and underweighted `false_no`.
- Initial manual checks may have sampled dates where the `short_16_30` or `medium_31_90` segment still returned some positives.
- “Adequate” may have meant “does not crash and sometimes agrees with intuition”, not “passes a date-search product acceptance set”.

What would confirm it:

- Re-run the exact deployed artifact over a fixed acceptance calendar from the day it was introduced.
- Check positive-rate by horizon segment at rollout time, not only accuracy.

### 2. Input distribution drift made a static model more pessimistic

This is also plausible.

A model artifact does not degrade by itself if weights and code are unchanged, but its predictions can degrade when input features change. In this project, historical ML features are computed with an `as_of_date` cutoff. As more recent board outcomes are added, features such as recent cancellations, recent completions, same-month history, and days since last cancellation can change.

Possible drift sources:

- New board-derived final labels from late June changed recent-history features.
- The system had several recent difficult days; a model trained to emphasize recent cancellations can start suppressing broad future dates.
- The active dataset evolved after the artifact was trained, while the model remained fixed.

What would confirm it:

- Recompute predictions for the same target dates using older `as_of_date` values and compare raw probabilities.
- Store and compare feature vectors for representative dates before/after late-June outcomes entered the dataset.

### 3. Validation split / objective mismatch

Very plausible.

The product objective is not just “classify cancellations”. Users need help finding promising dates. A model that avoids false `yes` too aggressively creates many false `no` predictions and makes the product feel dead.

Symptoms:

- High `false_no` counts in all segments.
- Extremely low `f1_completed` in the long segment.
- Recent manual attempts show ML probabilities far below the old history probability even when historical support is decent.

What to change:

- Define an acceptance metric that penalizes false `no` heavily enough.
- Track positive-rate by horizon segment.
- Require minimum `f1_completed` or recall for completed days before deployment.
- Add a product acceptance calendar: a fixed set of future dates that must include plausible `yes` outputs.

### 4. Probability transformation / threshold semantics amplified pessimism

Possible, but probably secondary.

Historical ML returns a raw model score, then converts it into a user-facing probability with `user_probability_from_model_score(raw_probability, threshold)`. If raw score is below the segment threshold, displayed probability stays below `0.5`; if above, it is lifted above `0.51`.

This design keeps displayed probability consistent with the decision, but it can make a weak raw-score model look sharply pessimistic. In the logs before 2026-06-29, raw probability and raw threshold were not persisted in `prediction_logs.csv`, which made diagnosis slower.

Mitigation already applied:

- JSONL prediction logs now include `historical_ml_*` diagnostic fields for future backups.
- Backend and forecast monitor logs include `raw_probability_flight`, `raw_threshold`, `model_name`, and reason.

### 5. Segment boundary and long-horizon threshold issue

Very plausible.

The long segment uses threshold `0.57`, much higher than short/medium thresholds. That segment also has terrible completed detection. For user searches into late 2026 / early 2027, many dates land in `91-365`, so the user sees mostly `no`.

What to test:

- Disable only `long_91_365` and keep `short_16_30` / `medium_31_90` in shadow mode.
- Compare against a blended model where ML is an adjustment over `climate_history`, not a replacement.

### 6. Data or label semantics changed after arrival-board fixes

Possible.

The project recently tightened completion semantics around airport board arrivals and same-day outcomes. If the training dataset, final outcome labels, or feature rows were rebuilt before/after these fixes, the model may have learned from a label distribution that no longer matches production interpretation.

What to check:

- Exact training data snapshot used for `historical-ml-v2-2026-06-14`.
- Whether rows around 2026-06-14 to 2026-06-29 were included or excluded.
- Difference between Telegram/manual labels and board-derived labels.

## Does ML Degrade Over Time?

Yes, but the phrasing matters.

A saved model artifact does not “age” internally. The weights stay the same. But production performance can degrade because:

- real-world data distribution changes;
- input features are computed from fresh data and drift over time;
- labels or business semantics change;
- upstream parsers/sources change;
- user behavior changes, so the model is queried on different date ranges;
- the deployment objective differs from the training/validation objective.

For FlyForecast, the likely degradation mechanism is not spontaneous model decay. It is more likely one or more of:

- the model was already too pessimistic on the test split;
- late-June production features shifted into a pessimistic region;
- the validation objective did not represent the product need of finding plausible travel dates;
- the long-horizon segment had poor completed-day recall from the start.

## Immediate Mitigation Already Applied

- `historical_ml_enabled` default changed to `false`.
- Production `.env` should either omit `HISTORICAL_ML_ENABLED` or set `HISTORICAL_ML_ENABLED=false`.
- To re-enable ML, use `HISTORICAL_ML_ENABLED=true` only after retraining and acceptance checks.
- `prediction_logs.jsonl` now includes `historical_ml_*` diagnostic fields.
- Forecast monitor logs historical ML raw diagnostics when the code path is evaluated.

## Recommended Investigation Plan

1. Reconstruct the exact rollout timeline:
   - commit that introduced the artifact;
   - artifact file hashes on production;
   - first production timestamp with `forecast_mode=historical_ml`;
   - manual spot-check dates that looked “adequate”.

2. Build a fixed acceptance calendar:
   - 20-40 future dates across 16-30, 31-90, 91-365 horizons;
   - include historically good windows, historically bad windows, and neutral dates;
   - store expected qualitative behavior, not exact probabilities.

3. Run the current artifact in shadow mode:
   - over the acceptance calendar;
   - for several `as_of_date` values: rollout day, 2026-06-20, 2026-06-29;
   - log raw score, threshold, final user probability, old `climate_history` probability, and features.

4. Compare against the old `climate_history` formula:
   - positive rate;
   - false-no rate on known final days;
   - ability to surface at least some plausible `yes` dates.

5. Revisit training objective:
   - require minimum `f1_completed` / recall_completed per segment;
   - add explicit positive-rate sanity bounds;
   - penalize false `no` more strongly for date-search UX;
   - report segment metrics as blocking deployment gates.

6. Prefer shadow rollout:
   - keep public output on `climate_history`;
   - compute ML diagnostics in logs only;
   - promote to user-facing only after several days of acceptable shadow metrics.

## Re-enable Criteria

Do not re-enable historical ML publicly until all are true:

- Each horizon segment passes minimum completed-day recall / `f1_completed`.
- Long segment no longer has near-zero `f1_completed`.
- Acceptance calendar contains a healthy mix of `yes` and `no`.
- Public user-facing probabilities do not suppress old historical probabilities without a clear reason.
- Raw ML diagnostics are visible in backups for at least one shadow period.
- Product owner accepts the tradeoff between false `yes` and false `no`.
