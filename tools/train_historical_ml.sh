#!/usr/bin/env bash
set -euo pipefail

python pipelines/training/train_historical_ml_model.py \
  --families sklearn,catboost,lightgbm \
  --search-level standard \
  --deployment-mode auto \
  --temporal-folds 4 \
  --max-candidates-per-family 40
