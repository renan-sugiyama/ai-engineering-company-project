from __future__ import annotations

import hashlib
import json
import platform
from pathlib import Path

import joblib
import matplotlib
import numpy as np
import pandas as pd
import sklearn

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from data.pipelines.sales_forecast.core import fit_model, load_sales, recursive_forecast, split_sales, training_features
from data.pipelines.sales_forecast.metrics import regression_metrics


HISTORY_LENGTHS = (24, 36, 48, 60, 72)
HORIZON = 24


def temporal_folds(train: pd.DataFrame) -> list[tuple[np.ndarray, np.ndarray]]:
    expected = pd.date_range("2016-01-01", "2023-12-01", freq="MS")
    if not pd.DatetimeIndex(train["month"]).equals(expected):
        raise ValueError("Expected the ordered 2016-2023 training months without gaps")
    return [
        (np.arange(history_length), np.arange(history_length, history_length + HORIZON))
        for history_length in HISTORY_LENGTHS
    ]


def error_metrics(actual: np.ndarray, predicted: np.ndarray) -> dict:
    actual = np.asarray(actual, dtype=float)
    predicted = np.asarray(predicted, dtype=float)
    if actual.ndim != 1 or actual.shape != predicted.shape or not len(actual):
        raise ValueError("Expected equally sized nonempty one-dimensional arrays")
    if not np.isfinite(actual).all() or not np.isfinite(predicted).all():
        raise ValueError("Metrics require finite values")
    if np.any(actual <= 0):
        raise ValueError("Revenue actuals must be positive")
    errors = predicted - actual
    positive = errors[errors > 0]
    regression = regression_metrics(actual, predicted)
    return {
        "mae_usd": regression["mae_usd"],
        "rmse_usd": regression["rmse_usd"],
        "mean_signed_error_usd": float(np.mean(errors)),
        "overforecast_frequency": float(len(positive) / len(errors)),
        "mean_overforecast_usd": float(np.mean(positive)) if len(positive) else None,
    }


def summarize_folds(folds: list[dict]) -> dict:
    if not folds:
        raise ValueError("At least one fold is required")
    return {
        name: {
            "mean": float(np.mean(values)),
            "std": float(np.std(values, ddof=0)),
        }
        for name in ("mae_usd", "rmse_usd")
        for values in [[fold["validation_metrics"][name] for fold in folds]]
    }


def evaluate_history(history: pd.DataFrame, validation: pd.DataFrame) -> dict:
    history_months = pd.DatetimeIndex(history["month"])
    validation_months = pd.DatetimeIndex(validation["month"])
    if len(history_months) < 13 or len(validation_months) != HORIZON:
        raise ValueError("Expected at least 13 training months and 24 validation months")
    all_months = history_months.append(validation_months)
    if not all_months.equals(pd.date_range(all_months[0], periods=len(all_months), freq="MS")):
        raise ValueError("History and validation must be contiguous and chronological")
    if all_months[-1] > pd.Timestamp("2023-12-01"):
        raise ValueError("Evaluation must not use the 2024-2025 holdout")

    features, target = training_features(history)
    model = fit_model(history)
    training_predictions = model.predict(features)
    forecast = recursive_forecast(model, history, periods=HORIZON)
    if not pd.DatetimeIndex(forecast["month"]).equals(validation_months):
        raise ValueError("Forecast and validation months differ")
    baseline = np.tile(history["revenue_usd"].to_numpy(dtype=float)[-12:], 2)
    actual = validation["revenue_usd"].to_numpy(dtype=float)
    predictions = forecast["prediction"].to_numpy(dtype=float)
    return {
        "history_start": history_months[0].strftime("%Y-%m"),
        "history_end": history_months[-1].strftime("%Y-%m"),
        "history_months": len(history),
        "eligible_training_rows": len(features),
        "validation_start": validation_months[0].strftime("%Y-%m"),
        "validation_end": validation_months[-1].strftime("%Y-%m"),
        "validation_months": len(validation),
        "training_metrics": error_metrics(target.to_numpy(), training_predictions),
        "validation_metrics": error_metrics(actual, predictions),
        "baseline_metrics": error_metrics(actual, baseline),
        "validation_actual_usd": actual.tolist(),
        "validation_prediction_usd": predictions.tolist(),
        "baseline_prediction_usd": baseline.tolist(),
        "model_parameters": model.named_steps["forest"].get_params(),
    }


def evaluate_training_period(train: pd.DataFrame) -> dict:
    fold_indices = temporal_folds(train)
    folds = [evaluate_history(train.iloc[history], train.iloc[validation]) for history, validation in fold_indices]
    origin = HISTORY_LENGTHS[-1]
    validation = train.iloc[origin:origin + HORIZON]
    curve = [evaluate_history(train.iloc[origin - length:origin], validation) for length in HISTORY_LENGTHS]
    return {"folds": folds, "fold_summary": summarize_folds(folds), "learning_curve": curve}


def run_evaluation(input_path: Path, output_dir: Path) -> dict:
    data = load_sales(input_path)
    train, _ = split_sales(data)
    result = evaluate_training_period(train)
    result["input"] = {
        "path": "data/raw/nexova_sales.csv",
        "sha256": hashlib.sha256(input_path.read_bytes()).hexdigest(),
        "target": "revenue_usd",
        "business_line": "consolidated",
        "training_period": ["2016-01", "2023-12"],
        "holdout_period": ["2024-01", "2025-12"],
    }
    result["versions"] = {
        "python": platform.python_version(),
        "numpy": np.__version__,
        "pandas": pd.__version__,
        "scikit-learn": sklearn.__version__,
        "matplotlib": matplotlib.__version__,
        "joblib": joblib.__version__,
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "evaluation_data.json").write_text(json.dumps(result, indent=2, allow_nan=False) + "\n")

    curve = result["learning_curve"]
    figure, axis = plt.subplots(figsize=(9, 5), layout="constrained")
    axis.plot([point["eligible_training_rows"] for point in curve],
              [point["training_metrics"]["mae_usd"] for point in curve],
              color="#155d45", marker="o", label="Training MAE (in-sample)")
    axis.plot([point["eligible_training_rows"] for point in curve],
              [point["validation_metrics"]["mae_usd"] for point in curve],
              color="#a94d24", marker="o", label="Validation MAE (24-month recursive)")
    axis.set(title="Nexova sales forecast learning curve", xlabel="Eligible training rows after 12-month warm-up", ylabel="Mean absolute error (USD)")
    axis.grid(axis="y", alpha=0.25)
    axis.legend(loc="best")
    figure.savefig(output_dir / "learning_curve.png", dpi=160)
    plt.close(figure)
    return result