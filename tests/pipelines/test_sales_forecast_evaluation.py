from pathlib import Path
import json
import sys

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from data.pipelines.sales_forecast import core, evaluation
from data.pipelines.sales_forecast.core import load_sales, split_sales, training_features
from data.pipelines.sales_forecast.evaluation import HISTORY_LENGTHS, HORIZON, temporal_folds


def test_folds_preserve_chronology_and_horizon() -> None:
    train, _ = split_sales(load_sales(ROOT / "data/raw/nexova_sales.csv"))
    folds = temporal_folds(train)
    assert len(folds) == 5
    for (train_indices, validation_indices), history_length in zip(folds, HISTORY_LENGTHS, strict=True):
        np.testing.assert_array_equal(train_indices, np.arange(history_length))
        np.testing.assert_array_equal(validation_indices, np.arange(history_length, history_length + HORIZON))
        assert len(np.intersect1d(train_indices, validation_indices)) == 0
        assert train.iloc[train_indices]["month"].is_monotonic_increasing
        assert train.iloc[validation_indices]["month"].is_monotonic_increasing
        assert train.iloc[train_indices[-1]]["month"] < train.iloc[validation_indices[0]]["month"]
        assert train.iloc[validation_indices[-1]]["month"] <= pd.Timestamp("2023-12-01")


def test_folds_reject_shuffled_or_incomplete_training_data() -> None:
    train, _ = split_sales(load_sales(ROOT / "data/raw/nexova_sales.csv"))
    for invalid in (train.sample(frac=1, random_state=42), train.iloc[:-1]):
        with pytest.raises(ValueError, match="ordered"):
            temporal_folds(invalid)


def test_recursive_validation_never_reads_validation_actuals(monkeypatch: pytest.MonkeyPatch) -> None:
    train, _ = split_sales(load_sales(ROOT / "data/raw/nexova_sales.csv"))
    history = train.iloc[:36].copy()
    validation = train.iloc[36:60].copy()
    captured_fits = []
    captured_rows = []

    def record_fit(fit_history: pd.DataFrame):
        fitted = core.fit_model(fit_history)
        captured_fits.append((fit_history.copy(), fitted.named_steps["imputer"].statistics_.copy()))

        class RecordingModel:
            named_steps = fitted.named_steps

            def predict(self, features: pd.DataFrame):
                predictions = fitted.predict(features)
                if len(features) == 1:
                    captured_rows.append((features.copy(), float(predictions[0])))
                return predictions

        return RecordingModel()

    monkeypatch.setattr(evaluation, "fit_model", record_fit)
    original = evaluation.evaluate_history(history, validation)
    changed = validation.copy()
    changed["revenue_usd"] *= 3
    changed["active_contracts"] *= 10
    changed["avg_contract_value_usd"] *= 10
    repeated = evaluation.evaluate_history(history, changed)

    assert original["validation_prediction_usd"] == repeated["validation_prediction_usd"]
    assert original["training_metrics"] == repeated["training_metrics"]
    assert original["validation_metrics"] != repeated["validation_metrics"]
    expected_features, _ = training_features(history)
    for fit_history, imputer_statistics in captured_fits:
        pd.testing.assert_frame_equal(fit_history, history)
        np.testing.assert_allclose(imputer_statistics, expected_features.median().to_numpy())
    revenue_history = history["revenue_usd"].tolist()
    for features, prediction in captured_rows[:HORIZON]:
        assert features.iloc[0]["lag_1"] == pytest.approx(revenue_history[-1])
        assert features.iloc[0]["lag_12"] == pytest.approx(revenue_history[-12])
        assert features.iloc[0]["rolling_3"] == pytest.approx(np.mean(revenue_history[-3:]))
        revenue_history.append(prediction)
    assert len(captured_rows) == 2 * HORIZON


def test_error_metrics_and_fold_summary_are_hand_calculated() -> None:
    metrics = evaluation.error_metrics(np.array([1., 2., 4.]), np.array([3., 1., 4.]))
    assert metrics["mae_usd"] == pytest.approx(1)
    assert metrics["rmse_usd"] == pytest.approx(np.sqrt(5 / 3))
    assert metrics["mean_signed_error_usd"] == pytest.approx(1 / 3)
    assert metrics["overforecast_frequency"] == pytest.approx(1 / 3)
    assert metrics["mean_overforecast_usd"] == pytest.approx(2)
    assert evaluation.error_metrics(np.array([2.]), np.array([1.]))["mean_overforecast_usd"] is None
    summary = evaluation.summarize_folds([
        {"validation_metrics": {"mae_usd": 1., "rmse_usd": 2.}},
        {"validation_metrics": {"mae_usd": 3., "rmse_usd": 6.}},
    ])
    assert summary == {"mae_usd": {"mean": 2., "std": 1.}, "rmse_usd": {"mean": 4., "std": 2.}}
    with pytest.raises(ValueError):
        evaluation.error_metrics(np.array([1.]), np.array([np.nan]))


def test_evaluation_outputs_are_reproducible_and_use_training_period(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    original_fit = evaluation.fit_model
    fitted_histories = []

    def record_fit(history: pd.DataFrame):
        fitted_histories.append(history.copy())
        return original_fit(history)

    monkeypatch.setattr(evaluation, "fit_model", record_fit)
    data_path = ROOT / "data/raw/nexova_sales.csv"
    result = evaluation.run_evaluation(data_path, tmp_path)
    assert len(fitted_histories) == 10
    assert all(history["month"].max() <= pd.Timestamp("2021-12-01") for history in fitted_histories)
    assert [point["history_months"] for point in result["learning_curve"]] == list(HISTORY_LENGTHS)
    assert [point["eligible_training_rows"] for point in result["learning_curve"]] == [12, 24, 36, 48, 60]
    assert all(point["history_end"] == "2021-12" and point["validation_start"] == "2022-01"
               and point["validation_end"] == "2023-12" for point in result["learning_curve"])
    for fold in result["folds"]:
        assert fold["validation_months"] == 24
        assert np.isfinite(list(fold["validation_metrics"].values())[:4]).all()
        assert len(fold["validation_prediction_usd"]) == 24
    assert (tmp_path / "learning_curve.png").read_bytes().startswith(b"\x89PNG\r\n\x1a\n")
    assert json.loads((tmp_path / "evaluation_data.json").read_text()) == result
    report = (ROOT / "data/eval/evaluation_report.md").read_text()
    for metric in ("mae_usd", "rmse_usd"):
        summary = result["fold_summary"][metric]
        assert f"${summary['mean']:,.2f} ± ${summary['std']:,.2f}" in report
    rows = [[cell.strip() for cell in line.strip("|").split("|")]
            for line in report.splitlines() if line.startswith("|")]
    for fold in result["folds"]:
        row = next(row for row in rows if row[0] == fold["history_end"])
        expected = [f"{fold[group][metric]:,.0f}"
                    for group in ("training_metrics", "validation_metrics", "baseline_metrics")
                    for metric in ("mae_usd", "rmse_usd")]
        assert row[3:9] == expected
    for point in result["learning_curve"]:
        row = next(row for row in rows
                   if row[:2] == [str(point["history_months"]), point["history_start"]])
        expected = [f"{point[group][metric]:,.0f}"
                    for group in ("training_metrics", "validation_metrics")
                    for metric in ("mae_usd", "rmse_usd")]
        assert row[3:7] == expected
    repeated = evaluation.run_evaluation(data_path, tmp_path)
    assert result == repeated


def test_holdout_rejected_for_evaluation() -> None:
    data = load_sales(ROOT / "data/raw/nexova_sales.csv")
    with pytest.raises(ValueError, match="holdout"):
        evaluation.evaluate_history(data.iloc[:96], data.iloc[96:])