from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from data.pipelines.sales_forecast.evaluation import run_evaluation


if __name__ == "__main__":
    result = run_evaluation(ROOT / "data/raw/nexova_sales.csv", ROOT / "data/eval")
    for metric, summary in result["fold_summary"].items():
        print(f"Validation {metric}: {summary['mean']:.2f} ± {summary['std']:.2f}")