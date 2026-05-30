import json
import os
from datetime import datetime, timezone
from pathlib import Path

import joblib
import matplotlib
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

matplotlib.use("Agg")
import matplotlib.pyplot as plt


DEFAULT_DATASET_PATH = Path(os.getenv("DATASET_PATH", "data/ml_training_data_90days.parquet"))
DEFAULT_ARTIFACTS_DIR = Path(os.getenv("ARTIFACTS_DIR", "artifacts/isolation_forest"))

REQUIRED_COLUMNS = [
    "timestamp",
    "symbol",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "log_returns",
    "volatility_30m",
    "z_score",
]
SYMBOLS = ["BTC/USDT", "ETH/USDT"]
FEATURE_COLUMNS = ["volume", "log_returns", "z_score", "volatility_30m"]
CONTAMINATION_VALUES = [0.01, 0.02, 0.03, 0.04, 0.05]
PREFERRED_CONTAMINATIONS = [0.03, 0.02]


def symbol_slug(symbol):
    return symbol.replace("/", "").lower()


def validate_dataset(dataframe):
    missing = [column for column in REQUIRED_COLUMNS if column not in dataframe.columns]
    if missing:
        raise ValueError(f"Dataset missing required columns: {missing}")

    df = dataframe[REQUIRED_COLUMNS].copy()
    try:
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    except Exception as exc:
        raise ValueError("timestamp column is not parseable as datetime") from exc

    null_counts = df[REQUIRED_COLUMNS].isna().sum()
    if null_counts.any():
        raise ValueError(f"Dataset has NaN values: {null_counts[null_counts > 0].to_dict()}")

    present_symbols = set(df["symbol"].unique())
    missing_symbols = [symbol for symbol in SYMBOLS if symbol not in present_symbols]
    if missing_symbols:
        raise ValueError(f"Dataset missing required symbols: {missing_symbols}")

    sorted_df = df.sort_values(["symbol", "timestamp"], kind="mergesort").reset_index(drop=True)
    for symbol, group in sorted_df.groupby("symbol"):
        if not group["timestamp"].is_monotonic_increasing:
            raise ValueError(f"Could not sort timestamps for {symbol}")

    return sorted_df


def baseline_labels(df_symbol):
    labels = np.where(df_symbol["z_score"].abs().to_numpy() > 3.0, -1, 1)
    return pd.Series(labels, index=df_symbol.index)


def fit_isolation_forest(df_symbol, contamination):
    scaler = StandardScaler()
    X = scaler.fit_transform(df_symbol[FEATURE_COLUMNS].to_numpy(dtype=np.float64))

    model = IsolationForest(
        n_estimators=200,
        contamination=contamination,
        random_state=42,
        n_jobs=-1,
    )
    predictions = model.fit_predict(X)
    scores = model.decision_function(X)
    return model, scaler, pd.Series(predictions, index=df_symbol.index), pd.Series(scores, index=df_symbol.index)


def summarize_predictions(df_symbol, baseline, predictions, contamination):
    row_count = int(len(df_symbol))
    baseline_mask = baseline == -1
    isolation_mask = predictions == -1
    overlap_mask = baseline_mask & isolation_mask
    isolation_only_mask = isolation_mask & ~baseline_mask
    baseline_only_mask = baseline_mask & ~isolation_mask

    volume_percentile = df_symbol["volume"].rank(pct=True)
    isolation_only_abs_z = df_symbol.loc[isolation_only_mask, "z_score"].abs()
    isolation_only_volume_pct = volume_percentile.loc[isolation_only_mask]

    return {
        "contamination": contamination,
        "row_count": row_count,
        "baseline_anomaly_count": int(baseline_mask.sum()),
        "baseline_anomaly_pct": float(baseline_mask.mean() * 100),
        "isolation_anomaly_count": int(isolation_mask.sum()),
        "isolation_anomaly_pct": float(isolation_mask.mean() * 100),
        "overlap_count": int(overlap_mask.sum()),
        "isolation_only_count": int(isolation_only_mask.sum()),
        "baseline_only_count": int(baseline_only_mask.sum()),
        "isolation_only_avg_abs_z_score": (
            float(isolation_only_abs_z.mean()) if len(isolation_only_abs_z) else None
        ),
        "isolation_only_avg_volume_percentile": (
            float(isolation_only_volume_pct.mean()) if len(isolation_only_volume_pct) else None
        ),
    }


def choose_contamination(evaluations):
    reasonable = [
        item
        for item in evaluations
        if 0.5 <= item["isolation_anomaly_pct"] <= 6.0
    ]
    candidates = reasonable or evaluations

    for preferred in PREFERRED_CONTAMINATIONS:
        for item in candidates:
            if np.isclose(item["contamination"], preferred):
                return item

    baseline_pct = evaluations[0]["baseline_anomaly_pct"]
    return min(candidates, key=lambda item: abs(item["isolation_anomaly_pct"] - baseline_pct))


def save_anomaly_plot(df_symbol, symbol, baseline, predictions, output_path):
    baseline_mask = baseline == -1
    isolation_mask = predictions == -1

    fig, ax = plt.subplots(figsize=(16, 7))
    ax.plot(df_symbol["timestamp"], df_symbol["close"], color="#1f77b4", linewidth=1.0, label="Close")
    ax.scatter(
        df_symbol.loc[baseline_mask, "timestamp"],
        df_symbol.loc[baseline_mask, "close"],
        s=16,
        color="#ff7f0e",
        alpha=0.8,
        label="Baseline abs(z_score) > 3",
    )
    ax.scatter(
        df_symbol.loc[isolation_mask, "timestamp"],
        df_symbol.loc[isolation_mask, "close"],
        s=12,
        color="#d62728",
        marker="x",
        alpha=0.8,
        label="Isolation Forest",
    )
    ax.set_title(f"{symbol} price with detected anomalies")
    ax.set_xlabel("Timestamp")
    ax.set_ylabel("Close price")
    ax.legend(loc="best")
    ax.grid(True, alpha=0.25)
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(output_path, dpi=140)
    plt.close(fig)


def train_symbol_pipeline(dataframe, symbol, artifacts_dir, dataset_path):
    df_symbol = dataframe[dataframe["symbol"] == symbol].copy().reset_index(drop=True)
    if df_symbol.empty:
        raise ValueError(f"No rows found for {symbol}")

    baseline = baseline_labels(df_symbol)

    evaluations = []
    fitted_by_contamination = {}
    for contamination in CONTAMINATION_VALUES:
        model, scaler, predictions, scores = fit_isolation_forest(df_symbol, contamination)
        summary = summarize_predictions(df_symbol, baseline, predictions, contamination)
        evaluations.append(summary)
        fitted_by_contamination[contamination] = {
            "model": model,
            "scaler": scaler,
            "predictions": predictions,
            "scores": scores,
        }

    chosen_summary = choose_contamination(evaluations)
    chosen_contamination = chosen_summary["contamination"]
    chosen_fit = fitted_by_contamination[chosen_contamination]

    slug = symbol_slug(symbol)
    model_path = artifacts_dir / f"isolation_forest_{slug}.pkl"
    plot_path = artifacts_dir / f"anomalies_{slug}.png"

    bundle = {
        "model": chosen_fit["model"],
        "scaler": chosen_fit["scaler"],
        "feature_columns": FEATURE_COLUMNS,
        "symbol": symbol,
        "contamination": chosen_contamination,
        "training_timestamp": datetime.now(timezone.utc).isoformat(),
        "metadata": {
            "dataset_path": str(Path(dataset_path)),
            "row_count": int(len(df_symbol)),
            "baseline_rule": "abs(z_score) > 3",
            "contamination_values_tried": CONTAMINATION_VALUES,
            "chosen_rule": "prefer 0.03 or 0.02 when anomaly rate is between 0.5% and 6.0%",
        },
        "metrics": chosen_summary,
    }
    joblib.dump(bundle, model_path)
    save_anomaly_plot(df_symbol, symbol, baseline, chosen_fit["predictions"], plot_path)

    print(f"\n{symbol} contamination sweep")
    table = pd.DataFrame(evaluations)
    display_columns = [
        "contamination",
        "baseline_anomaly_count",
        "baseline_anomaly_pct",
        "isolation_anomaly_count",
        "isolation_anomaly_pct",
        "overlap_count",
        "isolation_only_count",
        "baseline_only_count",
        "isolation_only_avg_abs_z_score",
        "isolation_only_avg_volume_percentile",
    ]
    print(table[display_columns].to_string(index=False))
    print(f"Chosen contamination for {symbol}: {chosen_contamination}")
    print(f"Saved model artifact: {model_path}")
    print(f"Saved anomaly plot:   {plot_path}")

    return {
        "symbol": symbol,
        "row_count": int(len(df_symbol)),
        "feature_columns": FEATURE_COLUMNS,
        "baseline_rule": "abs(z_score) > 3",
        "contamination_values_tried": CONTAMINATION_VALUES,
        "chosen_contamination": chosen_contamination,
        "evaluations": evaluations,
        "chosen_metrics": chosen_summary,
        "artifacts": {
            "model_path": str(model_path),
            "plot_path": str(plot_path),
        },
    }


def save_metrics(metrics_summary, artifacts_dir):
    metrics_path = artifacts_dir / "metrics_summary.json"
    metrics_path.write_text(json.dumps(metrics_summary, indent=2), encoding="utf-8")
    print(f"\nSaved metrics summary: {metrics_path}")


def run_pipeline(dataset_path=DEFAULT_DATASET_PATH, artifacts_dir=DEFAULT_ARTIFACTS_DIR):
    dataset_path = Path(dataset_path)
    artifacts_dir = Path(artifacts_dir)
    artifacts_dir.mkdir(parents=True, exist_ok=True)

    print(f"Resolved dataset path: {dataset_path.resolve()}")
    if not dataset_path.exists():
        raise FileNotFoundError(f"Dataset not found at: {dataset_path}")

    df = pd.read_parquet(dataset_path)
    df = validate_dataset(df)

    print(f"Dataset shape: {df.shape}")
    print("Symbol counts:", df["symbol"].value_counts().to_dict())
    print("Timestamp range:", df["timestamp"].min(), "->", df["timestamp"].max())
    print("Features:", FEATURE_COLUMNS)
    print("Baseline rule: abs(z_score) > 3")

    metrics_summary = [
        train_symbol_pipeline(df, symbol, artifacts_dir, dataset_path)
        for symbol in SYMBOLS
    ]
    save_metrics(metrics_summary, artifacts_dir)

    print("\nFinal chosen Isolation Forest artifacts")
    for item in metrics_summary:
        chosen = item["chosen_metrics"]
        print(
            f"{item['symbol']}: contamination={item['chosen_contamination']} | "
            f"baseline anomalies={chosen['baseline_anomaly_count']} "
            f"({chosen['baseline_anomaly_pct']:.2f}%) | "
            f"isolation anomalies={chosen['isolation_anomaly_count']} "
            f"({chosen['isolation_anomaly_pct']:.2f}%) | "
            f"overlap={chosen['overlap_count']}"
        )

    return metrics_summary


if __name__ == "__main__":
    run_pipeline()
