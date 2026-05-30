import json
import os
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

import joblib
import matplotlib
import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.preprocessing import MinMaxScaler

matplotlib.use("Agg")
import matplotlib.pyplot as plt


DEFAULT_DATASET_PATH = Path(os.getenv("DATASET_PATH", "data/ml_training_data_90days.parquet"))
DEFAULT_ARTIFACTS_DIR = Path(os.getenv("ARTIFACTS_DIR", "artifacts/lstm"))
DEFAULT_DASHBOARD_MODELS_DIR = Path(os.getenv("DASHBOARD_MODELS_DIR", "src/dashboard/models"))

REQUIRED_COLUMNS = [
    "timestamp",
    "symbol",
    "close",
    "volume",
    "log_returns",
    "volatility_30m",
    "z_score",
]
SYMBOLS = ["BTC/USDT", "ETH/USDT"]
FEATURE_COLUMNS = ["close", "volume", "log_returns", "volatility_30m", "z_score"]
TARGET_TYPE = "return"
WINDOW_SIZE = 60
TRAIN_RATIO = 0.70
VALIDATION_RATIO = 0.15
TEST_RATIO = 0.15
MAX_EPOCHS = int(os.getenv("LSTM_MAX_EPOCHS", "50"))
BATCH_SIZE = int(os.getenv("LSTM_BATCH_SIZE", "128"))
RANDOM_SEED = 42
MODEL_ARCHITECTURE = (
    "LSTM(64, return_sequences=True) -> Dropout(0.2) -> "
    "LSTM(32) -> Dropout(0.2) -> Dense(16, relu) -> Dense(1)"
)


def require_tensorflow():
    try:
        import tensorflow as tf
        from tensorflow.keras.callbacks import EarlyStopping, ReduceLROnPlateau
        from tensorflow.keras.layers import LSTM, Dense, Dropout, Input
        from tensorflow.keras.models import Sequential
        from tensorflow.keras.optimizers import Adam

        return {
            "tf": tf,
            "EarlyStopping": EarlyStopping,
            "ReduceLROnPlateau": ReduceLROnPlateau,
            "LSTM": LSTM,
            "Dense": Dense,
            "Dropout": Dropout,
            "Input": Input,
            "Sequential": Sequential,
            "Adam": Adam,
        }
    except ImportError as exc:
        print("TensorFlow is not installed, so LSTM training was skipped.")
        print("Install it with:")
        print("  pip install tensorflow")
        print(f"Import error: {exc}")
        return None


def symbol_slug(symbol):
    return symbol.replace("/", "").lower()


def short_symbol(symbol):
    return symbol.split("/")[0].lower()


def rmse(y_true, y_pred):
    return float(np.sqrt(mean_squared_error(y_true, y_pred)))


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


def split_time_series(df_symbol):
    n_rows = len(df_symbol)
    train_end = int(n_rows * TRAIN_RATIO)
    validation_end = int(n_rows * (TRAIN_RATIO + VALIDATION_RATIO))

    train_df = df_symbol.iloc[:train_end].copy()
    validation_df = df_symbol.iloc[train_end:validation_end].copy()
    test_df = df_symbol.iloc[validation_end:].copy()

    if min(len(train_df), len(validation_df), len(test_df)) <= WINDOW_SIZE + 1:
        raise ValueError("One split is too small for a 60-step LSTM window")

    return train_df, validation_df, test_df


def build_windowed_samples(scaled_values, close_values, timestamps, train_end, validation_end):
    samples = {"train": [], "validation": [], "test": []}

    for target_index in range(WINDOW_SIZE, len(scaled_values)):
        last_close = close_values[target_index - 1]
        actual_close = close_values[target_index]
        target_return = (actual_close / last_close) - 1.0

        if target_index < train_end:
            split_name = "train"
        elif target_index < validation_end:
            split_name = "validation"
        else:
            split_name = "test"

        samples[split_name].append(
            {
                "X": scaled_values[target_index - WINDOW_SIZE : target_index, :],
                "target_return": target_return,
                "actual_close": actual_close,
                "last_close": last_close,
                "timestamp": timestamps[target_index],
            }
        )

    return samples


def materialize_samples(items, target_mean=None, target_std=None):
    X = np.asarray([item["X"] for item in items], dtype=np.float32)
    target_return = np.asarray([item["target_return"] for item in items], dtype=np.float64)
    actual_close = np.asarray([item["actual_close"] for item in items], dtype=np.float64)
    last_close = np.asarray([item["last_close"] for item in items], dtype=np.float64)
    timestamps = pd.Series([item["timestamp"] for item in items])

    if target_mean is None:
        target_mean = float(target_return.mean())
    if target_std is None:
        target_std = float(target_return.std(ddof=0))
    if target_std == 0:
        target_std = 1.0

    y = ((target_return - target_mean) / target_std).astype(np.float32)
    return X, y, target_return, actual_close, last_close, timestamps, target_mean, target_std


def build_model(tf_modules):
    tf = tf_modules["tf"]
    tf.random.set_seed(RANDOM_SEED)

    model = tf_modules["Sequential"](
        [
            tf_modules["Input"](shape=(WINDOW_SIZE, len(FEATURE_COLUMNS))),
            tf_modules["LSTM"](64, return_sequences=True),
            tf_modules["Dropout"](0.2),
            tf_modules["LSTM"](32),
            tf_modules["Dropout"](0.2),
            tf_modules["Dense"](16, activation="relu"),
            tf_modules["Dense"](1),
        ]
    )
    model.compile(optimizer=tf_modules["Adam"](learning_rate=0.001), loss="mse")
    return model


def save_prediction_plot(symbol, timestamps, actual, naive, predicted, output_path):
    fig, ax = plt.subplots(figsize=(16, 7))
    ax.plot(timestamps, actual, label="Actual close", color="#1f77b4", linewidth=1.1)
    ax.plot(timestamps, naive, label="Naive baseline", color="#ff7f0e", linewidth=0.9, alpha=0.8)
    ax.plot(timestamps, predicted, label="LSTM prediction", color="#2ca02c", linewidth=0.9, alpha=0.9)
    ax.set_title(f"{symbol} next-close prediction on test set")
    ax.set_xlabel("Timestamp")
    ax.set_ylabel("Close price")
    ax.grid(True, alpha=0.25)
    ax.legend(loc="best")
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(output_path, dpi=140)
    plt.close(fig)


def train_symbol_pipeline(dataframe, symbol, artifacts_dir, dashboard_models_dir, tf_modules):
    df_symbol = dataframe[dataframe["symbol"] == symbol].copy().reset_index(drop=True)
    train_df, validation_df, test_df = split_time_series(df_symbol)
    n_rows = len(df_symbol)
    train_end = int(n_rows * TRAIN_RATIO)
    validation_end = int(n_rows * (TRAIN_RATIO + VALIDATION_RATIO))

    scaler = MinMaxScaler(feature_range=(0, 1))
    train_scaled = scaler.fit_transform(train_df[FEATURE_COLUMNS].to_numpy(dtype=np.float64))
    scaled_values = scaler.transform(df_symbol[FEATURE_COLUMNS].to_numpy(dtype=np.float64))
    close_values = df_symbol["close"].to_numpy(dtype=np.float64)
    timestamps = df_symbol["timestamp"].to_numpy()

    samples = build_windowed_samples(scaled_values, close_values, timestamps, train_end, validation_end)
    X_train, y_train, _, _, _, _, target_mean, target_std = materialize_samples(samples["train"])
    X_validation, y_validation, _, _, _, _, _, _ = materialize_samples(
        samples["validation"],
        target_mean,
        target_std,
    )
    X_test, _, y_test_return, y_test_close, naive_test_close, test_timestamps, _, _ = materialize_samples(
        samples["test"],
        target_mean,
        target_std,
    )

    model = build_model(tf_modules)
    early_stopping = tf_modules["EarlyStopping"](
        monitor="val_loss",
        patience=7,
        restore_best_weights=True,
    )
    reduce_lr = tf_modules["ReduceLROnPlateau"](
        monitor="val_loss",
        factor=0.5,
        patience=3,
        min_lr=1e-5,
    )

    print(f"\nTraining LSTM for {symbol}")
    print(f"X_train shape: {X_train.shape}")
    print(f"X_validation shape: {X_validation.shape}")
    print(f"X_test shape: {X_test.shape}")

    history = model.fit(
        X_train,
        y_train,
        validation_data=(X_validation, y_validation),
        epochs=MAX_EPOCHS,
        batch_size=BATCH_SIZE,
        shuffle=False,
        callbacks=[early_stopping, reduce_lr],
        verbose=1,
    )

    predictions_scaled_return = model.predict(X_test, batch_size=BATCH_SIZE, verbose=0).reshape(-1)
    predictions_return = (predictions_scaled_return * target_std) + target_mean
    predictions_close = naive_test_close * (1.0 + predictions_return)

    baseline_rmse = rmse(y_test_close, naive_test_close)
    baseline_mae = float(mean_absolute_error(y_test_close, naive_test_close))
    lstm_rmse = rmse(y_test_close, predictions_close)
    lstm_mae = float(mean_absolute_error(y_test_close, predictions_close))
    accepted = bool(lstm_rmse < baseline_rmse)

    coin = short_symbol(symbol)
    slug = symbol_slug(symbol)
    model_path = artifacts_dir / f"lstm_{coin}_model.h5"
    scaler_path = artifacts_dir / f"scaler_{coin}.pkl"
    plot_path = artifacts_dir / f"predictions_{slug}.png"

    model.save(model_path)
    joblib.dump(
        {
            "scaler": scaler,
            "feature_columns": FEATURE_COLUMNS,
            "symbol": symbol,
            "window_size": WINDOW_SIZE,
            "target_type": TARGET_TYPE,
            "target": "next_step_return",
            "target_mean": target_mean,
            "target_std": target_std,
            "training_timestamp": datetime.now(timezone.utc).isoformat(),
        },
        scaler_path,
    )
    save_prediction_plot(symbol, test_timestamps, y_test_close, naive_test_close, predictions_close, plot_path)

    dashboard_model_path = dashboard_models_dir / model_path.name
    dashboard_scaler_path = dashboard_models_dir / scaler_path.name
    shutil.copy2(model_path, dashboard_model_path)
    shutil.copy2(scaler_path, dashboard_scaler_path)

    metrics = {
        "symbol": symbol,
        "split_sizes": {
            "train": int(len(train_df)),
            "validation": int(len(validation_df)),
            "test": int(len(test_df)),
        },
        "window_size": WINDOW_SIZE,
        "target_type": TARGET_TYPE,
        "target_description": "target_return = close(t+1) / close(t) - 1; predicted_close = close(t) * (1 + predicted_return)",
        "model_architecture": MODEL_ARCHITECTURE,
        "target_scaling": {
            "method": "standardize_return_with_training_target_statistics",
            "mean": target_mean,
            "std": target_std,
        },
        "feature_columns": FEATURE_COLUMNS,
        "x_shapes": {
            "train": list(X_train.shape),
            "validation": list(X_validation.shape),
            "test": list(X_test.shape),
        },
        "baseline": {
            "method": "naive close(t+1) = close(t)",
            "rmse": baseline_rmse,
            "mae": baseline_mae,
        },
        "lstm": {
            "rmse": lstm_rmse,
            "mae": lstm_mae,
            "epochs_ran": int(len(history.history.get("loss", []))),
            "best_val_loss": float(min(history.history.get("val_loss", [np.nan]))),
            "predicted_return_mean": float(predictions_return.mean()),
            "predicted_return_std": float(predictions_return.std(ddof=0)),
            "actual_return_mean": float(y_test_return.mean()),
            "actual_return_std": float(y_test_return.std(ddof=0)),
        },
        "accepted": accepted,
        "artifacts": {
            "model_path": str(model_path),
            "scaler_path": str(scaler_path),
            "plot_path": str(plot_path),
            "dashboard_model_path": str(dashboard_model_path),
            "dashboard_scaler_path": str(dashboard_scaler_path),
        },
    }

    print(f"\n{symbol} metrics")
    print(f"Baseline RMSE: {baseline_rmse:.6f}")
    print(f"Baseline MAE:  {baseline_mae:.6f}")
    print(f"LSTM RMSE:     {lstm_rmse:.6f}")
    print(f"LSTM MAE:      {lstm_mae:.6f}")
    print(f"Accepted:      {accepted}")
    print(f"Saved model:   {model_path}")
    print(f"Saved scaler:  {scaler_path}")
    print(f"Saved plot:    {plot_path}")

    return metrics


def save_metrics(metrics_summary, artifacts_dir):
    metrics_path = artifacts_dir / "metrics_summary.json"
    metrics_path.write_text(json.dumps(metrics_summary, indent=2), encoding="utf-8")
    print(f"\nSaved metrics summary: {metrics_path}")


def run_pipeline(dataset_path=DEFAULT_DATASET_PATH, artifacts_dir=DEFAULT_ARTIFACTS_DIR):
    tf_modules = require_tensorflow()
    if tf_modules is None:
        return None

    dataset_path = Path(dataset_path)
    artifacts_dir = Path(artifacts_dir)
    dashboard_models_dir = DEFAULT_DASHBOARD_MODELS_DIR
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    dashboard_models_dir.mkdir(parents=True, exist_ok=True)

    print(f"Resolved dataset path: {dataset_path.resolve()}")
    if not dataset_path.exists():
        raise FileNotFoundError(f"Dataset not found at: {dataset_path}")

    df = pd.read_parquet(dataset_path)
    df = validate_dataset(df)

    print(f"Dataset shape: {df.shape}")
    print("Symbol counts:", df["symbol"].value_counts().to_dict())
    print("Timestamp range:", df["timestamp"].min(), "->", df["timestamp"].max())
    print("Features:", FEATURE_COLUMNS)
    print(f"Window size: {WINDOW_SIZE}")
    print(f"Max epochs: {MAX_EPOCHS}")
    print(f"Batch size: {BATCH_SIZE}")

    metrics_summary = [
        train_symbol_pipeline(df, symbol, artifacts_dir, dashboard_models_dir, tf_modules)
        for symbol in SYMBOLS
    ]
    save_metrics(metrics_summary, artifacts_dir)

    print("\nFinal LSTM summary")
    for item in metrics_summary:
        print(
            f"{item['symbol']}: accepted={item['accepted']} | "
            f"baseline RMSE={item['baseline']['rmse']:.6f} | "
            f"LSTM RMSE={item['lstm']['rmse']:.6f}"
        )

    return metrics_summary


if __name__ == "__main__":
    result = run_pipeline()
    if result is None:
        sys.exit(0)
