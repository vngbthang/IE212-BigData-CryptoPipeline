import json
import os
from itertools import product
from pathlib import Path

import joblib
import matplotlib
import numpy as np
import pandas as pd
from sklearn.metrics import mean_squared_error
from xgboost import XGBRegressor

matplotlib.use("Agg")
import matplotlib.pyplot as plt


DEFAULT_DATASET_PATH = Path(os.getenv("DATASET_PATH", "../ml_training_data_90days.parquet"))
DEFAULT_ARTIFACTS_DIR = Path(os.getenv("ARTIFACTS_DIR", "artifacts/xgboost"))

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
BASE_FEATURE_COLUMNS = ["close", "volume", "log_returns", "z_score"]
LAG_WINDOWS = [1, 5, 15, 30, 60]
ROLLING_WINDOWS_LONG = [5, 15, 30, 60]
ROLLING_WINDOWS_SHORT = [5, 15, 30]

STAGE_1_GRID = {
    "learning_rate": [0.03, 0.05, 0.1],
    "max_depth": [3, 5, 7],
    "n_estimators": [100, 200, 300],
    "subsample": [1.0],
    "colsample_bytree": [1.0],
    "min_child_weight": [1],
}

STAGE_2_GRID = {
    "learning_rate": [0.03, 0.05, 0.1],
    "max_depth": [3, 5, 7],
    "n_estimators": [100, 200, 300],
    "subsample": [0.8, 1.0],
    "colsample_bytree": [0.8, 1.0],
    "min_child_weight": [1, 5],
}


def rmse(y_true, y_pred):
    return float(np.sqrt(mean_squared_error(y_true, y_pred)))


def next_zero_proxy(log_returns, window=30):
    """Estimate next-step 30-window volatility by dropping the oldest known return and appending 0."""
    arr = log_returns.to_numpy(dtype=float)
    vals = np.full(len(arr), np.nan)
    for index in range(window - 1, len(arr)):
        hist = arr[index - window + 1 : index + 1]
        shifted = np.concatenate([hist[1:], [0.0]])
        vals[index] = np.std(shifted, ddof=1)
    return vals


def validate_dataset(df):
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"Dataset thiếu cột: {missing}")

    extra = [col for col in df.columns if col not in REQUIRED_COLUMNS]
    if extra:
        print(f"Cảnh báo: dataset có thêm cột ngoài đặc tả: {extra}")

    df = df[REQUIRED_COLUMNS].copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

    null_counts = df.isna().sum()
    if null_counts.any():
        raise ValueError(f"Dataset có NaN: {null_counts[null_counts > 0].to_dict()}")

    for symbol, group in df.groupby("symbol"):
        if not group["timestamp"].is_monotonic_increasing:
            raise ValueError(f"Timestamp không tăng dần cho {symbol}")

    return df


def split_time_series(df_symbol):
    n = len(df_symbol)
    train_end = int(n * 0.70)
    val_end = int(n * 0.85)

    train_df = df_symbol.iloc[:train_end].copy()
    val_df = df_symbol.iloc[train_end:val_end].copy()
    test_df = df_symbol.iloc[val_end:].copy()

    if len(train_df) == 0 or len(val_df) == 0 or len(test_df) == 0:
        raise ValueError("Một trong các split train/val/test rỗng")

    if train_df["timestamp"].max() >= val_df["timestamp"].min():
        raise ValueError("Validation bị chồng thời gian với train")
    if val_df["timestamp"].max() >= test_df["timestamp"].min():
        raise ValueError("Test bị chồng thời gian với validation")

    return train_df, val_df, test_df


def build_features(df_coin):
    df_feat = df_coin.copy()
    df_feat["target_next_volatility"] = df_feat["volatility_30m"].shift(-1)
    df_feat["abs_log_returns"] = df_feat["log_returns"].abs()
    df_feat["proxy_current_volatility_30m"] = df_feat["log_returns"].rolling(30).std()
    df_feat["proxy_next_zero"] = next_zero_proxy(df_feat["log_returns"], 30)
    df_feat["log_returns_roll_mean_30"] = df_feat["log_returns"].rolling(30).mean()

    feature_columns = list(BASE_FEATURE_COLUMNS) + [
        "abs_log_returns",
        "proxy_current_volatility_30m",
        "proxy_next_zero",
        "log_returns_roll_mean_30",
    ]

    for column in BASE_FEATURE_COLUMNS:
        for lag in LAG_WINDOWS:
            feature_name = f"{column}_lag_{lag}"
            df_feat[feature_name] = df_feat[column].shift(lag)
            feature_columns.append(feature_name)

    df_feat["outgoing_log_return_30"] = df_feat["log_returns"].shift(29)
    feature_columns.append("outgoing_log_return_30")

    for window in ROLLING_WINDOWS_LONG:
        feature_name = f"log_returns_roll_std_{window}"
        df_feat[feature_name] = df_feat["log_returns"].rolling(window).std()
        feature_columns.append(feature_name)

        feature_name = f"abs_log_returns_roll_mean_{window}"
        df_feat[feature_name] = df_feat["abs_log_returns"].rolling(window).mean()
        feature_columns.append(feature_name)

    for column in ["volume", "close", "z_score"]:
        for window in ROLLING_WINDOWS_SHORT:
            mean_name = f"{column}_roll_mean_{window}"
            std_name = f"{column}_roll_std_{window}"
            df_feat[mean_name] = df_feat[column].rolling(window).mean()
            df_feat[std_name] = df_feat[column].rolling(window).std()
            feature_columns.extend([mean_name, std_name])

    before_drop = len(df_feat)
    df_feat = df_feat.dropna(subset=["target_next_volatility"]).copy()
    shift_dropped_rows = before_drop - len(df_feat)
    if shift_dropped_rows != 1:
        raise ValueError(
            f"{df_coin['symbol'].iloc[0]}: số dòng mất sau shift phải bằng 1, hiện tại là {shift_dropped_rows}"
        )

    df_feat["target_residual"] = df_feat["target_next_volatility"] - df_feat["proxy_next_zero"]

    before_history_drop = len(df_feat)
    df_feat = df_feat.dropna(subset=feature_columns).reset_index(drop=True)
    history_dropped_rows = before_history_drop - len(df_feat)

    return df_feat, feature_columns, shift_dropped_rows, history_dropped_rows


def compute_baseline_metrics(val_df, test_df):
    baseline_val_pred = val_df["volatility_30m"]
    baseline_test_pred = test_df["volatility_30m"]
    baseline_rmse_val = rmse(val_df["target_next_volatility"], baseline_val_pred)
    baseline_rmse_test = rmse(test_df["target_next_volatility"], baseline_test_pred)
    return baseline_rmse_val, baseline_rmse_test


def build_xy(df_part, feature_columns):
    X = df_part[feature_columns].to_numpy(dtype=np.float32, copy=True)
    y_true = df_part["target_next_volatility"].to_numpy(dtype=np.float32, copy=True)
    y_residual = df_part["target_residual"].to_numpy(dtype=np.float32, copy=True)
    base_proxy = df_part["proxy_next_zero"].to_numpy(dtype=np.float32, copy=True)
    return X, y_true, y_residual, base_proxy


def parameter_rows(grid):
    for values in product(
        grid["learning_rate"],
        grid["max_depth"],
        grid["n_estimators"],
        grid["subsample"],
        grid["colsample_bytree"],
        grid["min_child_weight"],
    ):
        yield {
            "learning_rate": values[0],
            "max_depth": values[1],
            "n_estimators": values[2],
            "subsample": values[3],
            "colsample_bytree": values[4],
            "min_child_weight": values[5],
        }


def tune_stage(X_train, y_train_residual, X_val, y_val_true, val_base_proxy, grid, stage_name):
    best_params = None
    best_rmse = float("inf")
    rows = []
    total = (
        len(grid["learning_rate"])
        * len(grid["max_depth"])
        * len(grid["n_estimators"])
        * len(grid["subsample"])
        * len(grid["colsample_bytree"])
        * len(grid["min_child_weight"])
    )

    for index, params in enumerate(parameter_rows(grid), start=1):
        print(
            f"[{stage_name}] {index}/{total} "
            f"lr={params['learning_rate']} depth={params['max_depth']} "
            f"estimators={params['n_estimators']} subsample={params['subsample']} "
            f"colsample={params['colsample_bytree']} min_child_weight={params['min_child_weight']}",
            flush=True,
        )
        model = XGBRegressor(
            objective="reg:squarederror",
            random_state=42,
            n_jobs=-1,
            tree_method="hist",
            eval_metric="rmse",
            verbosity=0,
            **params,
        )
        model.fit(X_train, y_train_residual)
        val_pred = val_base_proxy + model.predict(X_val)
        val_rmse = rmse(y_val_true, val_pred)
        row = {
            "stage": stage_name,
            **params,
            "validation_rmse": val_rmse,
        }
        rows.append(row)

        if val_rmse < best_rmse:
            best_rmse = val_rmse
            best_params = params
            print(
                f"[{stage_name}] new best validation RMSE: {best_rmse:.8f} with {best_params}",
                flush=True,
            )

    tuning_df = pd.DataFrame(rows).sort_values("validation_rmse").reset_index(drop=True)
    return best_params, best_rmse, tuning_df


def tune_xgboost(X_train, y_train_residual, X_val, y_val_true, val_base_proxy, baseline_rmse_val):
    stage_1_params, stage_1_rmse, stage_1_df = tune_stage(
        X_train, y_train_residual, X_val, y_val_true, val_base_proxy, STAGE_1_GRID, "stage_1"
    )

    if stage_1_rmse < baseline_rmse_val:
        return stage_1_params, stage_1_rmse, stage_1_df, "stage_1"

    stage_2_params, stage_2_rmse, stage_2_df = tune_stage(
        X_train, y_train_residual, X_val, y_val_true, val_base_proxy, STAGE_2_GRID, "stage_2"
    )
    tuning_df = pd.concat([stage_1_df, stage_2_df], ignore_index=True)
    tuning_df = tuning_df.sort_values("validation_rmse").reset_index(drop=True)

    if stage_2_rmse < stage_1_rmse:
        return stage_2_params, stage_2_rmse, tuning_df, "stage_2"

    return stage_1_params, stage_1_rmse, tuning_df, "stage_1"


def save_feature_importance(model_bundle, feature_columns, symbol_slug, artifacts_dir, top_n=20):
    importance = pd.Series(model_bundle["model"].feature_importances_, index=feature_columns)
    importance = importance.sort_values(ascending=False).head(top_n).sort_values(ascending=True)

    fig, ax = plt.subplots(figsize=(10, 7))
    importance.plot(kind="barh", ax=ax, color="#1f77b4")
    ax.set_title(f"Top {top_n} Feature Importance - {symbol_slug.upper()}")
    ax.set_xlabel("Importance")
    ax.set_ylabel("Feature")
    fig.tight_layout()

    output_path = artifacts_dir / f"feature_importance_{symbol_slug}.png"
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return output_path


def train_xgboost_pipeline(dataframe, coin_symbol, artifacts_dir):
    print(f"\n{'=' * 60}")
    print(f"BẮT ĐẦU PIPELINE XGBOOST CHO: {coin_symbol}")
    print(f"{'=' * 60}")

    symbol_slug = coin_symbol.lower().replace("/", "")
    df_coin = dataframe[dataframe["symbol"] == coin_symbol].copy()
    df_coin = df_coin.sort_values("timestamp").reset_index(drop=True)

    feature_df, feature_columns, shift_dropped_rows, history_dropped_rows = build_features(df_coin)
    print(f"Số dòng mất do shift(-1): {shift_dropped_rows}")
    print(f"Số dòng mất thêm do lag/rolling: {history_dropped_rows}")
    print(f"Số feature dùng cho model: {len(feature_columns)}")

    train_df, val_df, test_df = split_time_series(feature_df)
    X_train, y_train_true, y_train_residual, train_base_proxy = build_xy(train_df, feature_columns)
    X_val, y_val_true, y_val_residual, val_base_proxy = build_xy(val_df, feature_columns)
    X_test, y_test_true, y_test_residual, test_base_proxy = build_xy(test_df, feature_columns)

    baseline_rmse_val, baseline_rmse_test = compute_baseline_metrics(val_df, test_df)
    proxy_rmse_val = rmse(y_val_true, val_base_proxy)
    proxy_rmse_test = rmse(y_test_true, test_base_proxy)
    print(f"Baseline RMSE (validation): {baseline_rmse_val:.8f}")
    print(f"Baseline RMSE (test):       {baseline_rmse_test:.8f}")
    print(f"Proxy RMSE (validation):    {proxy_rmse_val:.8f}")
    print(f"Proxy RMSE (test):          {proxy_rmse_test:.8f}")

    best_params, best_val_rmse, tuning_df, search_stage = tune_xgboost(
        X_train, y_train_residual, X_val, y_val_true, val_base_proxy, baseline_rmse_val
    )
    print(f"\nTop 5 cấu hình theo validation RMSE ({search_stage} thắng cuối cùng):")
    print(tuning_df.head(5).to_string(index=False))

    X_train_val = np.concatenate([X_train, X_val], axis=0)
    y_train_val_residual = np.concatenate([y_train_residual, y_val_residual], axis=0)

    final_model = XGBRegressor(
        objective="reg:squarederror",
        random_state=42,
        n_jobs=-1,
        tree_method="hist",
        eval_metric="rmse",
        verbosity=0,
        **best_params,
    )
    final_model.fit(X_train_val, y_train_val_residual)
    test_pred = test_base_proxy + final_model.predict(X_test)
    xgboost_rmse_test = rmse(y_test_true, test_pred)
    accepted = xgboost_rmse_test < baseline_rmse_test

    model_bundle = {
        "model": final_model,
        "feature_columns": feature_columns,
        "prediction_mode": "proxy_next_zero_plus_residual",
        "proxy_feature": "proxy_next_zero",
        "target_column": "target_next_volatility",
        "best_params": best_params,
        "search_stage": search_stage,
    }
    model_path = artifacts_dir / f"xgboost_vol_{symbol_slug}.pkl"
    joblib.dump(model_bundle, model_path)
    importance_path = save_feature_importance(model_bundle, feature_columns, symbol_slug, artifacts_dir)

    metrics = {
        "symbol": coin_symbol,
        "split_sizes": {
            "train": int(len(train_df)),
            "validation": int(len(val_df)),
            "test": int(len(test_df)),
        },
        "rows_dropped": {
            "shift_target": int(shift_dropped_rows),
            "feature_engineering_history": int(history_dropped_rows),
        },
        "feature_count": int(len(feature_columns)),
        "baseline_rmse_val": baseline_rmse_val,
        "baseline_rmse_test": baseline_rmse_test,
        "proxy_rmse_val": proxy_rmse_val,
        "proxy_rmse_test": proxy_rmse_test,
        "xgboost_rmse_val": best_val_rmse,
        "xgboost_rmse_test": xgboost_rmse_test,
        "best_params": best_params,
        "search_stage": search_stage,
        "accepted": bool(accepted),
        "artifacts": {
            "model_path": str(model_path),
            "feature_importance_path": str(importance_path),
        },
    }

    print(f"XGBoost RMSE (validation tốt nhất): {best_val_rmse:.8f}")
    print(f"XGBoost RMSE (test):                {xgboost_rmse_test:.8f}")
    print(f"Best params: {best_params}")
    print(f"Kết luận nghiệm thu: {'PASS' if accepted else 'FAIL'}")

    return metrics


def build_metrics_summary_df(metrics_summary):
    return pd.DataFrame(
        [
            {
                "symbol": item["symbol"],
                "train_size": item["split_sizes"]["train"],
                "validation_size": item["split_sizes"]["validation"],
                "test_size": item["split_sizes"]["test"],
                "feature_count": item["feature_count"],
                "baseline_rmse_val": item["baseline_rmse_val"],
                "baseline_rmse_test": item["baseline_rmse_test"],
                "proxy_rmse_val": item["proxy_rmse_val"],
                "proxy_rmse_test": item["proxy_rmse_test"],
                "xgboost_rmse_val": item["xgboost_rmse_val"],
                "xgboost_rmse_test": item["xgboost_rmse_test"],
                "best_params": json.dumps(item["best_params"], ensure_ascii=False),
                "search_stage": item["search_stage"],
                "accepted": item["accepted"],
            }
            for item in metrics_summary
        ]
    )


def save_metrics(metrics_summary, artifacts_dir):
    metrics_summary_df = build_metrics_summary_df(metrics_summary)
    json_path = artifacts_dir / "metrics_summary.json"
    csv_path = artifacts_dir / "metrics_summary.csv"

    json_path.write_text(json.dumps(metrics_summary, ensure_ascii=False, indent=2), encoding="utf-8")
    metrics_summary_df.to_csv(csv_path, index=False)

    print("\nBẢNG KẾT LUẬN CUỐI CÙNG")
    print(metrics_summary_df.to_string(index=False))

    for item in metrics_summary:
        status = "PASS" if item["accepted"] else "FAIL"
        print(
            f"{item['symbol']}: {status} | "
            f"Baseline test RMSE = {item['baseline_rmse_test']:.8f} | "
            f"XGBoost test RMSE = {item['xgboost_rmse_test']:.8f}"
        )

    print(f"\nSaved metrics JSON: {json_path}")
    print(f"Saved metrics CSV:  {csv_path}")
    return metrics_summary_df


def run_pipeline(dataset_path=DEFAULT_DATASET_PATH, artifacts_dir=DEFAULT_ARTIFACTS_DIR):
    dataset_path = Path(dataset_path)
    artifacts_dir = Path(artifacts_dir)
    artifacts_dir.mkdir(parents=True, exist_ok=True)

    print(f"Resolved dataset path: {dataset_path.resolve()}")
    if not dataset_path.exists():
        raise FileNotFoundError(f"Không tìm thấy dataset tại: {dataset_path}")

    df = pd.read_parquet(dataset_path)
    df = validate_dataset(df)

    print(f"Dataset shape: {df.shape}")
    print("Symbol counts:", df["symbol"].value_counts().to_dict())
    print("Timestamp range:", df["timestamp"].min(), "->", df["timestamp"].max())

    metrics_btc = train_xgboost_pipeline(df, "BTC/USDT", artifacts_dir)
    metrics_eth = train_xgboost_pipeline(df, "ETH/USDT", artifacts_dir)
    metrics_summary = [metrics_btc, metrics_eth]
    metrics_summary_df = save_metrics(metrics_summary, artifacts_dir)
    return metrics_summary, metrics_summary_df


if __name__ == "__main__":
    run_pipeline()
