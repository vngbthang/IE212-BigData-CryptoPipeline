# Khoa - Dự Đoán Độ Biến Động Với XGBoost

Tài liệu này mô tả phần triển khai mô hình `XGBoost` để dự báo `độ biến động thị trường ở bước thời gian tiếp theo` từ bộ dữ liệu `ml_training_data_90days.parquet`.

## 1. Mục tiêu

Mục tiêu của phần này là xây dựng mô hình học máy dạng cây quyết định nâng cao để dự báo mức độ rủi ro thị trường, cụ thể là giá trị `volatility_30m` của nến tiếp theo.

Điều kiện nghiệm thu:

- `RMSE test` của mô hình `XGBoost` phải nhỏ hơn `RMSE test` của mô hình cơ sở `Baseline SMA`.

## 2. Dataset và biến sử dụng

Dataset đầu vào:

- `ml_training_data_90days.parquet`

Các cột gốc được dùng làm đầu vào theo đặc tả:

- `close`
- `volume`
- `log_returns`
- `z_score`

Biến mục tiêu:

- `target_next_volatility = volatility_30m.shift(-1)`

Lưu ý:

- Dòng cuối mỗi coin bị loại bỏ sau `shift(-1)` vì target bị `NaN`.
- Mô hình được huấn luyện riêng cho `BTC/USDT` và `ETH/USDT`.

## 3. Baseline

Baseline dùng phương pháp `Simple Moving Average` theo đúng đặc tả:

- Dự báo `volatility_30m[t+1] = volatility_30m[t]`

Chỉ số đánh giá baseline:

- `RMSE` trên validation
- `RMSE` trên test

Mốc nghiệm thu chính là `baseline_rmse_test`.

## 4. Tiền xử lý và chia dữ liệu

Pipeline thực hiện các bước sau:

1. Đọc dataset từ biến môi trường `DATASET_PATH` hoặc mặc định từ `../ml_training_data_90days.parquet`.
2. Kiểm tra đủ 10 cột theo đặc tả.
3. Kiểm tra không có `NaN`.
4. Kiểm tra `timestamp` tăng dần trong từng `symbol`.
5. Tách riêng dữ liệu theo từng coin.
6. Tạo `target_next_volatility = volatility_30m.shift(-1)`.
7. Tạo thêm các feature engineering chỉ từ 4 cột gốc `close`, `volume`, `log_returns`, `z_score`.
8. Loại bỏ các dòng đầu không đủ lịch sử do lag và rolling window.
9. Chia dữ liệu theo thời gian với tỷ lệ:

- `train = 70%`
- `validation = 15%`
- `test = 15%`

Không có bước `shuffle`, không dùng test để tuning.

## 5. Feature Engineering

Ngoài 4 cột gốc, pipeline tạo thêm các đặc trưng suy diễn từ chính các cột này để tăng khả năng dự báo:

- `lag features` cho `close`, `volume`, `log_returns`, `z_score` tại các độ trễ `1, 5, 15, 30, 60`
- `abs_log_returns`
- `rolling mean` và `rolling std` trên các cửa sổ `5, 15, 30, 60`
- các proxy volatility được suy ra từ `log_returns`

Nguyên tắc giữ nguyên:

- Không dùng trực tiếp `volatility_30m` hiện tại làm input feature cho model.
- Mọi feature đều chỉ dùng thông tin quá khứ, tránh leakage.

## 6. Kiến trúc và huấn luyện

Mô hình dùng `XGBRegressor` với cấu hình nền:

```python
XGBRegressor(
    objective="reg:squarederror",
    random_state=42,
    n_jobs=-1,
    tree_method="hist",
    eval_metric="rmse",
)
```

Hyperparameter tuning tập trung vào các tham số cốt lõi:

- `learning_rate`
- `max_depth`
- `n_estimators`

Grid tìm kiếm tầng 1:

- `learning_rate`: `0.03`, `0.05`, `0.1`
- `max_depth`: `3`, `5`, `7`
- `n_estimators`: `100`, `200`, `300`

Nếu kết quả tầng 1 vẫn chưa vượt baseline validation, pipeline mở rộng tầng 2 với:

- `subsample`
- `colsample_bytree`
- `min_child_weight`

Sau khi chọn được `best_params` theo `validation RMSE`, mô hình sẽ:

1. Refit trên `train + validation`
2. Đánh giá đúng một lần trên `test`
3. Xuất model `.pkl`
4. Xuất biểu đồ `feature importance`

## 7. File triển khai

Các file chính của phần này:

- [xgboost_volatility_pipeline.py](/home/khoa/BigDataProject/IE212-BigData-CryptoPipeline/xgboost_volatility_pipeline.py:1): pipeline train, evaluate, export artifacts
- [xgboost_volatility_ml.ipynb](/home/khoa/BigDataProject/IE212-BigData-CryptoPipeline/xgboost_volatility_ml.ipynb:1): notebook entrypoint để chạy pipeline
- [requirements-ml.txt](/home/khoa/BigDataProject/IE212-BigData-CryptoPipeline/requirements-ml.txt:1): dependencies ML

## 8. Artifacts đầu ra

Pipeline xuất các artifacts sau vào thư mục [artifacts/xgboost](/home/khoa/BigDataProject/IE212-BigData-CryptoPipeline/artifacts/xgboost):

- `xgboost_vol_btcusdt.pkl`
- `xgboost_vol_ethusdt.pkl`
- `metrics_summary.json`
- `metrics_summary.csv`
- `feature_importance_btcusdt.png`
- `feature_importance_ethusdt.png`

Hai file model `.pkl` là `joblib bundle` chứa:

- model đã train
- danh sách `feature_columns`
- `best_params`
- chế độ dự báo
- metadata cần thiết để tái sử dụng model

## 9. Kết quả nghiệm thu hiện tại

Theo [metrics_summary.json](/home/khoa/BigDataProject/IE212-BigData-CryptoPipeline/artifacts/xgboost/metrics_summary.json:1), cả hai coin đều đã vượt baseline:

| Symbol | Baseline RMSE Test | XGBoost RMSE Test | Accepted |
| --- | ---: | ---: | --- |
| `BTC/USDT` | `2.3631859713898763e-05` | `1.767691660057757e-05` | `true` |
| `ETH/USDT` | `4.116352038788613e-05` | `3.043713735124203e-05` | `true` |

Kết luận:

- `BTC/USDT`: `PASS`
- `ETH/USDT`: `PASS`
- Toàn bộ phần `Khoa - XGBoost volatility` đã đạt tiêu chuẩn nghiệm thu.

## 10. Cách chạy lại pipeline

Từ thư mục repo:

```bash
python -m pip install -r requirements-ml.txt
python xgboost_volatility_pipeline.py
```

Hoặc chạy bằng notebook:

```bash
jupyter notebook xgboost_volatility_ml.ipynb
```

Nếu muốn chỉ định đường dẫn dataset khác:

```bash
DATASET_PATH=/path/to/ml_training_data_90days.parquet python xgboost_volatility_pipeline.py
```

## 11. Tóm tắt ngắn

Phần của Khoa đã hoàn thành các yêu cầu chính:

- Có baseline SMA
- Có chia dữ liệu theo thời gian
- Có huấn luyện `XGBRegressor`
- Có tối ưu hyperparameter
- Có biểu đồ feature importance
- Có model `.pkl`
- Có metrics tổng hợp
- Có `RMSE test` thấp hơn baseline cho cả `BTC/USDT` và `ETH/USDT`
