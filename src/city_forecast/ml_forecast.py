from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from .dbsql import has_databricks_creds, query_to_pandas, write_dataframe_as_values_table


ROOT = Path(__file__).resolve().parents[2]
ARTIFACTS_DIR = ROOT / "artifacts" / "city_forecast"
DATA_DIR = ROOT / "data" / "city_forecast"


CATALOG = "fp_hack"
SCHEMA = "alexander_groth_hackathon"

TRAINING_TABLE = f"{CATALOG}.{SCHEMA}.city_daily_sales_weather_training"
FORECAST_WEATHER_TABLE = f"{CATALOG}.{SCHEMA}.city_daily_weather_forecast"
FORECAST_OUTPUT_TABLE = f"{CATALOG}.{SCHEMA}.city_daily_sales_forecast"


FEATURE_COLUMNS = [
    "temp_max",
    "temp_min",
    "temp_avg",
    "cloud_cover",
    "precip_total",
    "rain",
    "snow",
    "uv",
    "humidity",
    "wind_speed",
]
TARGET_COLUMN = "sales_amount"
CATEGORICAL_COLUMNS = ["city", "state"]


@dataclass(frozen=True)
class TrainResult:
    model_name: str
    mae: float
    r2: float
    train_rows: int
    valid_rows: int
    trained_at_iso: str
    model_path: Path
    metrics_path: Path


def _ensure_dirs() -> None:
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def _date_split_per_city(df: pd.DataFrame, valid_days_per_city: int = 5) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date

    def split_group(group: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        group = group.sort_values("date")
        if len(group) <= valid_days_per_city:
            # too small: leave-one-out style
            return group.iloc[:-1], group.iloc[-1:]
        return group.iloc[:-valid_days_per_city], group.iloc[-valid_days_per_city:]

    train_parts = []
    valid_parts = []
    for _, g in df.groupby(["city", "state"], dropna=False):
        tr, va = split_group(g)
        train_parts.append(tr)
        valid_parts.append(va)

    train_df = pd.concat(train_parts, ignore_index=True)
    valid_df = pd.concat(valid_parts, ignore_index=True)
    return train_df, valid_df


def _build_model(model) -> Pipeline:
    numeric_features = FEATURE_COLUMNS
    categorical_features = CATEGORICAL_COLUMNS

    preprocessor = ColumnTransformer(
        transformers=[
            (
                "num",
                Pipeline(
                    steps=[
                        ("imputer", SimpleImputer(strategy="median")),
                        ("scaler", StandardScaler(with_mean=False)),
                    ]
                ),
                numeric_features,
            ),
            (
                "cat",
                Pipeline(
                    steps=[
                        ("imputer", SimpleImputer(strategy="most_frequent")),
                        ("onehot", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
                    ]
                ),
                categorical_features,
            ),
        ],
        remainder="drop",
    )

    return Pipeline([("pre", preprocessor), ("model", model)])


def train_select_best(df: pd.DataFrame) -> tuple[Pipeline, TrainResult]:
    if df.empty:
        raise ValueError("Training dataframe is empty.")

    train_df, valid_df = _date_split_per_city(df)

    X_train = train_df[FEATURE_COLUMNS + CATEGORICAL_COLUMNS]
    y_train = train_df[TARGET_COLUMN].astype(float)
    X_valid = valid_df[FEATURE_COLUMNS + CATEGORICAL_COLUMNS]
    y_valid = valid_df[TARGET_COLUMN].astype(float)

    candidates: list[tuple[str, Pipeline]] = [
        ("linear_regression", _build_model(LinearRegression())),
        (
            "random_forest",
            _build_model(RandomForestRegressor(n_estimators=400, max_depth=10, random_state=42)),
        ),
        (
            "gradient_boosting",
            _build_model(GradientBoostingRegressor(random_state=42)),
        ),
    ]

    best = None
    best_mae = float("inf")
    scores: dict[str, dict[str, float]] = {}
    for name, pipe in candidates:
        pipe.fit(X_train, y_train)
        pred = pipe.predict(X_valid)
        mae = float(mean_absolute_error(y_valid, pred))
        r2 = float(r2_score(y_valid, pred)) if len(np.unique(y_valid)) > 1 else float("nan")
        scores[name] = {"mae": mae, "r2": r2}
        if mae < best_mae:
            best_mae = mae
            best = (name, pipe, mae, r2)

    assert best is not None
    best_name, best_pipe, best_mae, best_r2 = best

    _ensure_dirs()
    trained_at_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    model_path = ARTIFACTS_DIR / "model.joblib"
    metrics_path = ARTIFACTS_DIR / "metrics.json"

    joblib.dump(best_pipe, model_path)
    metrics_payload = {
        "selected_model": best_name,
        "trained_at": trained_at_iso,
        "train_rows": int(len(train_df)),
        "valid_rows": int(len(valid_df)),
        "scores": scores,
    }
    metrics_path.write_text(json.dumps(metrics_payload, indent=2), encoding="utf-8")

    result = TrainResult(
        model_name=best_name,
        mae=best_mae,
        r2=best_r2,
        train_rows=int(len(train_df)),
        valid_rows=int(len(valid_df)),
        trained_at_iso=trained_at_iso,
        model_path=model_path,
        metrics_path=metrics_path,
    )
    return best_pipe, result


def predict_forecast(model: Pipeline, forecast_weather: pd.DataFrame) -> pd.DataFrame:
    if forecast_weather.empty:
        raise ValueError("Forecast weather dataframe is empty.")

    df = forecast_weather.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    X = df[FEATURE_COLUMNS + CATEGORICAL_COLUMNS]
    preds = model.predict(X)

    out = df[["city", "state", "date"] + FEATURE_COLUMNS].copy()
    out["sales_amount_pred"] = preds.astype(float)
    return out


def load_inputs_from_databricks(*, warehouse_id: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    training = query_to_pandas(f"SELECT * FROM {TRAINING_TABLE}", warehouse_id=warehouse_id)
    forecast_weather = query_to_pandas(f"SELECT * FROM {FORECAST_WEATHER_TABLE}", warehouse_id=warehouse_id)
    return training, forecast_weather


def save_local_csvs(training: pd.DataFrame, forecast_weather: pd.DataFrame) -> None:
    _ensure_dirs()
    training.to_csv(DATA_DIR / "city_daily_sales_weather_training.csv", index=False)
    forecast_weather.to_csv(DATA_DIR / "city_daily_weather_forecast.csv", index=False)


def load_local_csvs() -> tuple[pd.DataFrame, pd.DataFrame]:
    training_path = DATA_DIR / "city_daily_sales_weather_training.csv"
    forecast_path = DATA_DIR / "city_daily_weather_forecast.csv"
    if not training_path.exists() or not forecast_path.exists():
        raise FileNotFoundError(
            "Missing local CSV inputs. Either provide Databricks credentials and run with "
            "`--warehouse-id`, or create:\n"
            f"- {training_path}\n"
            f"- {forecast_path}"
        )
    return pd.read_csv(training_path), pd.read_csv(forecast_path)


def write_forecast_to_databricks(
    forecast_df: pd.DataFrame, *, warehouse_id: str, model_name: str, trained_at_iso: str
) -> None:
    df = forecast_df.copy()
    df["model_name"] = model_name
    df["trained_at"] = trained_at_iso

    # Keep it minimal and stable for the app: join to weather table by city/state/date if needed.
    column_types = {
        "city": "STRING",
        "state": "STRING",
        "date": "DATE",
        "sales_amount_pred": "DOUBLE",
        "model_name": "STRING",
        "trained_at": "STRING",
    }
    write_dataframe_as_values_table(
        df=df[list(column_types.keys())],
        warehouse_id=warehouse_id,
        full_table_name=FORECAST_OUTPUT_TABLE,
        column_types_sql=column_types,
    )


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Train model and write 10-day city sales forecast.")
    parser.add_argument(
        "--warehouse-id",
        default=os.getenv("DATABRICKS_WAREHOUSE_ID", ""),
        help="Databricks SQL Warehouse ID (e.g. cfe55031a9b649cb).",
    )
    parser.add_argument(
        "--no-databricks",
        action="store_true",
        help="Force local CSV mode (reads/writes under data/city_forecast/).",
    )
    parser.add_argument(
        "--write-databricks",
        action="store_true",
        help="Write forecast output table back to Databricks (requires creds + warehouse id).",
    )
    args = parser.parse_args()

    use_databricks = (not args.no_databricks) and has_databricks_creds() and bool(args.warehouse_id)

    if use_databricks:
        training, forecast_weather = load_inputs_from_databricks(warehouse_id=args.warehouse_id)
        save_local_csvs(training, forecast_weather)
    else:
        training, forecast_weather = load_local_csvs()

    model, train_result = train_select_best(training)
    forecast_df = predict_forecast(model, forecast_weather)

    _ensure_dirs()
    forecast_csv = DATA_DIR / "city_daily_sales_forecast.csv"
    forecast_df.to_csv(forecast_csv, index=False)

    if args.write_databricks:
        if not (has_databricks_creds() and args.warehouse_id):
            raise RuntimeError("Missing Databricks credentials or --warehouse-id.")
        write_forecast_to_databricks(
            forecast_df,
            warehouse_id=args.warehouse_id,
            model_name=train_result.model_name,
            trained_at_iso=train_result.trained_at_iso,
        )

    print(
        json.dumps(
            {
                "selected_model": train_result.model_name,
                "mae": train_result.mae,
                "r2": train_result.r2,
                "train_rows": train_result.train_rows,
                "valid_rows": train_result.valid_rows,
                "trained_at": train_result.trained_at_iso,
                "model_path": str(train_result.model_path),
                "metrics_path": str(train_result.metrics_path),
                "forecast_csv": str(forecast_csv),
                "databricks_used": bool(use_databricks),
                "databricks_written": bool(args.write_databricks),
                "forecast_table": FORECAST_OUTPUT_TABLE if args.write_databricks else None,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()

