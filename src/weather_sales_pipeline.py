from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import json

import joblib
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestRegressor
from sklearn.impute import SimpleImputer
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
ARTIFACTS_DIR = ROOT / "artifacts"
MODEL_PATH = ARTIFACTS_DIR / "weather_sales_model.joblib"
METRICS_PATH = ARTIFACTS_DIR / "metrics.json"
DATASET_PATH = DATA_DIR / "weather_sales_demo.csv"


NUMERIC_FEATURES = [
    "temperature_c",
    "precipitation_mm",
    "wind_speed_kmh",
    "humidity_pct",
    "discount_pct",
]
BOOLEAN_FEATURES = ["is_weekend", "is_holiday", "is_rainy"]
CATEGORICAL_FEATURES = ["season"]
TARGET = "sales_amount"


@dataclass
class TrainingResult:
    model_path: Path
    metrics_path: Path
    dataset_path: Path
    row_count: int
    metrics: dict[str, float]


def _season_from_month(month: int) -> str:
    if month in (12, 1, 2):
        return "winter"
    if month in (3, 4, 5):
        return "spring"
    if month in (6, 7, 8):
        return "summer"
    return "autumn"


def generate_demo_dataset(rows: int = 1200, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    dates = pd.date_range("2024-01-01", periods=rows, freq="D")

    temperature = rng.normal(loc=14, scale=11, size=rows).clip(-10, 35)
    precipitation = rng.gamma(shape=1.8, scale=2.2, size=rows)
    precipitation = np.where(rng.random(rows) < 0.42, precipitation, 0).clip(0, 30)
    humidity = rng.normal(loc=64, scale=13, size=rows).clip(20, 100)
    wind_speed = rng.normal(loc=15, scale=6, size=rows).clip(0, 45)
    discount = rng.choice([0, 5, 10, 15, 20, 25], size=rows, p=[0.16, 0.2, 0.24, 0.18, 0.14, 0.08])

    seasons = np.array([_season_from_month(date.month) for date in dates])
    is_weekend = np.array([date.dayofweek >= 5 for date in dates], dtype=int)
    is_holiday = rng.choice([0, 1], size=rows, p=[0.93, 0.07])
    is_rainy = (precipitation > 1.0).astype(int)

    seasonal_lift = np.select(
        [seasons == "winter", seasons == "spring", seasons == "summer", seasons == "autumn"],
        [18, 8, 26, 12],
        default=0,
    )
    rain_penalty = np.where(precipitation > 12, -22, np.where(precipitation > 0, -8, 6))
    comfort_bonus = 38 - np.abs(temperature - 21) * 2.4
    weekend_lift = is_weekend * 24
    holiday_lift = is_holiday * 30
    discount_lift = discount * 3.8
    noise = rng.normal(loc=0, scale=12, size=rows)

    sales_amount = (
        185
        + seasonal_lift
        + rain_penalty
        + comfort_bonus
        + weekend_lift
        + holiday_lift
        + discount_lift
        - humidity * 0.22
        - wind_speed * 0.35
        + noise
    ).clip(35, None)

    customers = (
        42
        + weekend_lift * 0.55
        + discount * 0.7
        + comfort_bonus * 0.45
        + rain_penalty * 0.3
        + rng.normal(loc=0, scale=6, size=rows)
    ).clip(8, None)

    df = pd.DataFrame(
        {
            "date": dates,
            "season": seasons,
            "temperature_c": np.round(temperature, 1),
            "precipitation_mm": np.round(precipitation, 1),
            "wind_speed_kmh": np.round(wind_speed, 1),
            "humidity_pct": np.round(humidity, 1),
            "discount_pct": discount,
            "is_weekend": is_weekend,
            "is_holiday": is_holiday,
            "is_rainy": is_rainy,
            "customer_count": np.round(customers).astype(int),
            "sales_amount": np.round(sales_amount, 2),
        }
    )
    return df


def ensure_demo_dataset() -> Path:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not DATASET_PATH.exists():
        generate_demo_dataset().to_csv(DATASET_PATH, index=False)
    return DATASET_PATH


def load_dataset(csv_path: str | Path | None = None) -> pd.DataFrame:
    path = Path(csv_path) if csv_path else ensure_demo_dataset()
    df = pd.read_csv(path)
    required = set(NUMERIC_FEATURES + BOOLEAN_FEATURES + CATEGORICAL_FEATURES + [TARGET])
    missing = sorted(required.difference(df.columns))
    if missing:
        raise ValueError(
            "Dataset is missing required columns: " + ", ".join(missing)
        )
    return df


def build_pipeline() -> Pipeline:
    preprocessor = ColumnTransformer(
        transformers=[
            (
                "numeric",
                Pipeline(
                    steps=[
                        ("imputer", SimpleImputer(strategy="median")),
                    ]
                ),
                NUMERIC_FEATURES,
            ),
            (
                "categorical",
                Pipeline(
                    steps=[
                        ("imputer", SimpleImputer(strategy="most_frequent")),
                        (
                            "encoder",
                            OneHotEncoder(handle_unknown="ignore", sparse_output=False),
                        ),
                    ]
                ),
                CATEGORICAL_FEATURES,
            ),
            (
                "boolean",
                Pipeline(
                    steps=[
                        ("imputer", SimpleImputer(strategy="most_frequent")),
                    ]
                ),
                BOOLEAN_FEATURES,
            ),
        ],
        remainder="drop",
    )

    model = RandomForestRegressor(
        n_estimators=300,
        max_depth=10,
        min_samples_leaf=3,
        random_state=42,
    )

    return Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            ("model", model),
        ]
    )


def train_model(csv_path: str | Path | None = None) -> TrainingResult:
    df = load_dataset(csv_path)

    X = df[NUMERIC_FEATURES + BOOLEAN_FEATURES + CATEGORICAL_FEATURES]
    y = df[TARGET]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    pipeline = build_pipeline()
    pipeline.fit(X_train, y_train)
    predictions = pipeline.predict(X_test)

    metrics = {
        "mae": float(mean_absolute_error(y_test, predictions)),
        "r2": float(r2_score(y_test, predictions)),
        "train_rows": int(len(X_train)),
        "test_rows": int(len(X_test)),
    }

    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    joblib.dump(pipeline, MODEL_PATH)
    METRICS_PATH.write_text(json.dumps(metrics, indent=2), encoding="utf-8")

    return TrainingResult(
        model_path=MODEL_PATH,
        metrics_path=METRICS_PATH,
        dataset_path=Path(csv_path) if csv_path else DATASET_PATH,
        row_count=len(df),
        metrics=metrics,
    )


def load_model() -> Pipeline:
    if not MODEL_PATH.exists():
        train_model()
    return joblib.load(MODEL_PATH)


def predict_sales(input_frame: pd.DataFrame) -> np.ndarray:
    model = load_model()
    return model.predict(input_frame)


if __name__ == "__main__":
    result = train_model()
    print(json.dumps(
        {
            "dataset_path": str(result.dataset_path),
            "model_path": str(result.model_path),
            "metrics_path": str(result.metrics_path),
            "row_count": result.row_count,
            "metrics": result.metrics,
        },
        indent=2,
    ))
