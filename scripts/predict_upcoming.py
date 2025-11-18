import os
import argparse
import numpy as np
import pandas as pd
import joblib
from db import get_engine
from config import logger


MODEL_PATH = "models/baseline_logreg.pkl"

def load_model():
    """Load the trained baseline model and feature list"""
    if not os.path.exists(MODEL_PATH):
        raise FileNotFoundError(
            f"Model file not found at {MODEL_PATH}. "
            "Run scripts/train_baseline_model.py first."
        )
    
    bundle = joblib.load(MODEL_PATH)
    model = bundle["model"]
    # list of diff_* column names
    feature_cols = bundle["feature_cols"]  
    return model, feature_cols


def load_fighter_features() -> pd.DataFrame:
    """Load fighter_features from the database"""
    engine = get_engine()
    df = pd.read_sql("SELECT * FROM fighter_features", engine)
    return df


def resolve_fighter(term: str, feats: pd.DataFrame) -> pd.Series:
    """
    Resolve a fighter by fighter_id or by (partial) name match
    Returns a single row (pd.Series) from fighter_features
    """
    # exact fighter_id match
    if term in feats["fighter_id"].values:
        row = feats.loc[feats["fighter_id"] == term].iloc[0]
        logger.info(f"Resolved '{term}' as fighter_id {row['fighter_id']} ({row['name']})")
        return row

    # case-insensitive partial name match
    mask = feats["name"].str.contains(term, case=False, na=False)
    matches = feats.loc[mask]

    if matches.empty:
        raise ValueError(f"No fighter found matching '{term}'")

    if len(matches) > 1:
        logger.warning(
            f"Multiple fighters matched '{term}'. "
            f"Using first match: {matches.iloc[0]['name']} "
            f"(fighter_id={matches.iloc[0]['fighter_id']})"
        )

    row = matches.iloc[0]
    logger.info(f"Resolved '{term}' as fighter_id {row['fighter_id']} ({row['name']})")
    return row


def build_feature_row(
    f1_row: pd.Series,
    f2_row: pd.Series,
    diff_feature_cols: list[str],
) -> pd.DataFrame:
    """
    Given two fighter_features rows and the model's expected diff_* columns,
    build a single-row DataFrame X matching the model's feature order
    diff_feature_cols: e.g. ["diff_career_win_rate", "diff_last3_sig_strikes_per_min", ...]
    """
    # fighter_features columns: fighter_id, name, career_..., last3_...
    # diff_feature_cols: "diff_" + base column name
    row_values = {}

    for diff_col in diff_feature_cols:
        if not diff_col.startswith("diff_"):
            base = diff_col
        else:
            base = diff_col[len("diff_"):]  

        if base in f1_row.index and base in f2_row.index:
            f1_val = f1_row[base]
            f2_val = f2_row[base]
            # safe numeric cast
            try:
                f1_val = float(f1_val) if pd.notna(f1_val) else 0.0
            except Exception:
                f1_val = 0.0
            try:
                f2_val = float(f2_val) if pd.notna(f2_val) else 0.0
            except Exception:
                f2_val = 0.0

            diff_val = f1_val - f2_val
        else:
            # if the base feature doesn't exist (schema changed, etc.), fall back to 0
            logger.warning(
                f"Base feature '{base}' not found in fighter_features; "
                f"setting {diff_col} = 0"
            )
            diff_val = 0.0

        row_values[diff_col] = diff_val

    # create DataFrame with a single row, ensure correct column order
    X = pd.DataFrame([row_values], columns=diff_feature_cols)
    X = X.replace([np.inf, -np.inf], np.nan).fillna(0.0)

    return X


def predict_matchup(f1_term: str, f2_term: str):
    """
    Predict probability that fighter1 (f1_term) wins against fighter2 (f2_term)
    """
    logger.info(f"Loading model from {MODEL_PATH}")
    model, feature_cols = load_model()

    logger.info("Loading fighter_features from DB")
    feats = load_fighter_features()

    # resolve fighters
    f1_row = resolve_fighter(f1_term, feats)
    f2_row = resolve_fighter(f2_term, feats)

    f1_name = f1_row["name"]
    f2_name = f2_row["name"]

    logger.info(f"Building feature vector for matchup: {f1_name} vs {f2_name}")
    X = build_feature_row(f1_row, f2_row, feature_cols)

    # predict
    proba = model.predict_proba(X)[0, 1]  # P(f1_win)
    pred = model.predict(X)[0]

    # output
    print("\n================ UFC Matchup Prediction ================\n")
    print(f"Fighter 1: {f1_name} (fighter_id={f1_row['fighter_id']})")
    print(f"Fighter 2: {f2_name} (fighter_id={f2_row['fighter_id']})\n")
    print(f"Model predicts P(Fighter 1 wins) = {proba:.3f}")

    if pred == 1:
        print(f"\nPredicted winner: {f1_name}")
    else:
        print(f"\nPredicted winner: {f2_name}")

    print("\n(Interpretation is based currently on career and last 3 stats.)\n")


def main():
    parser = argparse.ArgumentParser(
        description="Predict winner probability for a UFC matchup using the baseline model."
    )
    parser.add_argument("fighter1", help="Fighter 1 (name or fighter_id)")
    parser.add_argument("fighter2", help="Fighter 2 (name or fighter_id)")

    args = parser.parse_args()
    predict_matchup(args.fighter1, args.fighter2)


if __name__ == "__main__":
    main()