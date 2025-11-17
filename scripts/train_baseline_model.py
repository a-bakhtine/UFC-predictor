import os
import pandas as pd
import numpy as np
import joblib
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, roc_auc_score, classification_report
from db import get_engine
from config import logger


def load_matchups() -> pd.DataFrame:
    """Load fight-level matchup dataset from the database"""
    engine = get_engine()
    df = pd.read_sql("SELECT * FROM fight_matchups", engine)
    
    if "event_date" in df.columns:
        df["event_date"] = pd.to_datetime(df["event_date"])
    return df


def make_feature_matrix(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series, list[str]]:
    """
    Build feature matrix (X) and labels (y) from fight_matchups DataFrame
    Uses diff_ columns as features and f1_win as the label
    Returns a tuple of (X, y, feature_columns)
    """
    if "f1_win" not in df.columns:
        raise ValueError("fight_matchups table must contain column 'f1_win'")
    
    y = df["f1_win"].astype(int)
    
    # use only diff_ features
    diff_cols = [c for c in df.columns if c.startswith("diff_")]
    if not diff_cols:
        raise ValueError("No diff_ columns found in fight_matchups")
    
    X = df[diff_cols].copy()
    
    # handle infinities and NaNs
    X = X.replace([np.inf, -np.inf], np.nan).fillna(0.0)
    
    return X, y, diff_cols


def stratified_train_test_split(
    df: pd.DataFrame, 
    test_size: float = 0.2,
    random_state: int = 42
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Split fight_matchups using stratified sampling to ensure both classes in train/test
    Returns a tuple of (train_df, test_df)
    """
    if "f1_win" not in df.columns:
        raise ValueError("DataFrame must have f1_win column for stratification")
    
    # check class distribution
    class_counts = df["f1_win"].value_counts()
    logger.info(f"Class distribution: {class_counts.to_dict()}")
    
    if len(class_counts) < 2:
        raise ValueError(
            f"Only one class found in data: {class_counts.index[0]}. "
            "Cannot train a binary classifier."
        )
    
    train_df, test_df = train_test_split(
        df, 
        test_size=test_size, 
        random_state=random_state,
        stratify=df["f1_win"]
    )
    
    logger.info(f"Stratified split: {len(train_df)} train, {len(test_df)} test")
    return train_df, test_df


def augment_with_mirrors(df: pd.DataFrame) -> pd.DataFrame:
    """
    For each fight row where fighter1 is the winner, create a mirrored row
    where fighter1 is the loser, fighter2 is the winner, f1_win = 0,
    and all diff_* features are negated
    **Gives both positive and negative examples.
    """
    df = df.copy()

    # original rows: fighter1 is the winner in your scraped data
    df["f1_win"] = 1  # just to be explicit

    # create mirrored copy
    mirror = df.copy()

    # swap fighter1_id and fighter2_id if present
    if "fighter1_id" in mirror.columns and "fighter2_id" in mirror.columns:
        tmp = mirror["fighter1_id"].copy()
        mirror["fighter1_id"] = mirror["fighter2_id"]
        mirror["fighter2_id"] = tmp

    # swap fighter1_name and fighter2_name if present
    if "fighter1_name" in mirror.columns and "fighter2_name" in mirror.columns:
        tmp = mirror["fighter1_name"].copy()
        mirror["fighter1_name"] = mirror["fighter2_name"]
        mirror["fighter2_name"] = tmp

    # swap any f1_* and f2_* feature columns (for consistency / debugging),
    # even though the model only uses diff_*.
    f1_cols = [c for c in mirror.columns if c.startswith("f1_")]
    for f1_col in f1_cols:
        suffix = f1_col[3:]  # strip "f1_"
        f2_col = f"f2_{suffix}"
        if f2_col in mirror.columns:
            tmp = mirror[f1_col].copy()
            mirror[f1_col] = mirror[f2_col]
            mirror[f2_col] = tmp

    # flip sign of all diff_* columns
    diff_cols = [c for c in mirror.columns if c.startswith("diff_")]
    for col in diff_cols:
        mirror[col] = -mirror[col]

    # in mirrored rows, fighter1 is now the loser
    mirror["f1_win"] = 0

    # combine original (all f1_win=1) and mirrored (all f1_win=0)
    full = pd.concat([df, mirror], ignore_index=True)
    logger.info(
        f"Augmented with mirrored examples: "
        f"original {len(df)}, mirrored {len(mirror)}, total {len(full)}"
    )

    # sanity-check new class distribution
    logger.info(f"Augmented class distribution: {full['f1_win'].value_counts().to_dict()}")

    return full


def evaluate_model(model, X_test: pd.DataFrame, y_test: pd.Series) -> dict:
    """
    Evaluate model performance on test set.
    Returns a dict with accuracy and auc metrics
    """
    y_pred = model.predict(X_test)
    y_proba = model.predict_proba(X_test)[:, 1]
    
    acc = accuracy_score(y_test, y_pred)
    
    try:
        auc = roc_auc_score(y_test, y_proba)
    except ValueError:
        auc = float("nan")
    
    logger.info(f"Test accuracy: {acc:.3f}")
    logger.info(f"Test ROC AUC: {auc:.3f}")
    logger.info("Classification report:\n" + classification_report(y_test, y_pred, digits=3))
    
    return {"accuracy": acc, "auc": auc}


def train_baseline_model():
    """
    Train a baseline logistic regression model to predict f1_win
    from diff_ features and save to disk.
    """
    logger.info("Loading fight_matchups from DB")
    df = load_matchups()
    logger.info(f"Loaded {len(df)} matchup rows")

    # log original distribution (should be all 1s right now)
    orig_counts = df["f1_win"].value_counts()
    logger.info(f"Original class distribution: {orig_counts.to_dict()}")

    # augment with mirrored examples to get both classes
    df_aug = augment_with_mirrors(df)

    # stratified split (ensures both classes in train/test)
    train_df, test_df = stratified_train_test_split(df_aug, test_size=0.2)
    
    # build feature matrices
    X_train, y_train, feature_cols = make_feature_matrix(train_df)
    X_test, y_test, _ = make_feature_matrix(test_df)
    
    logger.info(f"Using {len(feature_cols)} diff_ features")
    logger.info(f"Train size: {X_train.shape[0]}, Test size: {X_test.shape[0]}")
    
    # train model
    logger.info("Training LogisticRegression baseline model")
    model = LogisticRegression(max_iter=1000, random_state=42)
    model.fit(X_train, y_train)
    
    # evaluate
    evaluate_model(model, X_test, y_test)
    
    # save model
    os.makedirs("models", exist_ok=True)
    model_path = "models/baseline_logreg.pkl"
    
    joblib.dump(
        {"model": model, "feature_cols": feature_cols},
        model_path,
    )
    
    logger.info(f"Saved baseline model to {model_path}")


if __name__ == "__main__":
    train_baseline_model()
