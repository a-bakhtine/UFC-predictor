import pandas as pd
from db import get_engine
from config import logger


def load_fighter_features(engine) -> pd.DataFrame:
    """Load fighter features from the database"""
    return pd.read_sql("SELECT * FROM fighter_features", engine)


def load_completed_fights(engine) -> pd.DataFrame:
    """Load fights that have a winner (no draws/upcoming fights)"""
    query = """
        SELECT
            fight_id,
            event_name,
            event_date,
            weight_class,
            fighter1_id,
            fighter2_id,
            winner_id
        FROM fights
        WHERE winner_id IS NOT NULL
    """
    df = pd.read_sql(query, engine)
    df["event_date"] = pd.to_datetime(df["event_date"])
    return df


def add_fighter_features(
    fights: pd.DataFrame,
    features: pd.DataFrame,
    fighter_num: int
) -> pd.DataFrame:
    """
    Join fighter features to fights for a specific fighter (1 or 2)
    Returns DataFrame with added features prefixed by f{fighter_num}_
    """
    fighter_col = f"fighter{fighter_num}_id"
    prefix = f"f{fighter_num}_"
    
    # get numeric feature columns (exclude fighter_id and name)
    feature_cols = [c for c in features.columns if c not in ("fighter_id", "name")]
    
    # rename columns with prefix
    rename_map = {
        "fighter_id": fighter_col,
        **{col: f"{prefix}{col}" for col in feature_cols}
    }
    fighter_feats = features[["fighter_id"] + feature_cols].rename(columns=rename_map)
    
    return fights.merge(fighter_feats, on=fighter_col, how="inner")


def add_fighter_names(
    fights: pd.DataFrame,
    features: pd.DataFrame
) -> pd.DataFrame:
    """
    Add fighter names to the fights DataFrame
    """
    names = features[["fighter_id", "name"]]
    
    # add fighter1 names
    f1_names = names.rename(columns={"fighter_id": "fighter1_id", "name": "fighter1_name"})
    fights = fights.merge(f1_names, on="fighter1_id", how="left")
    
    # add fighter2 names
    f2_names = names.rename(columns={"fighter_id": "fighter2_id", "name": "fighter2_name"})
    fights = fights.merge(f2_names, on="fighter2_id", how="left")
    
    return fights


def add_difference_features(df: pd.DataFrame, feature_cols: list[str]) -> pd.DataFrame:
    """
    Add difference features (fighter1 - fighter2) for each numeric feature
    """
    df = df.copy()
    
    for col in feature_cols:
        f1_col = f"f1_{col}"
        f2_col = f"f2_{col}"
        
        if f1_col in df.columns and f2_col in df.columns:
            df[f"diff_{col}"] = df[f1_col] - df[f2_col]
    
    return df


def build_matchup_dataset() -> None:
    """
    Build a fight-level dataset with fighter features and save to database
    Each row has:
    - Fight metadata
    - Fighter1 and Fighter2 features (prefixed with f1_ and f2_)
    - Difference features (fighter1 - fighter2, prefixed with diff_)
    - Label: f1_win (1 if fighter1 won, 0 otherwise)
    """
    engine = get_engine()
    
    logger.info("Loading fighter features and completed fights")
    features = load_fighter_features(engine)
    fights = load_completed_fights(engine)
    logger.info(f"Loaded {len(features)} fighters and {len(fights)} completed fights")
    
    if features.empty or fights.empty:
        logger.warning("No features or fights available; aborting")
        return
    
    # get feature column names for later use
    feature_cols = [c for c in features.columns if c not in ("fighter_id", "name")]
    
    # join fighter features for both fighters
    logger.info("Joining fighter features")
    df = add_fighter_features(fights, features, fighter_num=1)
    df = add_fighter_features(df, features, fighter_num=2)
    logger.info(f"After joining features: {len(df)} fights with both fighters")
    
    # add fighter names for readability
    df = add_fighter_names(df, features)
    
    # create label: did fighter1 win?
    df["f1_win"] = (df["winner_id"] == df["fighter1_id"]).astype(int)
    
    # add difference features
    logger.info("Computing difference features")
    df = add_difference_features(df, feature_cols)
    
    # save to database
    with engine.begin() as conn:
        df.to_sql("fight_matchups", conn, if_exists="replace", index=False)
        logger.info(f"Wrote {len(df)} rows to fight_matchups table")


if __name__ == "__main__":
    build_matchup_dataset()