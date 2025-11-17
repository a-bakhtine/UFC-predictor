import pandas as pd
from db import get_engine
from config import logger

def load_stats_with_dates() -> pd.DataFrame:
    """
    Load fighter_stats joined with fights (to get event_date) from Postgres
    Returns one row per fighter per fight
    """
    engine = get_engine()
    query = """
        SELECT
            fs.fight_id,
            fs.fighter_id,
            fs.is_winner,
            fs.knockdowns,
            fs.sig_strikes_landed,
            fs.sig_strikes_attempted,
            fs.total_strikes_landed,
            fs.total_strikes_attempted,
            fs.td_landed,
            fs.td_attempts,
            fs.sub_attempts,
            fs.control_time_seconds,
            fs.time_fought_seconds,
            f.event_date
        FROM fighter_stats fs
        JOIN fights f ON fs.fight_id = f.fight_id
    """
    df = pd.read_sql(query, engine)
    df["event_date"] = pd.to_datetime(df["event_date"])

    # make sure columns are numeric
    num_cols = [
        "knockdowns", "sig_strikes_landed", "sig_strikes_attempted",
        "total_strikes_landed", "total_strikes_attempted", "td_landed",
        "td_attempts", "sub_attempts", "control_time_seconds", "time_fought_seconds",
    ]
    df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce")
    
    return df

def add_per_fight_rates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add per fight rate columns, filters out rows with invalid fight time
    """
    df = df.copy()
    
    # only keep fights with valid positive fight time
    df = df[(df["time_fought_seconds"].notna()) & (df["time_fought_seconds"] > 0)]
    
    # sig strikes per minute
    df["sig_strikes_per_min"] = df["sig_strikes_landed"] / (df["time_fought_seconds"] / 60.0)
    
    # TD accuracy
    df["td_accuracy"] = df["td_landed"] / df["td_attempts"]
    df.loc[df["td_attempts"] <= 0, "td_accuracy"] = pd.NA
    
    return df

def compute_aggregated_features(
    df: pd.DataFrame, 
    prefix: str = "", 
    last_n: int | None = None
) -> pd.DataFrame:
    """
    Compute aggregated features per fighter
    """
    if last_n:
        df = df.sort_values(["fighter_id", "event_date"])
        df = df.groupby("fighter_id", group_keys=False).tail(last_n)
    
    # aggregate stats
    grouped = df.groupby("fighter_id", as_index=False).agg(
        fights_count=("fight_id", "nunique"),
        wins_count=("is_winner", lambda x: (x == True).sum()),
        total_time_seconds=("time_fought_seconds", "sum"),
        total_sig_strikes=("sig_strikes_landed", "sum"),
        total_tds_landed=("td_landed", "sum"),
        total_tds_attempts=("td_attempts", "sum"),
    )
    
    # compute derived metrics
    grouped["sig_strikes_per_min"] = grouped["total_sig_strikes"] / (
        grouped["total_time_seconds"] / 60.0
    )
    grouped["td_accuracy"] = grouped["total_tds_landed"] / grouped["total_tds_attempts"]
    grouped.loc[grouped["total_tds_attempts"] <= 0, "td_accuracy"] = pd.NA
    grouped["win_rate"] = grouped["wins_count"] / grouped["fights_count"]
    
    # add prefix to columns (except fighter_id)
    if prefix:
        rename_map = {
            col: f"{prefix}{col}" 
            for col in grouped.columns 
            if col != "fighter_id"
        }
        grouped = grouped.rename(columns=rename_map)
    
    # select final columns
    final_cols = ["fighter_id"]
    if prefix:
        final_cols += [
            f"{prefix}fights_count",
            f"{prefix}wins_count",
            f"{prefix}win_rate",
            f"{prefix}sig_strikes_per_min",
            f"{prefix}td_accuracy",
        ]
    else:
        final_cols += [
            "fights_count",
            "wins_count",
            "win_rate",
            "sig_strikes_per_min",
            "td_accuracy",
        ]
    
    return grouped[final_cols]

def build_and_save_features():
    """
    Load data, compute career and last-3 features, and save to database.
    """
    engine = get_engine()
    
    logger.info("Loading fighter_stats + fights from DB")
    df = load_stats_with_dates()
    logger.info(f"Loaded {len(df)} fighter-fight rows")
    
    logger.info("Adding per-fight rate columns")
    df_rates = add_per_fight_rates(df)
    logger.info(f"Rows with valid time_fought_seconds: {len(df_rates)}")
    
    logger.info("Computing career features")
    df_career = compute_aggregated_features(df_rates, prefix="career_")
    logger.info(f"Computed career features for {len(df_career)} fighters")
    
    logger.info("Computing last-3-fights features")
    df_last3 = compute_aggregated_features(df_rates, prefix="last3_", last_n=3)
    logger.info(f"Computed last-3 features for {len(df_last3)} fighters")
    
    # merge career + last3
    df_features = pd.merge(df_career, df_last3, on="fighter_id", how="left")
    logger.info(f"Final features rows: {len(df_features)}")
    
    # add fighter names for convenience
    with engine.begin() as conn:
        names = pd.read_sql("SELECT fighter_id, name FROM fighters", conn)
    df_features = df_features.merge(names, on="fighter_id", how="left")
    
    # reorder columns with name first
    cols = ["fighter_id", "name"] + [
        c for c in df_features.columns if c not in ("fighter_id", "name")
    ]
    df_features = df_features[cols]
    
    # write to database
    with engine.begin() as conn:
        df_features.to_sql("fighter_features", conn, if_exists="replace", index=False)
        logger.info("Wrote features to table 'fighter_features'")


if __name__ == "__main__":
    build_and_save_features()

