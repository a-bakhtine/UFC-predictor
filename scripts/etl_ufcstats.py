from sqlalchemy import text
from db import get_engine
from config import logger
from scrape_ufcstats import get_completed_event_urls, parse_event

def _insert_event_data(engine, df_fighters, df_fights, df_stats):
    """
    Insert parsed DataFrames for a single event into the DB 
    """
    with engine.begin() as conn:
        if not df_fighters.empty:
            df_fighters = df_fighters.drop_duplicates(subset=["fighter_id"])
            df_fighters.to_sql("fighters", conn, if_exists="append", index=False)
            logger.info(f"Inserted {len(df_fighters)} fighters")

        if not df_fights.empty:
            df_fights.to_sql("fights", conn, if_exists="append", index=False)
            logger.info(f"Inserted {len(df_fights)} fights")

        if not df_stats.empty:
            df_stats.to_sql("fighter_stats", conn, if_exists="append", index=False)
            logger.info(f"Inserted {len(df_stats)} fighter_stats rows")


def load_single_event(event_url: str):
    """
    **Dev helper: load a UFC event and its data into the database
    """
    engine = get_engine()

    df_fighters, df_fights, df_stats = parse_event(event_url)
    
    logger.info(f"Got {len(df_fighters)} fighters and {len(df_fights)} fights")

    with engine.begin() as conn:
        # for now get rid of tables so can re-run without worry of duplication
        logger.info("Truncating fighters, fights, fighter_stats")
        conn.execute(text("TRUNCATE TABLE fighter_stats, fights, fighters CASCADE;"))
    
    _insert_event_data(engine, df_fighters, df_fights, df_stats)


def load_recent_events(num_events: int = 5):
    """
    Load recent UFC events and their data into the database
    """
    engine = get_engine()
    event_urls = get_completed_event_urls(limit=num_events)
    logger.info(f"Loading {len(event_urls)} completed events")
    
    # clear existing data
    with engine.begin() as conn:
        logger.info("Truncating fighters, fights, fighter_stats")
        conn.execute(text("TRUNCATE TABLE fighter_stats, fights, fighters CASCADE;"))
    
    # track fighters added in THIS run
    seen_fighter_ids: set[str] = set()
    
    # process each event
    for url in event_urls:
        logger.info(f"Processing event {url}")
        
        df_fighters, df_fights, df_stats = parse_event(url)
        logger.info(
            f"Scraped: {len(df_fighters)} fighters, "
            f"{len(df_fights)} fights, {len(df_stats)} stats"
        )
        # if no fights/stats, probably upcoming / broken event
        if df_fights.empty or df_stats.empty:
            logger.info(f"No completed fights/stats for event {url} (likely upcoming). Skipping insert.")
            continue
        
        if not df_fighters.empty:
            new_mask = ~df_fighters["fighter_id"].isin(seen_fighter_ids)
            new_fighters = df_fighters[new_mask].copy()
            # update the seen set with ONLY the new ones
            seen_fighter_ids.update(new_fighters["fighter_id"].tolist())
        else:
            new_fighters = df_fighters  # empty

        # insert data into database
        _insert_event_data(engine, new_fighters, df_fights, df_stats)
 

if __name__ == "__main__":
    # for dev
    # load_single_event("http://ufcstats.com/event-details/a9df5ae20a97b090")
    load_recent_events(num_events=200)
            