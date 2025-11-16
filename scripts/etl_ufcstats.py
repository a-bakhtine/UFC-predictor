from sqlalchemy import text
from db import get_engine
from config import logger
from scrape_ufcstats import parse_event

EVENT_URL = "http://ufcstats.com/event-details/a9df5ae20a97b090"  # test URL

def load_single_event(event_url: str):
    engine = get_engine()

    df_fighters, df_fights, df_stats = parse_event(event_url)
    
    logger.info(f"Got {len(df_fighters)} fighters and {len(df_fights)} fights")

    with engine.begin() as conn:
        # for now get rid of tables so can re-run without worry of duplication
        logger.info("Truncating fighters, fights, fighter_stats")
        conn.execute(text("TRUNCATE TABLE fighter_stats, fights, fighters CASCADE;"))

        # insert fighters
        if not df_fighters.empty:
            df_fighters.to_sql("fighters", conn, if_exists="append", index=False)
            logger.info(f"Inserted {len(df_fighters)} fighers")

        # insert fights
        if not df_fights.empty:
            df_fights.to_sql("fights", conn, if_exists="append", index=False)
            logger.info(f"Inserted {len(df_fights)} fights")

        # insert stats **empty for now
        if not df_stats.empty:
            df_stats.to_sql("fighter_stats", conn, if_exists="append", index=False)
            logger.info(f"Inserted {len(df_stats)} fighter_stats rows")

if __name__ == "__main__":
    load_single_event(EVENT_URL)
            