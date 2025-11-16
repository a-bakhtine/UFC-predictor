from sqlalchemy import create_engine
from config import DB_URL, logger

_engine = None

def get_engine():
    global _engine
    if _engine is None:
        _engine = create_engine(DB_URL)
        logger.info("Created SQLAlchemy engine")
    return _engine