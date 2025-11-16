# central config - DB URL, base URLs, logging setup
import logging
import os
from dotenv import load_dotenv

load_dotenv()
DB_URL = os.getenv("DB_URL")

UFCSTATS_BASE = "http://www.ufcstats.com"
BETMMA_NEXT_EVENT_URL = "https://www.betmma.tips/next_ufc_event.php"

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)
