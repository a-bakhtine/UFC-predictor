import requests
import urllib.parse as urlparse
from bs4 import BeautifulSoup
import pandas as pd
import re
from datetime import datetime
from config import logger, UFCSTATS_BASE

"""
Goal of file is to scrape and then separate data into the three tables
Turns HTML to DataFrames
"""
 
COMPLETED_EVENTS_URL = f"{UFCSTATS_BASE}/statistics/events/completed?page=all"

"""
Fetch a URL and return BeautifulSoup object (helper for HTTP + parsing)
"""
def get_soup(url: str) -> BeautifulSoup:
    logger.info(f"Fetching {url}")
    resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
    resp.raise_for_status() # throw error
    return BeautifulSoup(resp.text, "html.parser")

"""
Scrape the 'Compelted Events' page and return a list of event-details URLs
Each URL looks like: 
    http://ufcstats.com/event-details/xxxxxxxxxxxxxxxx
`limit` allows you to only take the first N for testing.
"""
def get_completed_event_urls(limit: int | None = None) -> list[str]:
    soup = get_soup(COMPLETED_EVENTS_URL)
    event_urls=[]

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "event-details" in href:
            full_url = urlparse.urljoin(UFCSTATS_BASE, href)
            if full_url not in event_urls:
                event_urls.append(full_url)
    
    if limit is not None:
        event_urls = event_urls[:limit]

    logger.info(f"Found {len(event_urls)} completed event URLs")
    return event_urls

"""
Scrape one UFCStats event-details page
Return 3 DFs:
    - df_fighters:  columns [fighter_id, name]
    - df_fights:    matches 'fights' table schema (minus odds)
    - df_stats:     matches 'fighter_stats' schema (stubbed empty for now)
"""
def parse_event(event_url: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    soup = get_soup(event_url)

    # extract event name and date
    title_span = soup.find("span", class_="b-content__title-highlight")
    event_name = title_span.get_text(strip=True) if title_span else "Unknown Event"

    event_date = None
    for li in soup.find_all("li", class_="b-list__box-list-item"):
        text = li.get_text(" ", strip=True)
        if "Date:" in text:
            date_str = text.split("Date:")[-1].strip()
            try:
                event_date = datetime.strptime(date_str, "%B %d, %Y").date()
            except ValueError:
                logger.warning(f"Could not parse event date from '{date_str}'")
            break

    if event_date is None:
        # fallback so don't crash
        event_date = datetime.today().date()

    logger.info(f"Parsing event '{event_name}' on {event_date}")

    # prepare containers for fighters, fights, stats
    fighters_dict: dict[str, dict] = {}
    fights_rows: list[dict] = []
    stats_rows: list[dict] = []

    # fights table
    fight_table = soup.find("table", class_=re.compile("b-fight-details__table"))
    if not fight_table:
        logger.error("Could not find fights table on event page")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    tbody = fight_table.find("tbody")
    if not tbody:
        logger.error("Could not find tbody in fights table")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    # loop thru each fight row
    for row in tbody.find_all("tr"):
        cols = row.find_all("td")
        if not cols:
            continue  # skip header/empty rows

        # 1) get fight URL and fight
        # look for a link whose href contains 'fight-details'
        fight_link = row.find("a", href=re.compile("fight-details"))
        if not fight_link:
            logger.warning("Skipping row without fight-details link")
            continue

        fight_url = urlparse.urljoin(UFCSTATS_BASE, fight_link["href"].strip())
        fight_id = fight_url.split("fight-details/")[-1].strip("/")

        # 2) get fighter names + fighter_ids
        # on event page, each row has two fighter links (top/bottom).
        fighter_links = row.find_all("a", href=re.compile("fighter-details"))
        if len(fighter_links) != 2:
            logger.warning(f"Expected 2 fighter links, found {len(fighter_links)} in row; skipping")
            continue

        f1_tag, f2_tag = fighter_links
        f1_name = f1_tag.get_text(strip=True)
        f2_name = f2_tag.get_text(strip=True)
        f1_url = urlparse.urljoin(UFCSTATS_BASE, f1_tag["href"].strip())
        f2_url = urlparse.urljoin(UFCSTATS_BASE, f2_tag["href"].strip())
        f1_id = f1_url.split("fighter-details/")[-1].strip("/")
        f2_id = f2_url.split("fighter-details/")[-1].strip("/")

        # add/update fighters in dict
        fighters_dict[f1_id] = {"fighter_id": f1_id, "name": f1_name}
        fighters_dict[f2_id] = {"fighter_id": f2_id, "name": f2_name}

        # 3) method, round, time & weight_class will be parsed later:
        # last 3 columns are: Method, Round, Time
        method_text= cols[-3].get_text(" ", strip=True) if len(cols) >= 3 else None
        method = method_text.split()[0] if method_text else None
        round_text = cols[-2].get_text(strip=True) if len(cols) >= 2 else None
        time_ended = cols[-1].get_text(strip=True) if len(cols) >= 1 else None
        weight_class = None

        try:
            round_ended = int(round_text) if round_text and round_text.isdigit() else None
        except ValueError:
            round_ended = None

        # 4) determine winner_id 
        winner_id = None

        # cols[0] is the W/L column
        wl_cell = cols[0]
        wl_text = wl_cell.get_text(" ", strip=True).lower()

        # for completed fights, the top fighter is the winner (unless draw/NC/etc.)
        if "win" in wl_text:
            winner_id = f1_id
        else:
            # upcoming fight / draw / nc / no contest
            winner_id = None


        fights_rows.append(
            {
                "fight_id": fight_id,
                "event_name": event_name,
                "event_date": event_date,
                "weight_class": weight_class,
                "fighter1_id": f1_id,
                "fighter2_id": f2_id,
                "winner_id": winner_id,
                "method": method,
                "round_ended": round_ended,
                "time_ended": time_ended,
                # odds will come from BetMMA later, so set to None for now
                "fighter1_closing_odds": None,
                "fighter2_closing_odds": None,
            }
        )

        # TODO (later): call a parse_fight_stats(fight_url, f1_id, f2_id, winner_id, round_ended, time_ended)
        # and extend stats_rows with per-fighter stats for this fight.

    # build dataframes
    df_fighters = pd.DataFrame(list(fighters_dict.values()))
    df_fights = pd.DataFrame(fights_rows)
    df_stats = pd.DataFrame(stats_rows) # empty until fight-details parsing done

    return df_fighters, df_fights, df_stats