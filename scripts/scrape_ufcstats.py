import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
from datetime import datetime
import urllib.parse as urlparse
from config import logger, UFCSTATS_BASE

"""
This file scrapes from UFCstats and then separates its data into the three dataframes:
fighters, fights, stats
"""
 
COMPLETED_EVENTS_URL = f"{UFCSTATS_BASE}/statistics/events/completed?page=all"

def get_soup(url: str) -> BeautifulSoup:
    """
    Fetch a URL
    Return BeautifulSoup object (helper for HTTP + parsing)
    """
    logger.info(f"Fetching {url}")
    resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
    resp.raise_for_status() # throw error
    return BeautifulSoup(resp.text, "html.parser")

def get_completed_event_urls(limit: int | None = None) -> list[str]:
    """
    Scrape the 'Completed Events' page and return a list of event-details URLs
    Each URL looks like: 
        http://ufcstats.com/event-details/xxxxxxxxxxxxxxxx
    `limit` allows you to only take the first N for testing.
    """
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


def parse_time_to_seconds(time_str: str | None) -> int | None:
    """
    Convert time string to seconds (3:45 -> 225)
    Returns None if missing / malformed
    """
    if not time_str:
        return None
    
    time_str = time_str.strip()
    if time_str in ("--", "0:00"):
        return 0
    
    try:
        minutes, seconds = time_str.split(":")
        return int(minutes) * 60 + int(seconds)
    except Exception:
        return None
 

def parse_x_of_y(text:str | None) -> tuple[int, int] | tuple[None, None]:
    """
    Parse strings like '23 of 57' into (23, 57) (happens w/ sig/total strikes, TDs)
    Returns (None, None) if can't parse
    """ 
    if not text: 
        return (None, None)
    
    m = re.search(r"(\d+)\s*of\s*(\d+)", text)
    if not m: 
        return (None, None)
    
    return int(m.group(1)), int(m.group(2))

def parse_fight_stats(
    fight_url: str,
    fight_id: str,
    f1_id: str,
    f2_id: str,
    winner_id: str | None,
    round_ended: int | None,
    time_ended: str | None,
) -> list[dict]:
    """
    Scrapes ONE fight-details page from UFCStats and returns a list of
    two dicts, one row / fighter, matching the fighter_stats schema
    Returns [] if stats are missing
    """
    soup = get_soup(fight_url)

    # find the 'Totals' table by header labels
    totals_table = None
    for tbl in soup.find_all("table"):
        thead = tbl.find("thead", class_="b-fight-details__table-head")
        tbody = tbl.find("tbody", class_="b-fight-details__table-body")
        if not thead or not tbody:
            continue

        header_cells = thead.find_all(["th", "td"])
        labels = [c.get_text(" ", strip=True).lower() for c in header_cells]

        # needed stats 
        required = ["fighter", "kd", "sig. str.", "total str.", "td", "sub. att", "ctrl"]
        if all(any(req in lab for lab in labels) for req in required):
            totals_table = tbl
            break

    if totals_table is None:
        logger.warning(f"No totals table found for fight {fight_id}")
        return []

    row = tbody.find("tr")
    if not row:
        logger.warning(f"No totals row found for fight {fight_id}")
        return []

    # all stat data in this row
    cells = row.find_all("td", class_="b-fight-details__table-col")

    # map column indices from header labels 
    kd_idx = sig_idx = tot_idx = td_idx = sub_idx = ctrl_idx = None
    for i, lab in enumerate(labels):
        if lab.startswith("kd"):
            kd_idx = i
        elif lab.startswith("sig. str.") and "%" not in lab:
            sig_idx = i
        elif lab.startswith("total str."):
            tot_idx = i
        elif lab.startswith("td") and "%" not in lab:
            td_idx = i
        elif lab.startswith("sub. att"):
            sub_idx = i
        elif lab.startswith("ctrl"):
            ctrl_idx = i

    if None in (kd_idx, sig_idx, tot_idx, td_idx, sub_idx, ctrl_idx):
        logger.warning(f"Could not map all columns in totals table for fight {fight_id}")
        return []

    # get text for fighter_index (0/1) from a given cell index
    def get_cell_text(col_idx: int, fighter_index: int) -> str | None:
        if col_idx < 0 or col_idx >= len(cells):
            return None
        cell = cells[col_idx]
        ps = cell.find_all("p", class_="b-fight-details__table-text")
        if fighter_index < 0 or fighter_index >= len(ps):
            return None
        return ps[fighter_index].get_text(" ", strip=True)

    fighter_cell = cells[0]
    links = fighter_cell.find_all("a", href=re.compile("fighter-details"))

    id_order: list[str] = []
    for a in links:
        href = a.get("href", "").strip()
        if href:
            fid = href.split("fighter-details/")[-1].strip("/")
            id_order.append(fid)

    index_for_id = {fid: idx for idx, fid in enumerate(id_order)}
    stats_rows: list[dict] = []

    # compute fight duration once
    duration_seconds = None
    if round_ended is not None and time_ended:
        base = (round_ended - 1) * 5 * 60  # 5 min rounds
        t = parse_time_to_seconds(time_ended)
        if t is not None:
            duration_seconds = base + t

    # build rows for fighter 1 and fighter 2 
    for fighter_id in [f1_id, f2_id]:
        idx = index_for_id.get(fighter_id)
        if idx is None:
            logger.warning(f"fighter_id {fighter_id} not found in totals table for fight {fight_id}")
            continue

        kd_text = get_cell_text(kd_idx, idx)
        sig_text = get_cell_text(sig_idx,idx)
        tot_text = get_cell_text(tot_idx,idx)
        td_text  = get_cell_text(td_idx, idx)
        sub_text = get_cell_text(sub_idx,idx)
        ctrl_text = get_cell_text(ctrl_idx, idx)

        kd = int(kd_text) if kd_text and kd_text.isdigit() else 0
        sig_landed, sig_att = parse_x_of_y(sig_text)
        tot_landed, tot_att = parse_x_of_y(tot_text)
        td_landed, td_att = parse_x_of_y(td_text)
        sub_attempts = int(sub_text) if sub_text and sub_text.isdigit() else 0
        ctrl_seconds = parse_time_to_seconds(ctrl_text) if ctrl_text else None

        is_winner = (fighter_id == winner_id) if winner_id is not None else None

        stats_rows.append(
            {
                "fight_id": fight_id,
                "fighter_id": fighter_id,
                "is_winner": is_winner,
                "knockdowns": kd,
                "sig_strikes_landed": sig_landed,
                "sig_strikes_attempted": sig_att,
                "total_strikes_landed": tot_landed,
                "total_strikes_attempted": tot_att,
                "td_landed": td_landed,
                "td_attempts": td_att,
                "sub_attempts": sub_attempts,
                "control_time_seconds": ctrl_seconds,
                "time_fought_seconds": duration_seconds,
            }
        )

    return stats_rows


def parse_event(event_url: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Scrape one UFCStats event-details page
    Return 3 DFs:
        - df_fighters: columns [fighter_id, name]
        - df_fights: matches 'fights' table schema (minus odds)
        - df_stats: matches 'fighter_stats' schema (stubbed empty for now)
    """
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

    # find fights table
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

        # get fight URL and fight
        fight_link = row.find("a", href=re.compile("fight-details"))
        if not fight_link:
            logger.warning("Skipping row without fight-details link")
            continue

        fight_url = urlparse.urljoin(UFCSTATS_BASE, fight_link["href"].strip())
        fight_id = fight_url.split("fight-details/")[-1].strip("/")

        # get fighter names + fighter_ids
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

        # last 3 columns are: Method, Round, Time
        weight_class = cols[-4].get_text(strip=True) if len(cols) >= 4 else None
        method_text= cols[-3].get_text(" ", strip=True) if len(cols) >= 3 else None
        method = method_text.split()[0] if method_text else None
        round_text = cols[-2].get_text(strip=True) if len(cols) >= 2 else None
        time_ended = cols[-1].get_text(strip=True) if len(cols) >= 1 else None

        round_ended = int(round_text) if round_text and round_text.isdigit() else None

        # determine winner from W/L col 
        wl_cell = cols[0]
        wl_text = wl_cell.get_text(" ", strip=True).lower()
        winner_id = f1_id if "win" in wl_text else None

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
                "fighter1_closing_odds": None,
                "fighter2_closing_odds": None,
            }
        )

        try:
            fight_stats_rows = parse_fight_stats(
                fight_url=fight_url,
                fight_id=fight_id,
                f1_id=f1_id,
                f2_id=f2_id,
                winner_id=winner_id,
                round_ended=round_ended,
                time_ended=time_ended,
            )
            stats_rows.extend(fight_stats_rows)
        except Exception as e:
            logger.warning(f"Failed to parse stats for fight {fight_id}: {e}")

    # build dataframes
    df_fighters = pd.DataFrame(list(fighters_dict.values()))
    df_fights = pd.DataFrame(fights_rows)
    df_stats = pd.DataFrame(stats_rows) 

    return df_fighters, df_fights, df_stats