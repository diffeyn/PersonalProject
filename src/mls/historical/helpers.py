"""
helpers.py — Utilities, constants, and shared tools for the MLS pipeline
"""
from __future__ import annotations
from datetime import datetime
import time
from pathlib import Path
from typing import Optional
from typing import Optional

import pandas as pd

# ─────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────

RAW_BASE = Path("data/raw/matches")

PATHS = {
    "outfield":   RAW_BASE / "players/outfield",
    "gk":         RAW_BASE / "players/gk",
    "team_stats": RAW_BASE / "match_team_stats",
    "feed":       RAW_BASE / "match_feed",
    "match_data": RAW_BASE / "match_data",
}

MASTER_FILES = {
    "outfield":   PATHS["outfield"]   / "master_outfield.csv",
    "gk":         PATHS["gk"]         / "master_gk.csv",
    "team_stats": PATHS["team_stats"] / "master_team_stats.csv",
    "feed":       PATHS["feed"]       / "master_feed.csv",
    "match_data": PATHS["match_data"] / "master_match_data.csv",
}


DEDUP_KEYS = {
    "outfield":   ["match_id", "player_name"],
    "gk":         ["match_id", "player_name"],
    "team_stats": ["match_id"],
    "feed":       ["match_id", "event_id"],
    "match_data": ["match_id"],
}

URL_LIST_PATH = Path("mls_match_links.csv")

TEAM_MAP = {
    "Atlanta United": "ATL",
    "Austin FC": "ATX",
    "CF Montréal": "MTL",
    "Charlotte FC": "CLT",
    "Chicago Fire FC": "CHI",
    "FC Cincinnati": "CIN",
    "Colorado Rapids": "COL",
    "Columbus Crew": "CLB",
    "D.C. United": "DC",
    "FC Dallas": "DAL",
    "Houston Dynamo FC": "HOU",
    "Sporting Kansas City": "SKC",
    "LA Galaxy": "LA",
    "Los Angeles Football Club": "LAFC",
    "Inter Miami CF": "MIA",
    "Minnesota United": "MIN",
    "Minnesota United FC": "MIN",
    "Nashville SC": "NSH",
    "New England Revolution": "NE",
    "New York City Football Club": "NYC",
    "New York City FC": "NYC",
    "New York Red Bulls": "RBNY",
    "Orlando City": "ORL",
    "Philadelphia Union": "PHI",
    "Portland Timbers": "POR",
    "Real Salt Lake": "RSL",
    "San Diego FC": "SD",
    "San Jose Earthquakes": "SJ",
    "Seattle Sounders FC": "SEA",
    "St. Louis CITY SC": "STL",
    "Toronto FC": "TOR",
    "Vancouver Whitecaps FC": "VAN",
}

TEAM_ALIASES = {
    "chicago fire": "CHI",
    "houston dynamo": "HOU",
    "dc united": "DC",
    "los angeles fc": "LAFC",
    "orlando city sc": "ORL",
    "st louis city sc": "STL",
    "inter miami": "MIA",
    "minnesota united fc": "MIN",
    "new york red bulls": "RBNY",
    "new york city": "NYC",
    "new england": "NE",
    "sporting kansas city": "SKC",
    "kansas city": "SKC",
    "salt lake": "RSL",
    "real salt lake": "RSL",
    "la galaxy": "LA",
}


# ─────────────────────────────────────────────
# Selenium helpers — TODO: fill in
# ─────────────────────────────────────────────
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
import re

def set_up_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--start-maximized")

    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    
    
    log_file = open("chromedriver.log", "w", encoding="utf-8")
    service = Service(log_output=log_file) 

    driver = webdriver.Chrome(service=service, options=options)

    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
    "source": "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"
    })
    
    return driver


def dismiss_cookies(driver):
    time.sleep(2)
    try:
        btn = driver.find_element(By.ID, "onetrust-accept-btn-handler")
        driver.execute_script("arguments[0].click();", btn)
        print("  [COOKIES] dismissed via JS click")
        time.sleep(0.5)
        return
    except:
        pass
    # fallback — nuke from DOM
    try:
        driver.execute_script("""
            ['onetrust-banner-sdk', 'onetrust-pc-sdk', 'onetrust-button-group'].forEach(function(id) {
                var el = document.getElementById(id);
                if (el) el.remove();
            });
        """)
        print("  [COOKIES] dismissed via DOM removal")
    except:
        pass


# Function to scroll by a certain amount of pixels
def js_scroll_by(driver, by):
    driver.execute_script("window.scrollBy(0, arguments[0]);", by)

# Function to scroll an element into view
def js_scroll_into_view(driver, el):
    driver.execute_script(
        "arguments[0].scrollIntoView({block:'center', inline:'nearest'});", el)

def clean_mls_date(raw_date: str, season_year: int):
    raw_date = raw_date.split("\n")[0].strip()
    
    # Remove venue
    s = raw_date.split("•")[0].strip()

    # Remove weekday
    s = re.sub(
        r"^(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\s+",
        "",
        s
    )

    # Only append year if not already present
    if not re.search(r"\d{4}", s):
        s = f"{s} {season_year}"

    # Parse
    dt = pd.to_datetime(s, format="%B %d %Y", errors="coerce")

    if pd.isna(dt):
        raise ValueError(f"Failed to parse date: {raw_date}")

    return dt.date()

def scrape_cards(group, driver):
    return driver.execute_script("""
        const root = arguments[0];
        return [...root.querySelectorAll('.mls-o-stat-chart')].map(c => ({
        stat:   c.querySelector('.mls-o-stat-chart__header')?.textContent.trim() || '',
        first:  c.querySelector('.mls-o-stat-chart__first-value')?.textContent.trim() || '',
        second: c.querySelector('.mls-o-stat-chart__second-value')?.textContent.trim() || '',
        }));
    """, group)
    
def load_full_feed_by_height(driver, step_px=1000, delay=1.5,
                             max_rounds=80, stable_rounds_required=10):
    prev_h = -1
    stable = 0

    for i in range(max_rounds):
        h = driver.execute_script("return document.body.scrollHeight;")

        # If we're at (or near) the footer, nudge up to trigger lazy-loading.
        scroll_y = driver.execute_script("return window.pageYOffset;")
        view_h = driver.execute_script("return window.innerHeight;")
        if scroll_y + view_h >= h - 5:
            driver.execute_script("window.scrollBy(0, arguments[0]);", -500)
            time.sleep(delay)

        # Re-check height after any footer nudge.
        h = driver.execute_script("return document.body.scrollHeight;")
        
        if h == prev_h:
            stable += 1
        else:
            stable = 0

        if stable >= stable_rounds_required:
            return h

        prev_h = h
        driver.execute_script("window.scrollBy(0, arguments[0]);", step_px)
        time.sleep(delay)

    return driver.execute_script("return document.body.scrollHeight;")


def get_html_text(el) -> Optional[str]:
    if not el:
        return None
    t = el.get_text(" ", strip=True)
    return t if t else None


# ─────────────────────────────────────────────
# Hashing — TODO: fill in
# ─────────────────────────────────────────────
import re
import hashlib
from urllib.parse import urlparse

def make_match_id(link):
    # normalize
    parsed = urlparse(link)
    path = parsed.path

    # 1️⃣ Try match pattern
    pattern = r'/matches/([a-z0-9-]+)vs([a-z0-9-]+)-(\d{2}-\d{2}-\d{4})'
    m = re.search(pattern, path)

    if m:
        team1, team2, date = m.groups()
        base = f"{team1}vs{team2}-{date}"
        return hashlib.md5(base.encode('utf-8')).hexdigest()[:8]

    parts = [p for p in path.split('/') if p]

    if parts:
        fallback = parts[-1]
    else:
        fallback = link
    return hashlib.md5(fallback.encode('utf-8')).hexdigest()[:8]



# ─────────────────────────────────────────────
# File helpers
# ─────────────────────────────────────────────

def ensure_dirs():
    for p in PATHS.values():
        p.mkdir(parents=True, exist_ok=True)


def dated_filename(key: str) -> Path:
    today = datetime.today().strftime("%Y-%m-%d")
    names = {
        "outfield":   f"{today}_outfield.csv",
        "gk":         f"{today}_gk.csv",
        "team_stats": f"{today}_team_stats.csv",
        "feed":       f"{today}_feed.csv",
        "match_data": f"{today}_match_data.csv",
    }
    return PATHS[key] / names[key]


def write_csv(df: pd.DataFrame, path: Path):
    if df is None or df.empty:
        print(f"  [SKIP] Empty, not writing {path.name}")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    print(f"  [SAVED] {path} ({len(df)} rows)")


def append_to_master(df: pd.DataFrame, key: str):
    if df is None or df.empty:
        return

    master_path = MASTER_FILES[key]
    dedup_keys  = DEDUP_KEYS[key]

    if master_path.exists():
        master = pd.read_csv(master_path, low_memory=False)
        # deduplicate columns before concat
        master = master.loc[:, ~master.columns.duplicated()]
        df = df.loc[:, ~df.columns.duplicated()]
        combined = pd.concat([master, df], ignore_index=True)
    else:
        combined = df.loc[:, ~df.columns.duplicated()].copy()

    combined = combined.drop_duplicates(subset=dedup_keys, keep="last")
    combined.to_csv(master_path, index=False)
    print(f"  [MASTER] {master_path.name} → {len(combined)} total rows")


def add_links_to_master(new_links: list):
    if URL_LIST_PATH.exists():
        existing = pd.read_csv(URL_LIST_PATH)
    else:
        existing = pd.DataFrame(columns=["url"])
        
    combined = pd.concat(
        [existing, pd.DataFrame(new_links, columns=["url"])],
        ignore_index=True
    ).drop_duplicates(subset=["url"])

    URL_LIST_PATH.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(URL_LIST_PATH, index=False)
    print(f"[URL LIST] {len(combined)} total URLs saved to {URL_LIST_PATH}")


# ─────────────────────────────────────────────
# Team mapping helpers
# ─────────────────────────────────────────────

def _norm_team(s: str) -> str:
    if pd.isna(s):
        return ""
    return str(s).lower().strip().replace(".", "").replace("\u2019", "'")


def map_club(df: pd.DataFrame) -> pd.DataFrame:
    team_map_norm = {_norm_team(k): v for k, v in TEAM_MAP.items()}
    team_map_norm.update({_norm_team(k): v for k, v in TEAM_ALIASES.items()})
    abbrs = set(team_map_norm.values())

    if "club" not in df.columns:
        raise ValueError("No club column found.")

    df["_club_norm"] = df["club"].apply(_norm_team)
    df["club"] = df["_club_norm"].apply(
        lambda x: x.upper() if x.upper() in abbrs else team_map_norm.get(x)
    )
    return df.drop(columns=["_club_norm"])