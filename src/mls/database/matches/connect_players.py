from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
from rapidfuzz import fuzz, process
from sqlalchemy import text


# -------------------------
# name normalization
# -------------------------
def _strip_accents(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    return "".join(c for c in s if not unicodedata.combining(c))

def norm_name(s) -> str:
    """
    Aggressive normalization: accents off, lowercase, remove punctuation, collapse spaces.
    """
    if pd.isna(s):
        return ""
    s = str(s).replace("\u200b", "").replace("\xa0", " ").strip()
    s = _strip_accents(s)
    s = s.lower()
    s = re.sub(r"[^\w\s]", " ", s)     # kill punctuation
    s = re.sub(r"\s+", " ", s).strip()
    return s

def norm_initial_last(s) -> str:
    """
    Normalizes common MLS display names like 'A. Markanich' to 'a markanich'
    and also handles 'Á. Markanich', 'A.Markanich', etc.
    """
    s = norm_name(s)
    # turn "a." into "a"
    s = re.sub(r"\b([a-z])\b", r"\1", s)
    return s

def last_name_key(s) -> str:
    """
    Cheap helper for tie-breaking: last token.
    """
    s = norm_name(s)
    return s.split()[-1] if s else ""


# -------------------------
# team normalization
# -------------------------
TEAM_ALIASES = {
    "ny": "rbny", "new york": "rbny", "new york red bulls": "rbny",
    "new york city": "nyc", "new york city fc": "nyc",
    "los angeles": "la", "la galaxy": "la",
    "los angeles fc": "lafc", "la fc": "lafc",
    "dc": "dc", "d c": "dc", "d c united": "dc",
    "st louis": "stl", "st louis city": "stl",
    "montreal": "mtl", "cf montreal": "mtl",
}

def norm_club(club) -> str:
    if pd.isna(club):
        return ""
    s = str(club).strip()
    s = s.upper()
    # if already an abbr like MIN, SKC, etc.
    if re.fullmatch(r"[A-Z]{2,4}", s):
        return s.lower()
    # otherwise normalize
    s2 = norm_name(s)
    return TEAM_ALIASES.get(s2, s2)


# -------------------------
# DB fetch helpers
# -------------------------
def fetch_players_general(engine) -> pd.DataFrame:
    """
    players_general must have: player_id, name (or player_name), and ideally club/team_abbr.
    """
    q = text("""
        SELECT
            CAST(player_id AS CHAR) AS player_id,
            COALESCE(name, player_name) AS name,
            COALESCE(club, team, team_abbr) AS club
        FROM players_general
        WHERE player_id IS NOT NULL
    """)
    return pd.read_sql(q, engine)

def _table_columns(engine, table: str) -> set[str]:
    q = text("""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = :t
    """)
    cols = pd.read_sql(q, engine, params={"t": table})["COLUMN_NAME"].tolist()
    return set(c.lower() for c in cols)

def _pick(cols: set[str], candidates: list[str]) -> str | None:
    for c in candidates:
        if c.lower() in cols:
            return c
    return None

def fetch_team_roster(engine) -> pd.DataFrame:
    cols = _table_columns(engine, "team_roster")

    pid = _pick(cols, ["player_id"])
    name = _pick(cols, ["player_name", "name", "player", "full_name"])
    club = _pick(cols, ["team_abbr", "club", "team", "team_name"])

    if not pid:
        raise ValueError("team_roster is missing player_id column")

    # if you *truly* don't have a name column, you can still join by player_id elsewhere,
    # but for your matching logic you need a name.
    if not name:
        raise ValueError(f"team_roster has no usable name column. Found columns: {sorted(cols)[:50]} ...")

    if not club:
        # allow roster without club (worst case)
        club_expr = "NULL AS club"
    else:
        club_expr = f"{club} AS club"

    q = text(f"""
        SELECT
            CAST({pid} AS CHAR) AS player_id,
            {name} AS name,
            {club_expr}
        FROM team_roster
        WHERE {pid} IS NOT NULL
    """)
    return pd.read_sql(q, engine)


# -------------------------
# core matching
# -------------------------
@dataclass
class AttachConfig:
    name_col: str = "player_name"
    club_col: str = "club"          # in match stats df: MIN/SKC etc
    out_col: str = "player_id"
    threshold: int = 92             # fuzzy threshold
    log_path: str = "data/interim/unmatched_match_players.csv"
    keep_best_guess: bool = False   # if True, keep best_guess columns for debugging


def _build_team_index(ref: pd.DataFrame) -> dict[str, Tuple[list[str], list[str], list[str]]]:
    """
    Returns: team -> (norm_names, player_ids, last_keys)
    """
    ref = ref.copy()
    ref["club_norm"] = ref["club"].map(norm_club)
    ref["name_norm"] = ref["name"].map(norm_initial_last)
    ref["last_key"] = ref["name"].map(last_name_key)

    idx = {}
    for club, g in ref.groupby("club_norm"):
        idx[club] = (g["name_norm"].tolist(), g["player_id"].tolist(), g["last_key"].tolist())
    return idx


def attach_player_ids(match_df: pd.DataFrame, engine, cfg: Optional[AttachConfig] = None) -> pd.DataFrame:
    """
    Attaches player_id to match player stats using team_roster (preferred) then players_general (fallback).

    Never inserts NULL ids silently:
    - returns df with player_id (string) column
    - writes unmatched rows to cfg.log_path
    """
    cfg = cfg or AttachConfig()
    df = match_df.copy()

    # normalize input cols
    if cfg.name_col not in df.columns:
        raise ValueError(f"attach_player_ids: missing {cfg.name_col}")
    if cfg.club_col not in df.columns:
        raise ValueError(f"attach_player_ids: missing {cfg.club_col}")

    df["_club_norm"] = df[cfg.club_col].map(norm_club)
    df["_name_norm"] = df[cfg.name_col].map(norm_initial_last)

    # fetch reference tables
    roster = fetch_team_roster(engine)
    general = fetch_players_general(engine)

    # build indices
    roster_idx = _build_team_index(roster)
    general_idx = _build_team_index(general)

    def match_one(club_norm: str, name_norm: str) -> Tuple[Optional[str], Optional[str], Optional[int]]:
        """
        Returns (player_id, source, score)
        """
        if not club_norm or not name_norm:
            return None, None, None

        # 1) exact match in roster for that club
        if club_norm in roster_idx:
            names, ids, _ = roster_idx[club_norm]
            try:
                j = names.index(name_norm)
                return ids[j], "roster_exact", 100
            except ValueError:
                pass

            # 2) fuzzy match in roster
            best = process.extractOne(name_norm, names, scorer=fuzz.token_sort_ratio)
            if best and best[1] >= cfg.threshold:
                return ids[best[2]], "roster_fuzzy", int(best[1])

        # 3) exact match in general for that club
        if club_norm in general_idx:
            names, ids, _ = general_idx[club_norm]
            try:
                j = names.index(name_norm)
                return ids[j], "general_exact", 100
            except ValueError:
                pass

            # 4) fuzzy match in general
            best = process.extractOne(name_norm, names, scorer=fuzz.token_sort_ratio)
            if best and best[1] >= cfg.threshold:
                return ids[best[2]], "general_fuzzy", int(best[1])

        return None, None, None

    matches = df.apply(lambda r: match_one(r["_club_norm"], r["_name_norm"]), axis=1)
    df[cfg.out_col] = [m[0] for m in matches]
    df["_id_source"] = [m[1] for m in matches]
    df["_id_score"]  = [m[2] for m in matches]

    # log unmatched
    missing = df[cfg.out_col].isna() | (df[cfg.out_col].astype(str).str.strip() == "")
    if missing.any():
        bad = df.loc[missing, ["match_id", cfg.club_col, cfg.name_col, "_club_norm", "_name_norm", "_id_source", "_id_score"]].copy()

        Path(cfg.log_path).parent.mkdir(parents=True, exist_ok=True)
        # append for automation runs
        write_header = not Path(cfg.log_path).exists()
        bad.to_csv(cfg.log_path, mode="a", header=write_header, index=False)

        # hard truth: drop them so DB doesn’t explode
        df = df.loc[~missing].copy()

    # cleanup
    drop_cols = ["_club_norm", "_name_norm"]
    if not cfg.keep_best_guess:
        drop_cols += ["_id_source", "_id_score"]

    return df.drop(columns=drop_cols, errors="ignore")