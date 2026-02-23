from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Dict, Set, Tuple

import pandas as pd
from rapidfuzz import process, fuzz
from sqlalchemy import text


# ============================================================
# NORMALIZATION
# ============================================================

def _strip_accents(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    return "".join(c for c in s if not unicodedata.combining(c))

def norm_name(s) -> str:
    """Aggressive normalize: accents off, lowercase, kill punctuation, collapse spaces."""
    if pd.isna(s):
        return ""
    s = str(s).replace("\u200b", "").replace("\xa0", " ").strip()
    s = _strip_accents(s).lower()
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def norm_match_player(s) -> str:
    """
    Match-feed usually: 'A. Markanich' -> 'a markanich'
    """
    s = norm_name(s)
    s = s.replace(".", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def norm_full_name(s) -> str:
    """
    players_general likely full names. Keep as normalized tokens.
    """
    return norm_name(s)


# ============================================================
# CONFIG
# ============================================================

@dataclass
class AttachConfig:
    # columns in match_player_stats df
    name_col: str = "player_name"
    club_col: str = "club"              # optional; used only for logging
    team_id_col: str = "team_id"        # REQUIRED for this approach
    match_id_col: str = "match_id"

    # output
    out_col: str = "player_id"

    # fuzzy
    threshold: int = 90                 # lower than before, because initials vs full name

    # logging
    log_path: str = "data/interim/unmatched_match_players.csv"


# ============================================================
# DB FETCH
# ============================================================

def fetch_players_general(engine) -> pd.DataFrame:
    """
    players_general must have: player_id, name
    """
    q = text("""
        SELECT
            CAST(player_id AS CHAR) AS player_id,
            name
        FROM players_general
        WHERE player_id IS NOT NULL
          AND name IS NOT NULL
    """)
    return pd.read_sql(q, engine)

def fetch_team_roster(engine) -> pd.DataFrame:
    """
    team_roster must have: player_id, team_id, stint_start, stint_end
    """
    q = text("""
        SELECT
            CAST(player_id AS CHAR) AS player_id,
            team_id,
            stint_start,
            stint_end
        FROM team_roster
        WHERE player_id IS NOT NULL
          AND team_id IS NOT NULL
    """)
    return pd.read_sql(q, engine)

def fetch_match_dates(engine) -> pd.DataFrame:
    """
    Need match_id -> match_date.
    Adjust table/column names if yours differ.
    """
    q = text("""
        SELECT
            match_id,
            match_date
        FROM matches
        WHERE match_id IS NOT NULL
    """)
    return pd.read_sql(q, engine)


# ============================================================
# ROSTER INDEX: (team_id, match_date) -> set(player_id)
# ============================================================

def _build_roster_index(team_roster: pd.DataFrame) -> Dict[int, list[Tuple[pd.Timestamp, pd.Timestamp, Set[str]]]]:
    """
    Build per-team stint windows:
      team_id -> list of (start, end, {player_ids active in that stint window})
    Because multiple stints overlap, we keep it simple: filter row-wise later.
    """
    tr = team_roster.copy()
    tr["stint_start"] = pd.to_datetime(tr["stint_start"], errors="coerce")
    tr["stint_end"] = pd.to_datetime(tr["stint_end"], errors="coerce")

    # If stint_end is NULL, treat as open-ended far future
    tr["stint_end"] = tr["stint_end"].fillna(pd.Timestamp("2100-01-01"))

    out: Dict[int, list[Tuple[pd.Timestamp, pd.Timestamp, Set[str]]]] = {}
    for team_id, g in tr.groupby("team_id"):
        out[int(team_id)] = [(row.stint_start, row.stint_end, {row.player_id}) for row in g.itertuples(index=False)]
    return out

def _roster_ids_for(team_index, team_id: int, match_date: pd.Timestamp) -> Set[str]:
    """
    Return player_ids where stint_start <= match_date <= stint_end.
    """
    stints = team_index.get(int(team_id), [])
    ids: Set[str] = set()
    for start, end, pid_set in stints:
        if pd.isna(start):
            continue
        if start <= match_date <= end:
            ids |= pid_set
    return ids


# ============================================================
# ATTACH
# ============================================================

def attach_player_ids(match_df: pd.DataFrame, engine, cfg: Optional[AttachConfig] = None) -> pd.DataFrame:
    cfg = cfg or AttachConfig()
    df = match_df.copy()

    # sanity
    for c in [cfg.match_id_col, cfg.name_col, cfg.team_id_col]:
        if c not in df.columns:
            raise ValueError(f"attach_player_ids: match_df missing required column '{c}'")

    # fetch refs
    pg = fetch_players_general(engine)
    tr = fetch_team_roster(engine)
    md = fetch_match_dates(engine)

    # normalize players_general
    pg = pg.copy()
    pg["_name_norm"] = pg["name"].map(norm_full_name)

    # match_id -> match_date
    md = md.copy()
    md["match_date"] = pd.to_datetime(md["match_date"], errors="coerce")
    match_date_map = dict(zip(md["match_id"], md["match_date"]))

    # roster index
    team_index = _build_roster_index(tr)

    # normalize match names
    df["_name_norm"] = df[cfg.name_col].map(norm_match_player)

    # attach match_date
    df["_match_date"] = df[cfg.match_id_col].map(match_date_map)

    # Main matcher
    def match_one(row) -> Tuple[Optional[str], Optional[str], Optional[int]]:
        name_norm = row["_name_norm"]
        team_id = row[cfg.team_id_col]
        mdate = row["_match_date"]

        if not name_norm or pd.isna(team_id) or pd.isna(mdate):
            return None, None, None

        # candidate IDs from roster at that time
        cand_ids = _roster_ids_for(team_index, int(team_id), pd.Timestamp(mdate))
        if not cand_ids:
            return None, "no_roster_candidates", None

        cand = pg[pg["player_id"].isin(cand_ids)]
        if cand.empty:
            return None, "no_names_for_roster_ids", None

        names = cand["_name_norm"].tolist()
        ids = cand["player_id"].tolist()

        # exact match first
        try:
            j = names.index(name_norm)
            return ids[j], "exact", 100
        except ValueError:
            pass

        # fuzzy within roster candidates
        best = process.extractOne(name_norm, names, scorer=fuzz.token_sort_ratio)
        if best and best[1] >= cfg.threshold:
            return ids[best[2]], "fuzzy", int(best[1])

        return None, "no_match", None

    res = df.apply(match_one, axis=1, result_type="expand")
    df[cfg.out_col] = res[0]
    df["_id_source"] = res[1]
    df["_id_score"] = res[2]

    # enforce NOT NULL player_id (log + drop)
    missing = df[cfg.out_col].isna() | (df[cfg.out_col].astype(str).str.strip() == "")
    if missing.any():
        cols = [cfg.match_id_col, cfg.team_id_col, cfg.club_col, cfg.name_col, "_id_source", "_id_score"]
        cols = [c for c in cols if c in df.columns]
        bad = df.loc[missing, cols].copy()

        Path(cfg.log_path).parent.mkdir(parents=True, exist_ok=True)
        header = not Path(cfg.log_path).exists()
        bad.to_csv(cfg.log_path, mode="a", header=header, index=False)

        df = df.loc[~missing].copy()

    return df.drop(columns=["_name_norm", "_match_date", "_id_source", "_id_score"], errors="ignore")