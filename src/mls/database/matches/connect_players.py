from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Dict

import pandas as pd
from rapidfuzz import process, fuzz
from sqlalchemy import text


# -------------------------
# config
# -------------------------
@dataclass
class AttachConfig:
    match_id_col: str = "match_id"
    club_col: str = "club"              # team abbreviation in match df (e.g. MIN, SKC)
    name_col: str = "player_name"       # player display name in match df (e.g. A. Markanich)
    out_col: str = "player_id"

    threshold: int = 88                 # initials vs full name -> keep this lenient
    log_path: str = "data/interim/unmatched_match_players.csv"


# -------------------------
# normalization
# -------------------------
def _strip_accents(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    return "".join(c for c in s if not unicodedata.combining(c))

def norm_name(s) -> str:
    """Accent-strip, lowercase, remove punctuation, collapse whitespace."""
    if pd.isna(s):
        return ""
    s = str(s).replace("\u200b", "").replace("\xa0", " ").strip()
    s = _strip_accents(s).lower()
    s = s.replace(".", " ")
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def norm_abbr(s) -> str:
    if pd.isna(s):
        return ""
    return str(s).strip().upper()


# -------------------------
# DB fetches (only what we need)
# -------------------------
def _fetch_players_general(engine) -> pd.DataFrame:
    # players_general: player_id, name
    q = text("""
        SELECT CAST(player_id AS CHAR) AS player_id, name
        FROM players_general
        WHERE player_id IS NOT NULL AND name IS NOT NULL
    """)
    return pd.read_sql(q, engine)

def _fetch_team_roster(engine) -> pd.DataFrame:
    # team_roster: player_id, team_id, stint_start, stint_end
    q = text("""
        SELECT
            CAST(player_id AS CHAR) AS player_id,
            team_id,
            stint_start,
            stint_end
        FROM team_roster
        WHERE player_id IS NOT NULL AND team_id IS NOT NULL
    """)
    return pd.read_sql(q, engine)

def _fetch_match_dates(engine) -> pd.DataFrame:
    # matches: match_id, match_date
    q = text("""
        SELECT match_id, match_date
        FROM matches
        WHERE match_id IS NOT NULL AND match_date IS NOT NULL
    """)
    df = pd.read_sql(q, engine)
    df["match_date"] = pd.to_datetime(df["match_date"], errors="coerce")
    return df[["match_id", "match_date"]]

def _fetch_team_abbr_map(engine) -> pd.DataFrame:
    # teams: team_id, team_abbr
    q = text("""
        SELECT team_id, team_abbr
        FROM teams
        WHERE team_id IS NOT NULL AND team_abbr IS NOT NULL
    """)
    df = pd.read_sql(q, engine)
    df["team_abbr"] = df["team_abbr"].astype(str).str.upper().str.strip()
    return df[["team_id", "team_abbr"]]


# -------------------------
# main
# -------------------------
def attach_player_ids(match_df: pd.DataFrame, engine, cfg: Optional[AttachConfig] = None) -> pd.DataFrame:
    """
    Attach player_id to match player stats using:
      club(abbr) -> teams.team_id
      match_id -> matches.match_date
      (team_id, match_date) -> team_roster player_ids
      player_id -> players_general.name
      fuzzy match within roster candidates

    Logs + drops unmatched rows to avoid NOT NULL insert failures.
    """
    cfg = cfg or AttachConfig()
    df = match_df.copy()

    # Validate required match_df columns
    for c in [cfg.match_id_col, cfg.club_col, cfg.name_col]:
        if c not in df.columns:
            raise ValueError(f"attach_player_ids: match_df missing required column '{c}'")

    # Normalize inputs
    df["_club"] = df[cfg.club_col].map(norm_abbr)
    df["_pname"] = df[cfg.name_col].map(norm_name)

    # Map club -> team_id using teams table
    teams = _fetch_team_abbr_map(engine)
    df = df.merge(
        teams,
        left_on="_club",
        right_on="team_abbr",
        how="left",
    ).drop(columns=["team_abbr"], errors="ignore")

    # Attach match_date
    md = _fetch_match_dates(engine)
    df = df.merge(
        md,
        left_on=cfg.match_id_col,
        right_on="match_id",
        how="left",
    ).drop(columns=["match_id"], errors="ignore")

    # Load reference tables
    pg = _fetch_players_general(engine)
    pg["_name"] = pg["name"].map(norm_name)

    tr = _fetch_team_roster(engine)
    tr["stint_start"] = pd.to_datetime(tr["stint_start"], errors="coerce")
    tr["stint_end"] = pd.to_datetime(tr["stint_end"], errors="coerce").fillna(pd.Timestamp("2100-01-01"))

    # Pre-group roster by team_id for speed
    roster_by_team: Dict[int, pd.DataFrame] = {int(tid): g for tid, g in tr.groupby("team_id")}

    def match_row(r):
        pname = r["_pname"]
        team_id = r.get("team_id", None)
        mdate = r.get("match_date", pd.NaT)

        if not pname:
            return pd.NA, "blank_name", None
        if pd.isna(team_id):
            return pd.NA, "no_team_id_from_teams", None
        if pd.isna(mdate):
            return pd.NA, "no_match_date", None

        team_id = int(team_id)
        g = roster_by_team.get(team_id)
        if g is None or g.empty:
            return pd.NA, "no_roster_for_team", None

        # roster candidates active on that match date
        active = g[(g["stint_start"].notna()) & (g["stint_start"] <= mdate) & (mdate <= g["stint_end"])]
        if active.empty:
            return pd.NA, "no_active_roster_on_date", None

        cand_ids = set(active["player_id"].astype(str))
        cand = pg[pg["player_id"].isin(cand_ids)]
        if cand.empty:
            return pd.NA, "no_names_for_candidate_ids", None

        names = cand["_name"].tolist()
        ids = cand["player_id"].tolist()

        # exact first
        try:
            j = names.index(pname)
            return ids[j], "exact", 100
        except ValueError:
            pass

        # fuzzy within roster candidates
        best = process.extractOne(pname, names, scorer=fuzz.token_sort_ratio)
        if best and best[1] >= cfg.threshold:
            return ids[best[2]], "fuzzy", int(best[1])

        return pd.NA, "no_match", int(best[1]) if best else None

    res = df.apply(match_row, axis=1, result_type="expand")
    df[cfg.out_col] = res[0]
    df["_id_source"] = res[1]
    df["_id_score"] = res[2]

    # Enforce NOT NULL: log + drop
    missing = df[cfg.out_col].isna() | (df[cfg.out_col].astype(str).str.strip() == "")
    if missing.any():
        log_cols = [cfg.match_id_col, cfg.club_col, "_club", "team_id", "match_date", cfg.name_col, "_id_source", "_id_score"]
        log_cols = [c for c in log_cols if c in df.columns]
        bad = df.loc[missing, log_cols].copy()

        Path(cfg.log_path).parent.mkdir(parents=True, exist_ok=True)
        header = not Path(cfg.log_path).exists()
        bad.to_csv(cfg.log_path, mode="a", header=header, index=False)

        df = df.loc[~missing].copy()

    # Cleanup helper cols
    return df.drop(columns=["_club", "_pname", "_id_source", "_id_score"], errors="ignore")