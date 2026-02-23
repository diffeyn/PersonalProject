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
    match_id_col: str = "match_id"          # used only for logging
    club_col: str = "club"                  # team abbreviation in match df (MIN, SKC)
    name_col: str = "player_name"           # player name in match df (A. Markanich)
    out_col: str = "player_id"

    # Optional: if match df has a date column, we use stint windows
    date_col: Optional[str] = None          # e.g. "match_date" or "date"

    threshold: int = 88                     # initials vs full names
    log_path: str = "data/interim/unmatched_match_players.csv"


# -------------------------
# normalization
# -------------------------
def _strip_accents(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    return "".join(c for c in s if not unicodedata.combining(c))

def norm_name(s) -> str:
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
# DB fetches (ONLY: teams, team_roster, players_general)
# -------------------------
def fetch_teams(engine) -> pd.DataFrame:
    q = text("""
        SELECT team_id, team_abbr
        FROM teams
        WHERE team_id IS NOT NULL AND team_abbr IS NOT NULL
    """)
    df = pd.read_sql(q, engine)
    df["team_abbr"] = df["team_abbr"].astype(str).str.upper().str.strip()
    return df[["team_id", "team_abbr"]]

def fetch_team_roster(engine) -> pd.DataFrame:
    q = text("""
        SELECT
            CAST(player_id AS CHAR) AS player_id,
            team_id,
            stint_start,
            stint_end
        FROM team_roster
        WHERE player_id IS NOT NULL AND team_id IS NOT NULL
    """)
    df = pd.read_sql(q, engine)
    df["stint_start"] = pd.to_datetime(df["stint_start"], errors="coerce")
    df["stint_end"] = pd.to_datetime(df["stint_end"], errors="coerce").fillna(pd.Timestamp("2100-01-01"))
    return df

def fetch_players_general(engine) -> pd.DataFrame:
    q = text("""
        SELECT CAST(player_id AS CHAR) AS player_id, name
        FROM players_general
        WHERE player_id IS NOT NULL AND name IS NOT NULL
    """)
    df = pd.read_sql(q, engine)
    df["_name_norm"] = df["name"].map(norm_name)
    return df[["player_id", "name", "_name_norm"]]


# -------------------------
# main
# -------------------------
def attach_player_ids(match_df: pd.DataFrame, engine, cfg: Optional[AttachConfig] = None) -> pd.DataFrame:
    cfg = cfg or AttachConfig()
    df = match_df.copy()

    # Required columns
    for c in [cfg.club_col, cfg.name_col]:
        if c not in df.columns:
            raise ValueError(f"attach_player_ids: match_df missing required column '{c}'")

    # Normalize match inputs
    df["_club"] = df[cfg.club_col].map(norm_abbr)
    df["_pname"] = df[cfg.name_col].map(norm_name)

    # Optional date
    use_date = bool(cfg.date_col) and cfg.date_col in df.columns
    if use_date:
        df["_mdate"] = pd.to_datetime(df[cfg.date_col], errors="coerce")
    else:
        df["_mdate"] = pd.NaT

    # Fetch refs
    teams = fetch_teams(engine)
    roster = fetch_team_roster(engine)
    players = fetch_players_general(engine)

    # Map club -> team_id
    df = df.merge(teams, left_on="_club", right_on="team_abbr", how="left").drop(columns=["team_abbr"], errors="ignore")

    # Pre-group roster and players for speed
    roster_by_team: Dict[int, pd.DataFrame] = {int(tid): g for tid, g in roster.groupby("team_id")}
    players_by_id = players.set_index("player_id")

    def match_row(r):
        pname = r["_pname"]
        team_id = r.get("team_id", None)

        if not pname:
            return pd.NA, "blank_name", None
        if pd.isna(team_id):
            return pd.NA, "no_team_id_from_teams", None

        team_id = int(team_id)
        g = roster_by_team.get(team_id)
        if g is None or g.empty:
            return pd.NA, "no_roster_for_team", None

        # Candidate roster rows
        if use_date and pd.notna(r["_mdate"]):
            mdate = r["_mdate"]
            cand_roster = g[(g["stint_start"].notna()) & (g["stint_start"] <= mdate) & (mdate <= g["stint_end"])]
        else:
            cand_roster = g  # no date: anyone who ever appeared for the team

        if cand_roster.empty:
            return pd.NA, "no_roster_candidates", None

        cand_ids = cand_roster["player_id"].astype(str).unique().tolist()

        # Bring candidate names from players_general
        cand_players = players[players["player_id"].isin(cand_ids)]
        if cand_players.empty:
            return pd.NA, "no_names_for_candidate_ids", None

        names = cand_players["_name_norm"].tolist()
        ids = cand_players["player_id"].tolist()

        # exact
        try:
            j = names.index(pname)
            return ids[j], "exact", 100
        except ValueError:
            pass

        # fuzzy
        best = process.extractOne(pname, names, scorer=fuzz.token_sort_ratio)
        if best and best[1] >= cfg.threshold:
            return ids[best[2]], "fuzzy", int(best[1])

        return pd.NA, "no_match", int(best[1]) if best else None

    res = df.apply(match_row, axis=1, result_type="expand")
    df[cfg.out_col] = res[0]
    df["_id_source"] = res[1]
    df["_id_score"] = res[2]

    # Log + drop unmatched to avoid DB NOT NULL failures
    missing = df[cfg.out_col].isna() | (df[cfg.out_col].astype(str).str.strip() == "")
    if missing.any():
        log_cols = [cfg.match_id_col, cfg.club_col, "_club", "team_id", cfg.name_col, cfg.date_col or "", "_id_source", "_id_score"]
        log_cols = [c for c in log_cols if c and c in df.columns]
        bad = df.loc[missing, log_cols].copy()

        Path(cfg.log_path).parent.mkdir(parents=True, exist_ok=True)
        header = not Path(cfg.log_path).exists()
        bad.to_csv(cfg.log_path, mode="a", header=header, index=False)

        df = df.loc[~missing].copy()

    return df.drop(columns=["_club", "_pname", "_mdate", "_id_source", "_id_score"], errors="ignore")