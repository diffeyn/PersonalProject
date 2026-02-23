from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

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
    if pd.isna(s):
        return ""
    s = str(s).replace("\u200b", "").replace("\xa0", " ").strip()
    s = _strip_accents(s)
    s = s.lower()
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def norm_initial_last(s) -> str:
    s = norm_name(s)
    return s

def norm_club(club) -> str:
    if pd.isna(club):
        return ""
    return str(club).strip().upper()


# ============================================================
# DB FETCH HELPERS
# ============================================================

def fetch_players_general(engine) -> pd.DataFrame:
    """
    Must contain:
        player_id
        name (or player_name)
        team_id OR club/team/team_abbr (optional but helpful)
    """

    q = text("""
        SELECT
            CAST(player_id AS CHAR) AS player_id,
            COALESCE(name, player_name) AS name,
            team_id,
            COALESCE(club, team, team_abbr) AS club
        FROM players_general
        WHERE player_id IS NOT NULL
    """)
    return pd.read_sql(q, engine)


def fetch_team_roster_ids(engine) -> pd.DataFrame:
    """
    team_roster has NO names.
    It only helps constrain by team_id.
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


# ============================================================
# CONFIG
# ============================================================

@dataclass
class AttachConfig:
    name_col: str = "player_name"
    club_col: str = "club"
    team_id_col: Optional[str] = "team_id"  # optional
    out_col: str = "player_id"
    threshold: int = 92
    log_path: str = "data/interim/unmatched_match_players.csv"


# ============================================================
# MAIN ATTACH FUNCTION
# ============================================================

def attach_player_ids(match_df: pd.DataFrame, engine, cfg: Optional[AttachConfig] = None) -> pd.DataFrame:
    cfg = cfg or AttachConfig()
    df = match_df.copy()

    if cfg.name_col not in df.columns:
        raise ValueError(f"Missing column: {cfg.name_col}")
    if cfg.club_col not in df.columns:
        raise ValueError(f"Missing column: {cfg.club_col}")

    # Normalize
    df["_name_norm"] = df[cfg.name_col].map(norm_initial_last)
    df["_club_norm"] = df[cfg.club_col].map(norm_club)

    # Load lookup tables
    players = fetch_players_general(engine)
    players["_name_norm"] = players["name"].map(norm_initial_last)
    players["_club_norm"] = players["club"].map(norm_club)

    roster_ids = None
    if cfg.team_id_col and cfg.team_id_col in df.columns:
        roster = fetch_team_roster_ids(engine)
        roster_ids = roster.groupby("team_id")["player_id"].apply(set).to_dict()

    def match_row(row):
        name = row["_name_norm"]
        club = row["_club_norm"]

        if not name:
            return pd.NA, None, None

        candidates = players

        # Filter by roster team_id if available
        if roster_ids and pd.notna(row.get(cfg.team_id_col)):
            team_id = row[cfg.team_id_col]
            valid_ids = roster_ids.get(team_id)
            if valid_ids:
                candidates = candidates[candidates["player_id"].isin(valid_ids)]

        # Filter by club if possible
        club_filtered = candidates[candidates["_club_norm"] == club]
        if len(club_filtered) > 0:
            candidates = club_filtered

        names = candidates["_name_norm"].tolist()
        ids = candidates["player_id"].tolist()

        # Exact match
        try:
            idx = names.index(name)
            return ids[idx], "exact", 100
        except ValueError:
            pass

        # Fuzzy match
        if names:
            best = process.extractOne(name, names, scorer=fuzz.token_sort_ratio)
            if best and best[1] >= cfg.threshold:
                return ids[best[2]], "fuzzy", int(best[1])

        return pd.NA, None, None

    results = df.apply(match_row, axis=1, result_type="expand")
    df[cfg.out_col] = results[0]
    df["_match_type"] = results[1]
    df["_match_score"] = results[2]

    # --------------------------------------------------------
    # Enforce NOT NULL player_id
    # --------------------------------------------------------

    missing = df[cfg.out_col].isna() | (df[cfg.out_col].astype(str).str.strip() == "")

    if missing.any():
        bad = df.loc[missing, ["match_id", cfg.club_col, cfg.name_col, "_match_type", "_match_score"]]

        Path(cfg.log_path).parent.mkdir(parents=True, exist_ok=True)
        write_header = not Path(cfg.log_path).exists()
        bad.to_csv(cfg.log_path, mode="a", header=write_header, index=False)

        # Drop so DB insert does not fail
        df = df.loc[~missing].copy()

    return df.drop(columns=["_name_norm", "_club_norm", "_match_type", "_match_score"], errors="ignore")