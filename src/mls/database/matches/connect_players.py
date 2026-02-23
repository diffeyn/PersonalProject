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
    return norm_name(s)

def norm_club(club) -> str:
    if pd.isna(club):
        return ""
    s = str(club).strip()
    # keep abbreviations as-is
    return s.upper()


# ============================================================
# SCHEMA INTROSPECTION
# ============================================================

def _table_columns(engine, table: str) -> set[str]:
    q = text("""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = :t
    """)
    cols = pd.read_sql(q, engine, params={"t": table})["COLUMN_NAME"].tolist()
    return set(c.lower() for c in cols)

def _pick(cols: set[str], candidates: list[str]) -> Optional[str]:
    for c in candidates:
        if c.lower() in cols:
            return c
    return None


# ============================================================
# DB FETCH HELPERS (robust to schema)
# ============================================================

def fetch_players_general(engine) -> pd.DataFrame:
    q = text("""
        SELECT
            CAST(player_id AS CHAR) AS player_id,
            name AS name,
            team_id,
            COALESCE(club, team, team_abbr) AS club
        FROM players_general
        WHERE player_id IS NOT NULL
    """)
    return pd.read_sql(q, engine)


def fetch_team_roster_ids(engine) -> pd.DataFrame:
    """
    team_roster has no names. It's only for player_id + team_id constraints.
    """
    cols = _table_columns(engine, "team_roster")

    pid = _pick(cols, ["player_id"])
    team_id = _pick(cols, ["team_id"])
    stint_start = _pick(cols, ["stint_start"])
    stint_end = _pick(cols, ["stint_end"])

    if not pid or not team_id:
        raise ValueError(f"team_roster missing required columns. Found: {sorted(cols)[:60]}")

    stint_start_expr = f"{stint_start} AS stint_start" if stint_start else "NULL AS stint_start"
    stint_end_expr = f"{stint_end} AS stint_end" if stint_end else "NULL AS stint_end"

    q = text(f"""
        SELECT
            CAST({pid} AS CHAR) AS player_id,
            {team_id} AS team_id,
            {stint_start_expr},
            {stint_end_expr}
        FROM team_roster
        WHERE {pid} IS NOT NULL
          AND {team_id} IS NOT NULL
    """)
    return pd.read_sql(q, engine)


# ============================================================
# CONFIG
# ============================================================

@dataclass
class AttachConfig:
    name_col: str = "player_name"     # in match_player_stats df
    club_col: str = "club"            # MIN/SKC/etc
    team_id_col: Optional[str] = "team_id"  # optional in match df
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
        raise ValueError(f"Missing column in match df: {cfg.name_col}")
    if cfg.club_col not in df.columns:
        raise ValueError(f"Missing column in match df: {cfg.club_col}")

    df["_name_norm"] = df[cfg.name_col].map(norm_initial_last)
    df["_club_norm"] = df[cfg.club_col].map(norm_club)

    # Reference players
    players = fetch_players_general(engine)
    players["_name_norm"] = players["name"].map(norm_initial_last)
    players["_club_norm"] = players["club"].map(norm_club)

    # Optional constraint by team_roster (if match df has team_id)
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

        # constrain by roster if possible
        if roster_ids and pd.notna(row.get(cfg.team_id_col)):
            team_id = row[cfg.team_id_col]
            valid = roster_ids.get(team_id)
            if valid:
                candidates = candidates[candidates["player_id"].isin(valid)]

        # constrain by club if useful
        club_filtered = candidates[candidates["_club_norm"] == club]
        if len(club_filtered) > 0:
            candidates = club_filtered

        names = candidates["_name_norm"].tolist()
        ids = candidates["player_id"].tolist()

        # exact
        try:
            j = names.index(name)
            return ids[j], "exact", 100
        except ValueError:
            pass

        # fuzzy
        if names:
            best = process.extractOne(name, names, scorer=fuzz.token_sort_ratio)
            if best and best[1] >= cfg.threshold:
                return ids[best[2]], "fuzzy", int(best[1])

        return pd.NA, None, None

    res = df.apply(match_row, axis=1, result_type="expand")
    df[cfg.out_col] = res[0]
    df["_match_type"] = res[1]
    df["_match_score"] = res[2]

    # log + drop missing ids (automation friendly)
    missing = df[cfg.out_col].isna() | (df[cfg.out_col].astype(str).str.strip() == "")
    if missing.any():
        bad = df.loc[missing, ["match_id", cfg.club_col, cfg.name_col, "_match_type", "_match_score"]].copy()

        Path(cfg.log_path).parent.mkdir(parents=True, exist_ok=True)
        header = not Path(cfg.log_path).exists()
        bad.to_csv(cfg.log_path, mode="a", header=header, index=False)

        df = df.loc[~missing].copy()

    return df.drop(columns=["_name_norm", "_club_norm", "_match_type", "_match_score"], errors="ignore")