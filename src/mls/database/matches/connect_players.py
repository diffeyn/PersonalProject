import pandas as pd
from sqlalchemy import text
from rapidfuzz import process, fuzz

import re
import unicodedata

def norm(x):
    if pd.isna(x):
        return None
    
    # remove accents
    x = unicodedata.normalize("NFKD", str(x))
    x = "".join(c for c in x if not unicodedata.combining(c))
    
    # lowercase
    x = x.lower().strip()
    
    # remove punctuation
    x = re.sub(r"[^\w\s]", "", x)
    
    # collapse multiple spaces
    x = re.sub(r"\s+", " ", x)
    
    return x


def attach_player_ids(match_players, engine, cutoff):
    players = pd.read_sql(text("SELECT player_id, name FROM players_general"), engine)

    roster = pd.read_sql(text("""
        SELECT player_id, team_id, stint_end, obs_count
        FROM team_roster
    """), engine)

    teams = pd.read_sql(text("SELECT team_id, team_name FROM teams"), engine)

    roster["stint_end"] = pd.to_datetime(roster["stint_end"], errors="coerce")

    latest_team = (roster.sort_values(["player_id", "stint_end", "obs_count"],
                                      ascending=[True, False, False])
                         .drop_duplicates("player_id", keep="first")[["player_id", "team_id"]]
                         .merge(teams, on="team_id", how="left")
                         .dropna(subset=["team_name"]))

    lookup = (players.merge(latest_team[["player_id", "team_name"]], on="player_id", how="left")
                    .dropna(subset=["team_name"])
                    .assign(name_norm=lambda d: d["name"].map(norm),
                            club_norm=lambda d: d["team_name"].map(norm)))

    mp = (match_players.copy()
          .assign(name_norm=lambda d: d["player_name"].map(norm),
                  club_norm=lambda d: d["club"].map(norm)))

    # --- exact merge first ---
    out = mp.merge(lookup[["player_id", "name_norm", "club_norm"]],
                   on=["name_norm", "club_norm"], how="left")

    # --- fuzzy fallback for misses ---
    missing = out["player_id"].isna()
    if missing.any():

        # club -> list of (name_norm, player_id)
        club_map = {
            c: sub[["name_norm", "player_id"]].values.tolist()
            for c, sub in lookup.groupby("club_norm")
        }

        for i in out[missing].index:
            club = out.at[i, "club_norm"]
            name = out.at[i, "name_norm"]
            choices = club_map.get(club, [])
            if not choices:
                continue

            names = [x[0] for x in choices]
            best = process.extractOne(name, names, scorer=fuzz.token_sort_ratio)
            if best and best[1] >= cutoff:
                out.at[i, "player_id"] = choices[best[2]][1]
    
    out = out.drop(columns=["name_norm", "club_norm"])
    
    print(f'players not matched: {out["player_id"].isna().sum()} / {len(out)}')
    
    
    return out
