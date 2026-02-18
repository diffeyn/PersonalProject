import pandas as pd
from sqlalchemy import text
from rapidfuzz import process, fuzz
import re, unicodedata

def norm(x):
    if pd.isna(x):
        return None
    x = unicodedata.normalize("NFKD", str(x))
    x = "".join(c for c in x if not unicodedata.combining(c))
    x = x.lower().strip()
    x = re.sub(r"[^\w\s]", "", x)
    x = re.sub(r"\s+", " ", x)
    return x or None


def attach_player_ids(match_players, engine, cutoff=88, team_cutoff=80):
    mp = match_players.copy()

    # ---- load DB tables ----
    players = pd.read_sql(text("SELECT player_id, name FROM players_general"), engine)
    roster = pd.read_sql(text("""
        SELECT player_id, team_id, stint_end, obs_count
        FROM team_roster
    """), engine)
    teams = pd.read_sql(text("SELECT team_id, team_name FROM teams"), engine)

    # ---- normalize ----
    mp["name_norm"] = mp["player_name"].map(norm)
    mp["club_norm"] = mp["club"].map(norm)

    players["name_norm"] = players["name"].map(norm)

    teams["team_norm"] = teams["team_name"].map(norm)
    team_norms = teams["team_norm"].dropna().tolist()

    # ---- map scraped club -> team_id (fuzzy) ----
    club_to_team_id = {}
    for club in mp["club_norm"].dropna().unique():
        best = process.extractOne(club, team_norms, scorer=fuzz.token_sort_ratio)
        if best and best[1] >= team_cutoff:
            team_id = teams.loc[teams["team_norm"] == best[0], "team_id"].iloc[0]
            club_to_team_id[club] = team_id

    mp["team_id"] = mp["club_norm"].map(club_to_team_id)

    # ---- latest roster team per player_id ----
    roster["stint_end"] = pd.to_datetime(roster["stint_end"], errors="coerce")

    latest_team = (
        roster.sort_values(["player_id", "stint_end", "obs_count"], ascending=[True, False, False])
              .drop_duplicates("player_id", keep="first")[["player_id", "team_id"]]
    )

    lookup = (
        players.merge(latest_team, on="player_id", how="left")
               .dropna(subset=["team_id", "name_norm"])
               [["player_id", "team_id", "name_norm"]]
    )

    # ---- exact match (team_id + name_norm) ----
    out = mp.merge(
        lookup,
        on=["team_id", "name_norm"],
        how="left",
        suffixes=("", "_lk")
    )

    # ---- fuzzy fallback within team ----
    missing = out["player_id"].isna()
    if missing.any():
        team_map = {
            tid: sub[["name_norm", "player_id"]].values.tolist()
            for tid, sub in lookup.groupby("team_id")
        }

        # global fallback (optional but useful)
        global_choices = lookup[["name_norm", "player_id"]].values.tolist()
        global_names = [x[0] for x in global_choices]

        for i in out[missing].index:
            tid = out.at[i, "team_id"]
            name = out.at[i, "name_norm"]
            if pd.isna(tid) or not name:
                continue

            choices = team_map.get(tid, [])
            if choices:
                names = [x[0] for x in choices]
                best = process.extractOne(name, names, scorer=fuzz.token_sort_ratio)
                if best and best[1] >= cutoff:
                    out.at[i, "player_id"] = choices[best[2]][1]
                    continue

            # fallback global if team match fails
            best = process.extractOne(name, global_names, scorer=fuzz.token_sort_ratio)
            if best and best[1] >= cutoff:
                out.at[i, "player_id"] = global_choices[best[2]][1]

    out = out.drop(columns=["name_norm", "club_norm"], errors="ignore")

    print(f"team_id mapped: {out['team_id'].notna().sum()} / {len(out)}")
    print(f"players matched: {out['player_id'].notna().sum()} / {len(out)}")

    return out
