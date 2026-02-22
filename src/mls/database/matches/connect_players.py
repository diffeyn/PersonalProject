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

def last_name_norm(full_name):
    n = norm(full_name)
    if not n:
        return None
    parts = n.split()
    return parts[-1] if parts else None

def first_initial(full_name):
    if pd.isna(full_name):
        return None
    s = str(full_name).strip()
    return s[0].lower() if s else None


def attach_player_ids(match_players, engine, cutoff=70, team_cutoff=80):
    mp = match_players.copy()

    # ---- load DB tables ----
    players = pd.read_sql(text("SELECT player_id, name FROM players_general"), engine)
    roster = pd.read_sql(text("""
        SELECT player_id, team_id, stint_end, obs_count
        FROM team_roster
    """), engine)
    teams = pd.read_sql(text("SELECT team_id, team_abbr FROM teams"), engine)

    # ---- normalize scraped ----
    mp["name_norm"] = mp["player_name"].map(norm)
    mp["club_norm"] = mp["club"].map(norm)
    mp["last_norm"] = mp["player_name"].map(last_name_norm)
    mp["fi"] = mp["player_name"].map(first_initial)

    # ---- normalize DB ----
    players["name_norm"] = players["name"].map(norm)

    teams["abbr_norm"] = teams["team_abbr"].map(norm)
    abbr_to_team_id = dict(zip(teams["abbr_norm"], teams["team_id"]))

    # ---- map club -> team_id (exact abbr first, fuzzy only if needed) ----
    mp["team_id"] = mp["club_norm"].map(abbr_to_team_id)

    unmapped = mp["team_id"].isna() & mp["club_norm"].notna()
    if unmapped.any():
        team_norms = teams["abbr_norm"].dropna().tolist()
        club_to_team_id = {}
        for club in mp.loc[unmapped, "club_norm"].unique():
            best = process.extractOne(club, team_norms, scorer=fuzz.token_sort_ratio)
            if best and best[1] >= team_cutoff:
                club_to_team_id[club] = teams.loc[teams["abbr_norm"] == best[0], "team_id"].iloc[0]
        mp.loc[unmapped, "team_id"] = mp.loc[unmapped, "club_norm"].map(club_to_team_id)

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
               .copy()
    )

    # enrich lookup for initial/last matching
    lookup["last_norm"] = lookup["name_norm"].map(lambda x: x.split()[-1] if isinstance(x, str) and x else None)
    lookup["fi"] = lookup["name_norm"].map(lambda x: x[0] if isinstance(x, str) and x else None)

    # ---- pass 1: exact match (team_id + name_norm) ----
    out = mp.merge(
        lookup[["player_id", "team_id", "name_norm"]],
        on=["team_id", "name_norm"],
        how="left",
    )

    # ---- pass 2: initial+last fallback within team (handles "J. Ferreira") ----
    missing = out["player_id"].isna()
    if missing.any():
        lk2 = (
            lookup.dropna(subset=["team_id", "last_norm", "fi", "player_id"])
                  .drop_duplicates(subset=["team_id", "last_norm", "fi"])
        )
        key_to_pid = {
            (r.team_id, r.last_norm, r.fi): r.player_id
            for r in lk2.itertuples(index=False)
        }

        for i in out[missing].index:
            tid = out.at[i, "team_id"]
            ln = out.at[i, "last_norm"]
            fi = out.at[i, "fi"]
            pid = key_to_pid.get((tid, ln, fi))
            if pid is not None:
                out.at[i, "player_id"] = pid

    # ---- pass 3: fuzzy fallback within team ----
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

    # ---- cleanup ----
    out = out.drop(columns=["name_norm", "club_norm", "last_norm", "fi"], errors="ignore")

    print(f"team_id mapped: {out['team_id'].notna().sum()} / {len(out)}")
    print(f"players matched: {out['player_id'].notna().sum()} / {len(out)}")

    # optional: show a few remaining misses
    misses = out[out["player_id"].isna()][["match_id", "club", "player_name", "team_id"]].drop_duplicates()
    if len(misses):
        print("unmatched examples:")
        print(misses.head(15).to_string(index=False))

    return out