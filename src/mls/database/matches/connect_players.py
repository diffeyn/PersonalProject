import pandas as pd
from sqlalchemy import text
from rapidfuzz import process, fuzz
import re, unicodedata

def norm(x):
    """Normalize text for matching. Keep separators as spaces so hyphenated names don't get glued."""
    if pd.isna(x):
        return None
    x = unicodedata.normalize("NFKD", str(x))
    x = "".join(c for c in x if not unicodedata.combining(c))
    x = x.lower().strip()
    # IMPORTANT: replace punctuation with SPACE (not nothing)
    x = re.sub(r"[^\w\s]", " ", x)
    x = re.sub(r"\s+", " ", x).strip()
    return x or None

def first_initial(full_name):
    if pd.isna(full_name):
        return None
    s = str(full_name).strip()
    return s[0].lower() if s else None

def last1(full_name):
    n = norm(full_name)
    if not n:
        return None
    p = n.split()
    return p[-1] if p else None

def last2(full_name):
    n = norm(full_name)
    if not n:
        return None
    p = n.split()
    if len(p) >= 2:
        return " ".join(p[-2:])
    return p[-1] if p else None


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
    mp["fi"] = mp["player_name"].map(first_initial)
    mp["last1"] = mp["player_name"].map(last1)
    mp["last2"] = mp["player_name"].map(last2)

    # ---- normalize DB ----
    players["name_norm"] = players["name"].map(norm)

    teams["abbr_norm"] = teams["team_abbr"].map(norm)
    abbr_to_team_id = dict(zip(teams["abbr_norm"], teams["team_id"]))

    # ---- map club -> team_id (exact abbr first; fuzzy only if needed) ----
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
    lookup["fi"] = lookup["name_norm"].map(lambda x: x[0] if isinstance(x, str) and x else None)
    lookup["last1"] = lookup["name_norm"].map(lambda x: x.split()[-1] if isinstance(x, str) and x else None)
    lookup["last2"] = lookup["name_norm"].map(
        lambda x: " ".join(x.split()[-2:]) if isinstance(x, str) and x and len(x.split()) >= 2 else x
    )

    # ---- pass 1: exact match (team_id + name_norm) ----
    out = mp.merge(
        lookup[["player_id", "team_id", "name_norm"]],
        on=["team_id", "name_norm"],
        how="left",
    )

    # ---- pass 2: initial + last2, then last1 within team ----
    missing = out["player_id"].isna()
    if missing.any():
        lk2 = lookup.dropna(subset=["team_id", "fi", "player_id"]).copy()

        key_last2 = {
            (r.team_id, r.last2, r.fi): r.player_id
            for r in lk2.dropna(subset=["last2"])
                       .drop_duplicates(["team_id", "last2", "fi"])
                       .itertuples(index=False)
        }
        key_last1 = {
            (r.team_id, r.last1, r.fi): r.player_id
            for r in lk2.dropna(subset=["last1"])
                       .drop_duplicates(["team_id", "last1", "fi"])
                       .itertuples(index=False)
        }

        for i in out[missing].index:
            tid = out.at[i, "team_id"]
            fi = out.at[i, "fi"]
            if pd.isna(tid) or not fi:
                continue

            pid = key_last2.get((tid, out.at[i, "last2"], fi))
            if pid is None:
                pid = key_last1.get((tid, out.at[i, "last1"], fi))

            if pid is not None:
                out.at[i, "player_id"] = pid

    # ---- pass 3: fuzzy fallback within team, then global ----
    missing = out["player_id"].isna()
    if missing.any():
        team_map = {
            tid: sub[["name_norm", "player_id"]].values.tolist()
            for tid, sub in lookup.groupby("team_id")
        }

        global_choices = lookup[["name_norm", "player_id"]].values.tolist()
        global_names = [x[0] for x in global_choices]

        for i in out[missing].index:
            tid = out.at[i, "team_id"]
            name = out.at[i, "name_norm"]
            if pd.isna(tid) or not name:
                continue

            # team-scoped fuzzy
            choices = team_map.get(tid, [])
            if choices:
                names = [x[0] for x in choices]
                best = process.extractOne(name, names, scorer=fuzz.token_sort_ratio)
                if best and best[1] >= cutoff:
                    out.at[i, "player_id"] = choices[best[2]][1]
                    continue

            # global fuzzy fallback
            best = process.extractOne(name, global_names, scorer=fuzz.token_sort_ratio)
            if best and best[1] >= cutoff:
                out.at[i, "player_id"] = global_choices[best[2]][1]

    # ---- cleanup ----
    out = out.drop(columns=["name_norm", "club_norm", "fi", "last1", "last2"], errors="ignore")

    print(f"team_id mapped: {out['team_id'].notna().sum()} / {len(out)}")
    print(f"players matched: {out['player_id'].notna().sum()} / {len(out)}")

    misses = out[out["player_id"].isna()][["match_id", "club", "player_name", "team_id"]].drop_duplicates()
    if len(misses):
        print("unmatched examples:")
        print(misses.head(25).to_string(index=False))

    return out