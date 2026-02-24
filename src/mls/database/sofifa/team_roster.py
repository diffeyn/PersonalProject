from mls.database.engine import make_engine
from sqlalchemy import text
import pandas as pd



def create_snapshot_df(df, engine):
    df = df.copy()
    df.rename(columns={"id": "player_id"}, inplace=True)
    # access teams table in SQL to get team_id mapping
    teams_df = pd.read_sql("SELECT team_id, team_name FROM teams", engine)
    
    # merge to get team_id, player_id, and date columns
    snap_df = df.merge(teams_df, left_on="team_name", right_on="team_name", how="left")
    snap_df = snap_df[["date", "team_id", "player_id"]]
    snap_df.rename(columns={"date": "snap_date"}, inplace=True)
    return snap_df


def upsert_roster_snapshots(engine, snap_df: pd.DataFrame):
    snap_df = snap_df.copy()
    snap_df["snap_date"] = pd.to_datetime(snap_df["snap_date"]).dt.date
    snap_df[["team_id", "player_id"]] = snap_df[["team_id", "player_id"]].astype(int)

    # simplest approach: append and rely on PK with INSERT IGNORE via SQL
    rows = snap_df.to_dict("records")
    sql = text("""
        INSERT IGNORE INTO roster_snapshots (snap_date, team_id, player_id)
        VALUES (:snap_date, :team_id, :player_id)
    """)
    with engine.begin() as conn:
        conn.execute(sql, rows)

def read_snapshots(engine) -> pd.DataFrame:
    df = pd.read_sql("SELECT snap_date, team_id, player_id FROM roster_snapshots", engine)
    df["snap_date"] = pd.to_datetime(df["snap_date"])
    df["team_id"] = df["team_id"].astype(int)
    df["player_id"] = df["player_id"].astype(int)
    return df

def compute_stints(snap: pd.DataFrame) -> pd.DataFrame:
    s = snap.sort_values(["player_id", "snap_date", "team_id"]).copy()
    s["prev_team"] = s.groupby("player_id")["team_id"].shift()
    s["jump"] = (s["team_id"] != s["prev_team"]).astype(int)
    s.loc[s.groupby("player_id").head(1).index, "jump"] = 1
    s["stint_id"] = s.groupby("player_id")["jump"].cumsum()

    stints = (
        s.groupby(["player_id", "stint_id", "team_id"])
         .agg(
             stint_start=("snap_date", "min"),
             stint_end=("snap_date", "max"),
             days_observed=("snap_date", lambda x: (x.max() - x.min()).days + 1),
             obs_count=("snap_date", "count"),
         )
         .reset_index()
         .sort_values(["player_id", "stint_start"])
    )

    # MySQL DATE friendly
    stints["stint_start"] = pd.to_datetime(stints["stint_start"]).dt.date
    stints["stint_end"] = pd.to_datetime(stints["stint_end"]).dt.date
    return stints

def refresh_stage(engine, stints_df: pd.DataFrame):
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE team_roster_stage"))
    stints_df.to_sql("team_roster_stage", con=engine, if_exists="append", index=False, method="multi", chunksize=2000)

def upsert_stage_into_prod(engine):
    sql = text("""
        INSERT INTO team_roster
        (player_id, stint_id, team_id, stint_start, stint_end, days_observed, obs_count)
        SELECT
          player_id, stint_id, team_id, stint_start, stint_end, days_observed, obs_count
        FROM team_roster_stage
        ON DUPLICATE KEY UPDATE
          team_id = VALUES(team_id),
          stint_start = VALUES(stint_start),
          stint_end = VALUES(stint_end),
          days_observed = VALUES(days_observed),
          obs_count = VALUES(obs_count);
    """)
    with engine.begin() as conn:
        conn.execute(sql)

def refresh_team_roster(engine, new_snapshot_df):
    # 1) create snapshot df with team_id, player_id, snap_date
    snap_df = create_snapshot_df(new_snapshot_df, engine)
    
    # 2) store weekly snapshot
    upsert_roster_snapshots(engine, snap_df)

    # 3) rebuild stints from all snapshots
    snaps = read_snapshots(engine)
    stints = compute_stints(snaps)

    # 3) stage + merge
    refresh_stage(engine, stints)
    upsert_stage_into_prod(engine)