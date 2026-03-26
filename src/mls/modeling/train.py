"""
Step 1: Feature Importance Analysis
=====================================
Two methods:
  1. Random Forest Gini importance
  2. Absolute correlation with target

Features drawn from:
  - match_team_stats   (match-level team stats)
  - match_player_stats (match-level player stats, aggregated per match)
  - team_stats         (FIFA-style team ratings, most recent before match)
  - players_stats      (FIFA-style player ratings, averaged per squad via team_roster)
"""

import matplotlib
matplotlib.use("Agg")

import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder
import matplotlib.pyplot as plt
import seaborn as sns

from mls.database.engine import make_engine

engine = make_engine()

# ─────────────────────────────────────────────────────────────
# 1. LOAD DATA
# ─────────────────────────────────────────────────────────────
print("Loading tables...")
matches            = pd.read_sql("SELECT * FROM matches", engine)
match_team_stats   = pd.read_sql("SELECT * FROM match_team_stats", engine)
match_player_stats = pd.read_sql("SELECT * FROM match_player_stats", engine)
team_stats         = pd.read_sql("SELECT * FROM team_stats", engine)
players_stats      = pd.read_sql("SELECT * FROM players_stats", engine)
team_roster        = pd.read_sql("SELECT player_id, team_id, stint_start, stint_end FROM team_roster", engine)
teams              = pd.read_sql("SELECT team_id, team_abbr FROM teams", engine)

matches["date"]              = pd.to_datetime(matches["date"])
team_stats["date"]           = pd.to_datetime(team_stats["date"])
team_roster["stint_start"]   = pd.to_datetime(team_roster["stint_start"])
team_roster["stint_end"]     = pd.to_datetime(team_roster["stint_end"])

abbr_to_id = dict(zip(teams["team_abbr"], teams["team_id"]))
matches["home_team_id"] = matches["home_team"].map(abbr_to_id)
matches["away_team_id"] = matches["away_team"].map(abbr_to_id)

# ─────────────────────────────────────────────────────────────
# 2. TARGET
# ─────────────────────────────────────────────────────────────
def get_result(row):
    if row["home_score"] > row["away_score"]:   return "home_win"
    elif row["home_score"] < row["away_score"]: return "away_win"
    else:                                        return "draw"

matches["result"] = matches.apply(get_result, axis=1)

# ─────────────────────────────────────────────────────────────
# 3. BLOCK A — match_team_stats (drop leaky goal columns)
# ─────────────────────────────────────────────────────────────
LEAKY = ["shooting_goals_home", "shooting_goals_away",
         "general_goals_conceded_home", "general_goals_conceded_away"]
block_a = match_team_stats.drop(columns=LEAKY, errors="ignore")

# ─────────────────────────────────────────────────────────────
# 4. BLOCK B — match_player_stats aggregated per match
#    Sum counting stats, average percentage stats
#    (no home/away split possible without team_id on this table)
# ─────────────────────────────────────────────────────────────
print("Aggregating match_player_stats...")
pct_cols = [c for c in match_player_stats.columns if "perc" in c]
sum_cols  = [c for c in match_player_stats.columns
             if c not in pct_cols + ["match_id", "player_id"]]

agg_dict = {c: "sum" for c in sum_cols}
agg_dict.update({c: "mean" for c in pct_cols})

block_b = match_player_stats.groupby("match_id").agg(agg_dict).reset_index()
block_b.columns = ["match_id"] + [f"player_{c}" for c in block_b.columns[1:]]

# ─────────────────────────────────────────────────────────────
# 5. BLOCK C — team_stats: most recent rating before each match
#    Joined separately for home and away to avoid leakage
# ─────────────────────────────────────────────────────────────
print("Joining team ratings (may take a moment)...")
rating_cols = ["overall", "attack", "midfield", "defence"]

def latest_rating(team_id, match_date):
    subset = team_stats[
        (team_stats["team_id"] == team_id) &
        (team_stats["date"] < match_date)
    ]
    if subset.empty:
        return {c: np.nan for c in rating_cols}
    row = subset.sort_values("date").iloc[-1]
    return {c: row[c] for c in rating_cols}

rating_rows = []
for _, m in matches.iterrows():
    h = latest_rating(m["home_team_id"], m["date"])
    a = latest_rating(m["away_team_id"], m["date"])
    rating_rows.append({
        "match_id":       m["match_id"],
        **{f"team_{c}_home": h[c] for c in rating_cols},
        **{f"team_{c}_away": a[c] for c in rating_cols},
    })

block_c = pd.DataFrame(rating_rows)

# ─────────────────────────────────────────────────────────────
# 6. BLOCK D — players_stats averaged per squad via team_roster
#    For each match, find active squad members and average their
#    FIFA attribute ratings for home and away teams separately
# ─────────────────────────────────────────────────────────────
print("Averaging squad ratings via team_roster (slowest step)...")
ps_num_cols = [c for c in players_stats.select_dtypes(include=np.number).columns
               if c != "player_id"]

squad_rows = []
for _, m in matches.iterrows():
    entry = {"match_id": m["match_id"]}
    for label, tid in [("home", m["home_team_id"]), ("away", m["away_team_id"])]:
        squad_ids = team_roster[
            (team_roster["team_id"] == tid) &
            (team_roster["stint_start"] <= m["date"]) &
            ((team_roster["stint_end"].isna()) | (team_roster["stint_end"] >= m["date"]))
        ]["player_id"]

        squad_stats = players_stats[players_stats["player_id"].isin(squad_ids)]
        if squad_stats.empty:
            for c in ps_num_cols:
                entry[f"squad_{c}_{label}"] = np.nan
        else:
            avg = squad_stats[ps_num_cols].mean()
            for c in ps_num_cols:
                entry[f"squad_{c}_{label}"] = avg[c]
    squad_rows.append(entry)

block_d = pd.DataFrame(squad_rows)

# ─────────────────────────────────────────────────────────────
# 7. COMBINE ALL BLOCKS
# ─────────────────────────────────────────────────────────────
print("Combining all blocks...")
df = matches[["match_id", "result"]].copy()
df = df.merge(block_a, on="match_id", how="left")
df = df.merge(block_b, on="match_id", how="left")
df = df.merge(block_c, on="match_id", how="left")
df = df.merge(block_d, on="match_id", how="left")
df.dropna(inplace=True)

print(f"\nDataset: {len(df)} matches after dropping nulls")
print(f"Class distribution:\n{df['result'].value_counts()}")
print(f"Total features: {len(df.columns) - 2}\n")

# ─────────────────────────────────────────────────────────────
# 8. METHOD 1 — Random Forest Gini Importance
# ─────────────────────────────────────────────────────────────
FEATURE_COLS = [c for c in df.columns if c not in ["match_id", "result"]]
X = df[FEATURE_COLS]
le = LabelEncoder()
y  = le.fit_transform(df["result"])

print("Training Random Forest...")
rf = RandomForestClassifier(n_estimators=200, random_state=42, class_weight="balanced")
rf.fit(X, y)

importance_df = pd.DataFrame({
    "feature":    FEATURE_COLS,
    "importance": rf.feature_importances_
}).sort_values("importance", ascending=False)

print("Top 20 — Random Forest Gini importance:")
print(importance_df.head(20).to_string(index=False))

plt.figure(figsize=(10, 7))
sns.barplot(data=importance_df.head(20), x="importance", y="feature",
            hue="feature", legend=False, palette="Blues_r")
plt.title("Feature Importance — Random Forest (Gini)")
plt.xlabel("Importance")
plt.ylabel("")
plt.tight_layout()
plt.savefig("feature_importance_rf.png", dpi=150)
print("Saved: feature_importance_rf.png")

# ─────────────────────────────────────────────────────────────
# 9. METHOD 2 — Absolute Correlation with Target
# ─────────────────────────────────────────────────────────────
result_map = {"home_win": 2, "draw": 1, "away_win": 0}
df["result_num"] = df["result"].map(result_map)

correlations = X.corrwith(df["result_num"]).abs().sort_values(ascending=False)

print("\nTop 20 — Absolute correlation with result:")
print(correlations.head(20).to_string())

plt.figure(figsize=(10, 7))
correlations.head(20).sort_values().plot(kind="barh", color="steelblue")
plt.title("Feature Correlation with Match Result")
plt.xlabel("Absolute Correlation")
plt.tight_layout()
plt.savefig("feature_importance_corr.png", dpi=150)
print("Saved: feature_importance_corr.png")

# ─────────────────────────────────────────────────────────────
# 10. AGREED TOP FEATURES (top 20 of both methods)
# ─────────────────────────────────────────────────────────────
top_rf   = set(importance_df.head(20)["feature"])
top_corr = set(correlations.head(20).index)
agreed   = top_rf & top_corr

print(f"\nFeatures in top 20 of BOTH methods ({len(agreed)}):")
for f in sorted(agreed):
    print(f"  {f}")
print("\nThese are your strongest candidate features for the full model.")


mts = pd.read_sql("SELECT * FROM match_team_stats", engine)

pairs = [
    ("shooting_on_target_away", "xg_shots_on_target_away"),
    ("shooting_on_target_away", "general_shots_on_goal_away"),
    ("general_xg_conceded_home", "general_expected_goals_away"),
    ("general_xg_conceded_away", "general_expected_goals_home"),
    ("possession_81_85_home", "possession_86_90_home"),
]

for a, b in pairs:
    corr = mts[a].corr(mts[b])
    print(f"{a} vs {b}: {corr:.4f}")