import pandas as pd
import re
import unicodedata

# -------------------------
# name normalization
# -------------------------
def norm_name(x):
    if pd.isna(x):
        return None
    x = unicodedata.normalize("NFKD", str(x))
    x = "".join(c for c in x if not unicodedata.combining(c))
    x = x.lower().strip()
    x = re.sub(r"[^\w\s]", "", x)
    x = re.sub(r"\s+", " ", x)
    return x or None

# -------------------------
# team mapping
# -------------------------
team_map = {
    "Atlanta United": "ATL",
    "Austin FC": "ATX",
    "CF Montréal": "MTL",
    "Charlotte FC": "CLT",
    "Chicago Fire FC": "CHI",
    "FC Cincinnati": "CIN",
    "Colorado Rapids": "COL",
    "Columbus Crew": "CLB",
    "D.C. United": "DC",
    "FC Dallas": "DAL",
    "Houston Dynamo FC": "HOU",
    "Sporting Kansas City": "SKC",
    "LA Galaxy": "LA",
    "Los Angeles Football Club": "LAFC",
    "Inter Miami CF": "MIA",
    "Minnesota United": "MIN",
    "Minnesota United FC": "MIN",
    "Nashville SC": "NSH",
    "New England Revolution": "NE",
    "New York City Football Club": "NYC",
    "New York City FC": "NYC",
    "New York Red Bulls": "RBNY",
    "Orlando City": "ORL",
    "Philadelphia Union": "PHI",
    "Portland Timbers": "POR",
    "Real Salt Lake": "RSL",
    "San Diego FC": "SD",
    "San Jose Earthquakes": "SJ",
    "Seattle Sounders FC": "SEA",
    "St. Louis CITY SC": "STL",
    "Toronto FC": "TOR",
    "Vancouver Whitecaps FC": "VAN"
}

def clean_team_name(s):
    if pd.isna(s):
        return None
    s = str(s)
    s = re.sub(r'\bFootball Club\b', '', s, flags=re.I)
    s = re.sub(r'\bCF\b', '', s, flags=re.I)
    s = re.sub(r'\bFC\b', '', s, flags=re.I)
    s = re.sub(r'\bSC\b', '', s, flags=re.I)
    s = re.sub(r'\bUnited\b', '', s, flags=re.I)
    s = re.sub(r'\.', '', s)
    s = s.strip().lower()
    s = re.sub(r'\s+', ' ', s)
    return s

team_map_clean = {clean_team_name(k): v for k, v in team_map.items()}

# -------------------------
# remove glued position suffixes from Sofifa "Name"
# -------------------------
POS_CODES = ["GK","RWB","LWB","RB","LB","CB","CDM","CM","CAM","RM","LM","RW","LW","CF","ST"]
POS_CODES = sorted(POS_CODES, key=len, reverse=True)
pos_pat = re.compile(rf"^(?P<name>.*?)(?P<pos>(?:{'|'.join(POS_CODES)})+)$")

def split_name_pos(s):
    if pd.isna(s):
        return pd.Series([None, None])
    s = str(s).strip()
    m = pos_pat.match(s)
    if m:
        return pd.Series([m.group("name").strip(), m.group("pos").strip()])
    return pd.Series([s, None])

# -------------------------
# parse "Team & Contract" field: "GK(14)2018 ~ 2020"
# -------------------------
team_contract_pat = re.compile(r'(?P<pos>\w+)\((?P<num>\d+)\)(?P<start>\d{4})\s*~\s*(?P<end>\d{4})')

def parse_team_contract(s):
    if pd.isna(s):
        return pd.Series([pd.NA, pd.NA, pd.NA, pd.NA])
    s = str(s)
    m = team_contract_pat.search(s)
    if not m:
        return pd.Series([pd.NA, pd.NA, pd.NA, pd.NA])
    return pd.Series([m.group("pos"), m.group("num"), m.group("start"), m.group("end")])

# -------------------------
# money parsing ("€150K", "€1.2M") robust
# -------------------------
def parse_money_eur(x):
    if pd.isna(x):
        return pd.NA
    s = str(x)
    s = s.replace("â‚¬", "€")
    s = s.replace("€", "").replace(",", "").strip()
    mult = 1
    if s.endswith(("K", "k")):
        mult = 1_000
        s = s[:-1]
    elif s.endswith(("M", "m")):
        mult = 1_000_000
        s = s[:-1]
    try:
        return int(float(s) * mult)
    except:
        return pd.NA

def parse_height_cm(x):
    if pd.isna(x): 
        return pd.NA
    s = str(x)
    m = re.search(r"(\d+)\s*cm", s, flags=re.I)
    return int(m.group(1)) if m else pd.NA

def parse_weight_kg(x):
    if pd.isna(x): 
        return pd.NA
    s = str(x)
    m = re.search(r"(\d+)\s*kg", s, flags=re.I)
    return int(m.group(1)) if m else pd.NA

# -------------------------
# safe eval for "85+2" etc (your original behavior)
# -------------------------
def safe_eval(x):
    try:
        s = str(x)
        if '+' in s:
            return sum(int(i) for i in s.split('+'))
        if '-' in s:
            parts = [int(i) for i in s.split('-')]
            return parts[0] - sum(parts[1:])
        return int(s)
    except:
        return x

# -------------------------
# FINAL schema (exact)
# -------------------------
FINAL_COLS = [
    'id', 'date', 'name', 'age', 'height_cm', 'weight_kg', 'team_name',
    'contract_start', 'contract_end', 'position', 'foot', 'jersey_num',
    'wage_eur', 'value_eur', 'overall_rating', 'best_overall',
    'best_position', 'total_attacking', 'crossing', 'finishing',
    'heading_accuracy', 'short_passing', 'volleys', 'total_skill',
    'dribbling', 'curve', 'fk_accuracy', 'long_passing', 'ball_control',
    'total_movement', 'acceleration', 'sprint_speed', 'agility',
    'reactions', 'balance', 'total_power', 'shot_power', 'jumping',
    'stamina', 'long_shots', 'total_mentality', 'aggression',
    'interceptions', 'attack_position', 'vision', 'penalties', 'composure',
    'total_defending', 'defensive_awareness', 'standing_tackle',
    'sliding_tackle', 'total_goalkeeping', 'gk_diving', 'gk_handling',
    'gk_kicking', 'gk_positioning', 'gk_reflexes'
]

# -------------------------
# Cleaner for automation
# -------------------------
def clean_player_stats(df):
    df = df.copy()

    # drop Unnamed columns
    df = df.loc[:, ~df.columns.str.contains(r"^Unnamed", na=False)]

    # normalize colnames
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

    # tolerate legacy column names from your older cleaner
    rename_map = {
        "id": "id",
        "player_id": "id",
        "name": "name",
        "player_name": "name",
        "age": "age",
        "height": "height",
        "weight": "weight",
        "wage": "wage",
        "value": "value",
        "team": "team_name",
        "club": "team_name",
        "team_name": "team_name",
        "team_&_contract": "team_&_contract",
        "team_and_contract": "team_&_contract",
        "team_contract": "team_&_contract",
    }
    # Apply only those that exist
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # ---- required: id, name (create if possible) ----
    if "id" not in df.columns:
        raise ValueError("Need an id column (id or player_id).")
    if "name" not in df.columns:
        raise ValueError("Need a name column (name or player_name).")

    # ---- name cleanup (glued suffix + accent strip) ----
    df[["name_clean", "_pos_suffix"]] = df["name"].apply(split_name_pos)
    df["name"] = df["name_clean"].astype(str).apply(lambda x: unicodedata.normalize("NFKD", x))
    df["name"] = df["name"].apply(lambda x: "".join(c for c in x if not unicodedata.combining(c)))
    df["name"] = df["name"].str.replace("\xa0", " ", regex=False).str.replace("\u200b", "", regex=False)
    df["name"] = df["name"].str.replace(r"\s+", " ", regex=True).str.strip()
    df["_name_norm"] = df["name"].map(norm_name)

    # ---- team_name: backfill from common sources if empty/missing ----
    if "team_name" not in df.columns or df["team_name"].isna().all():
        for cand in ["team", "club", "squad"]:
            if cand in df.columns and df[cand].notna().any():
                df["team_name"] = df[cand]
                break
    if "team_name" not in df.columns:
        df["team_name"] = pd.NA

    df["team_name"] = (
        df["team_name"]
        .astype(str)
        .str.replace("\xa0", " ", regex=False)
        .str.strip()
        .replace({"nan": pd.NA, "None": pd.NA})
    )
    df["_team_abbr"] = df["team_name"].map(clean_team_name).map(team_map_clean)

    # ---- contract parsing from combined field ----
    if "team_&_contract" in df.columns and df["team_&_contract"].notna().any():
        df[["position", "jersey_num", "contract_start", "contract_end"]] = df["team_&_contract"].apply(parse_team_contract)
    else:
        for c in ["position", "jersey_num", "contract_start", "contract_end"]:
            if c not in df.columns:
                df[c] = pd.NA

    # ---- height/weight ----
    if "height_cm" not in df.columns:
        df["height_cm"] = df["height"].map(parse_height_cm) if "height" in df.columns else pd.NA
    if "weight_kg" not in df.columns:
        df["weight_kg"] = df["weight"].map(parse_weight_kg) if "weight" in df.columns else pd.NA

    # ---- money ----
    if "wage_eur" not in df.columns:
        df["wage_eur"] = df["wage"].map(parse_money_eur) if "wage" in df.columns else pd.NA
    if "value_eur" not in df.columns:
        df["value_eur"] = df["value"].map(parse_money_eur) if "value" in df.columns else pd.NA

    # ---- date ----
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"].astype(str), errors="coerce").dt.date
    else:
        df["date"] = pd.NA

    # ---- safe_eval on “stat” columns (skip obvious text/date columns) ----
    skip_eval = {"date", "name", "team_name", "foot", "best_position", "position"}
    for col in df.columns:
        if col in skip_eval:
            continue
        # only apply where it looks like sofifa math strings
        if df[col].dtype == object:
            df[col] = df[col].apply(safe_eval)

    # ---- ensure FINAL_COLS exist ----
    for c in FINAL_COLS:
        if c not in df.columns:
            df[c] = pd.NA

    # ---- numeric coercion for non-text FINAL_COLS ----
    text_cols = {"name", "team_name", "position", "foot", "best_position", "date"}
    for c in FINAL_COLS:
        if c in text_cols:
            continue
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # final exact schema/order
    return df[FINAL_COLS].copy()

