import pandas as pd


def clean_teams(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and transform raw team data into a standardized format.
    This function processes a DataFrame containing team information by:
    - Removing the first and last columns
    - Renaming columns to standardized names
    - Parsing team formation into lineup and style components
    - Cleaning team names by removing "Major League Soccer" text
    - Converting worth values from Euro format (e.g., "€50M") to numeric
    - Converting specified columns to numeric types
    - Setting team_id as the index
    Parameters
    ----------
    df : pd.DataFrame
        Raw DataFrame containing team data with columns such as Name, ID, 
        Formation, Overall, Attack, Midfield, Defence, Players, and 
        Club worth/Club.worth.
    Returns
    -------
    pd.DataFrame
        Cleaned DataFrame with standardized column names, numeric types where
        appropriate, parsed formation data, and team_id as the index.
    Notes
    -----
    - The function creates a copy of the input DataFrame to avoid modifying 
      the original data.
    - Formation strings are expected to be in the format "lineup style [trash]".
    - Worth values are expected to be in Euro format with 'M' suffix for millions.
    - Any conversion errors when casting to numeric types will result in NaN values.
    """
    df = df.copy()

    df = df.iloc[:, 1:-1]

    rename_map = {
        "Name": "team_name",
        "ID": "team_id",
        "Formation": "team_formation",
        "Overall": "overall_score",
        "Attack": "attack",
        "Midfield": "midfield",
        "Defence": "defense",
        "Players": "num_players",
    }

    for possible in ["Club worth", "Club.worth"]:
        if possible in df.columns:
            rename_map[possible] = "worth_euro"

    df = df.rename(columns=rename_map)

    if "team_formation" in df.columns:
        split_cols = df["team_formation"].str.split(" ", expand=True)
        if split_cols.shape[1] >= 2:
            df["lineup"] = split_cols[0]
            df["style"] = split_cols[1]
        if split_cols.shape[1] >= 3:
            df["trash2"] = split_cols[2]
        else:
            df["trash2"] = None

    df["team_name"] = df["team_name"].str.replace("Major League Soccer", "", regex=False)

    if "worth_euro" in df.columns:
        df["worth_euro"] = (
            df["worth_euro"]
            .astype(str)
            .str.replace("€", "", regex=False)
            .str.replace("M", "", regex=False)
        )

    numeric_cols = ["overall_score", "attack", "midfield", "defense", "worth_euro", "num_players"]
    for c in numeric_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    if "trash2" in df.columns:
        df = df.drop(columns=["trash2"])

    if "team_id" in df.columns:
        df = df.set_index("team_id")
    
    df['date'] = pd.to_datetime(df['date'])
        
    df = df.reset_index().set_index('date').sort_index()
    
    df = df.rename(columns={'lineup': 'formation_base', 'style': 'formation_style', 'overall_score': 'overall', 'defense': 'defence', 'worth_euro': 'club_worth', 'team_name': 'name', 'num_players': 'players'}) 
        
    return df