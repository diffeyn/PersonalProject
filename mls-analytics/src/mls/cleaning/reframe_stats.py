import pandas as pd
import re
def reframe_stats(df, fname: str | None = None):
    """
    Transform a long-format statistics DataFrame into a wide-format DataFrame with separate home/away columns.
    This function takes a DataFrame with statistics in long format (with 'stat', 'home_value', and 'away_value' columns)
    and pivots it into wide format where each statistic becomes two columns (one for home, one for away).
    It also extracts match metadata (teams and date) from the filename if provided.
    Parameters
    ----------
    df : pd.DataFrame
        A DataFrame in long format containing at minimum the columns: 'stat', 'home_value', and 'away_value'.
        May optionally contain a 'match_id' column which will be preserved in the output.
        The DataFrame may have a 'source_filename' attribute in df.attrs.
    fname : str or None, optional
        Filename containing match information in the format: '{home}_vs_{away}_{MM-DD-YYYY}'.
        If None, attempts to use df.attrs['source_filename']. Default is None.
    Returns
    -------
    pd.DataFrame
        A wide-format DataFrame where each statistic from the input becomes two columns:
        '{stat}_home' and '{stat}_away'. Additional columns include:
        - 'match_id': Match identifier (if present in input)
        - 'teams_home', 'teams_away': Team codes extracted from filename
        - 'match_date': Match date extracted from filename (only one column, away date is dropped)
        Column names are normalized: spaces replaced with underscores, '%' replaced with 'pct',
        hyphens replaced with underscores, and all converted to lowercase.
    Raises
    ------
    KeyError
        If the input DataFrame is missing required columns: 'stat', 'home_value', or 'away_value'.
    Notes
    -----
    - The filename is expected to match the pattern: '{home}_vs_{away}_{MM-DD-YYYY}' (case-insensitive)
    - Team codes are converted to uppercase
    - The 'date_away' column is dropped as it's redundant with 'date_home' (renamed to 'match_date')
    """
    if fname is None:
        fname = str(df.attrs.get('source_filename', '') or '')

    df = df.copy()
    m = re.search(r'([a-z]{3})[ _-]*v?s[ _-]*([a-z]{3}).*?(\d{2}-\d{2}-\d{4})', fname, re.I)

    parts = []
    if m:
        home, away, date_str = m.groups()
        home, away = home.upper(), away.upper()
        date = pd.to_datetime(date_str, format="%m-%d-%Y")
        parts.append(pd.DataFrame({'home_value': [home], 'away_value': [away], 'stat': ['teams']}))
        parts.append(pd.DataFrame({'home_value': [date], 'away_value': [date], 'stat': ['date']}))

    if parts:
        df = pd.concat([df, *parts], ignore_index=True)

    need = {'stat', 'home_value', 'away_value'}
    missing = need - set(df.columns)
    if missing:
        raise KeyError(f"reframe_stats expected {need}; missing {missing}. Got: {list(df.columns)[:10]}")

    out = {}
    for _, row in df.iterrows():
        out[f"{row['stat']}_home"] = row['home_value']
        out[f"{row['stat']}_away"] = row['away_value']

    wide = pd.DataFrame([out])
    if 'date_away' in wide.columns:
        wide = wide.drop(columns=['date_away'])
    wide = wide.rename(columns={'date_home': 'match_date'})
    wide.columns = (pd.Index(wide.columns)
                    .str.replace(' ', '_')
                    .str.replace('%', 'pct')
                    .str.replace('-', '_')
                    .str.lower())
    
    if 'match_id' in df.columns:                
        wide.insert(0, 'match_id', df['match_id'].iloc[0])  
        
    return wide
