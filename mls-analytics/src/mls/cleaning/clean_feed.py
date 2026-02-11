import pandas as pd
import re

def clean_feed(df):
    """
    Clean and standardize match feed data from a DataFrame.
    This function processes raw match feed data by:
    - Standardizing event titles based on comment content (Corner, Foul, Offside)
    - Removing lineup announcements and match status events (KICK OFF, HALF TIME, etc.)
    - Filtering out rows without minute values
    - Reversing the order of events and resetting the index
    - Handling substitution events by filling missing titles and formatting comments
    - Creating sequential feed IDs for each match
    - Parsing and standardizing date formats to datetime objects
    - Selecting and reordering relevant columns
    Args:
        df (pandas.DataFrame): Input DataFrame containing match feed data with columns:
            - match_id: Unique identifier for the match
            - date: Match date (string format)
            - minute: Minute of the event
            - title: Event title/type
            - comment: Additional event description
            - in_player: Player entering (for substitutions)
            - out_player: Player leaving (for substitutions)
    Returns:
        pandas.DataFrame: Cleaned DataFrame with columns:
            - match_id: Unique identifier for the match
            - date: Standardized datetime object
            - feed_id: Sequential event number within each match
            - minute: Minute of the event
            - title: Standardized event title
            - comment: Processed event description
    """
    df = df.copy()
    
    df['title'] = df.apply(lambda x: 'Corner' if pd.notna(x['comment']) and 'corner' in x['comment'].lower() else x['title'], axis=1)

    df['title'] = df.apply(lambda x: 'Foul' if pd.notna(x['comment']) and 'foul' in x['comment'].lower() else x['title'], axis=1)

    df['title'] = df.apply(lambda x: 'Offside' if pd.notna(x['comment']) and 'offside' in x['comment'].lower() else x['title'], axis=1)   
    
    df = df[~df['comment'].str.contains('Lineups', na=False)]

    df = df[~df['title'].str.contains('KICK OFF|HALF TIME|FULL TIME|END OF SECOND HALF', na=False)]
    
    df = df[df['minute'].notna()]
    
    df = df.iloc[::-1].reset_index(drop=True)
    
    df['title'] = df['title'].fillna('Substitution')

    df['comment'] = df.apply(lambda x: f"Substitution: {x['out_player']} out, {x['in_player']} in" if x['title'] == 'Substitution' else x['comment'], axis=1)
    
    df['feed_id'] = df.groupby('match_id').cumcount() + 1
    
    df['date'] = df['date'].astype(str)
    
    df['date'] = df['date'].str.replace(r"^[A-Za-z]+ ", "", regex=True)

    month_map = {
        "january": "01", "february": "02", "march": "03", "april": "04",
        "may": "05", "june": "06", "july": "07", "august": "08",
        "september": "09", "october": "10", "november": "11", "december": "12"
    }

    df['date'] = df['date'].apply(
        lambda x: re.sub(
            r"[A-Za-z]+",
            lambda m: month_map.get(m.group(0).lower(), m.group(0)),
            x
        )
    )

    df['date'] = df['date'].apply(
        lambda x: x + " 2025" if not re.search(r"\b\d{4}\b", x) else x
    )
    
    df['date'] = pd.to_datetime(df['date'], format="%m %d %Y")
    
    df = df.sort_values(by=['date', 'match_id', 'feed_id'], ascending=[False, True, False]).reset_index(drop=False)
    
    df = df.drop(columns=['in_player', 'out_player'])

    df = df[['match_id', 'date', 'feed_id', 'minute', 'title', 'comment']]
    
    df.rename(columns={'minute' : 'event_minute', 'title': 'event_type', 'comment' : 'event_comment', 'feed_id' : 'event_id'}, inplace=True)
    
    df = df[['event_id','match_id', 'event_minute', 'event_type', 'event_comment']]
    
    return df
