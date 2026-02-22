import pandas as pd
import re

def clean_match_feed(df):
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
    
   # 1) make string + remove venue (anything after •)
    s = df["date"].astype(str).str.split("•", n=1).str[0].str.strip()

    # 2) remove leading weekday if present (Sunday, Mon, etc.)
    s = s.str.replace(
        r"^(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\s+",
        "",
        regex=True
    )

    # 3) optional: collapse multiple spaces
    s = s.str.replace(r"\s+", " ", regex=True).str.strip()

    # 4) parse dates (handles both "Feb 16 2026" and "10 5 2025")
    # Try strict known formats first, then fall back to inference.
    dt = pd.to_datetime(s, format="%b %d %Y", errors="coerce")  # Feb 16 2026
    dt2 = pd.to_datetime(s, format="%B %d %Y", errors="coerce") # February 16 2026
    dt3 = pd.to_datetime(s, format="%m %d %Y", errors="coerce") # 10 5 2025

    df["date"] = dt.fillna(dt2).fillna(dt3)
        
    df = df.sort_values(by=['date', 'match_id', 'feed_id'], ascending=[False, True, False]).reset_index(drop=False)
    
    df = df.drop(columns=['in_player', 'out_player'])

    df = df[['match_id', 'date', 'feed_id', 'minute', 'title', 'comment']]
    
    df.rename(columns={'minute' : 'event_minute', 'title': 'event_type', 'comment' : 'event_comment', 'feed_id' : 'event_id'}, inplace=True)
    
    df = df[['event_id','match_id', 'event_minute', 'event_type', 'event_comment']]
    
    return df
