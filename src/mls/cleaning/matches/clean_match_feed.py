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
        
    df = df.sort_values(by=['date', 'match_id', 'feed_id'], ascending=[False, True, False]).reset_index(drop=False)
    
    df = df.drop(columns=['in_player', 'out_player'])

    df = df[['match_id', 'date', 'feed_id', 'minute', 'title', 'comment']]
    
    df.rename(columns={'minute' : 'event_minute', 'title': 'event_type', 'comment' : 'event_comment', 'feed_id' : 'event_id'}, inplace=True)
    
    df = df[['event_id','match_id', 'event_minute', 'event_type', 'event_comment']]
    
    return df
