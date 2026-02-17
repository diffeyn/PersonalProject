import sql_upload as sql_upload
import pandas as pd

sql_upload.upload_to_sql(mls_latest_feed, 'match_events')

sql_upload.upload_to_sql(mls_latest_team_stats, 'matches_stats')

sql_upload.upload_to_sql(mls_latest_player_stats, 'match_player_stats')

