import pandas as pd

def upload_to_db(df, table_name, engine):
    df.to_sql(table_name, engine, if_exists='append', index=False)
