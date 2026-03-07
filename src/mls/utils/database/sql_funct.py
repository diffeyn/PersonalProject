from sqlalchemy import text

def upload_to_db(df, table_name, engine, method='ignore'):
    if df.empty:
        return
    
    records = df.to_dict(orient='records')
    cols = ', '.join(f'`{c}`' for c in df.columns)
    placeholders = ', '.join([f':{c}' for c in df.columns])
    
    if method == 'ignore':
        sql = f"INSERT IGNORE INTO {table_name} ({cols}) VALUES ({placeholders})"
    elif method == 'replace':
        sql = f"REPLACE INTO {table_name} ({cols}) VALUES ({placeholders})"
    else:
        sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"
    
    with engine.connect() as conn:
        conn.execute(text(sql), records)
        conn.commit()