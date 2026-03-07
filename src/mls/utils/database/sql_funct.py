from sqlalchemy import text

def upload_to_db(df, table_name, engine):
    # Build upsert: insert, skip on duplicate key
    cols = ", ".join([f"`{c}`" for c in df.columns])
    placeholders = ", ".join([f":{c}" for c in df.columns])
    update_clause = ", ".join([f"`{c}` = VALUES(`{c}`)" for c in df.columns])
    
    sql = text(f"INSERT IGNORE INTO {table_name} ({cols}) VALUES ({placeholders})")
    
    with engine.begin() as conn:
        conn.execute(sql, df.to_dict(orient="records"))