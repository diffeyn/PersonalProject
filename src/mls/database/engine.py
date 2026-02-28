from sqlalchemy import create_engine
import os



def make_engine():
    db_string = os.getenv("DB_STRING")
    return create_engine(db_string)