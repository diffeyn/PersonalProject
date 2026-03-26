from sqlalchemy import create_engine
import os
import pymysql



DB_STRING = 'mysql+pymysql://root:!wMoU9lBdLZWW6Y8@o8ukkP9TliImN.h.filess.io:58202/MLS'


def make_engine():
    db_string = DB_STRING
    return create_engine(db_string)