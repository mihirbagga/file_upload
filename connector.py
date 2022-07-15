import subprocess
from impala.dbapi import connect
import psycopg2
import configparser
from psycopg2 import Error
from sqlalchemy import create_engine, false
from pathlib import Path
import pandas as pd

base_path = Path(__file__).parent
CFG_PATH = (base_path / "cusa.conf").resolve()
config = configparser.ConfigParser()

config.read(CFG_PATH)
db = config['POSTGRES']


class PostgresConnector:

    def PostgreSQL_connect():
        conn = psycopg2.connect(dbname=db['database'], user=db['user'], password=db['password'],
                                host=db['host'], port=db['port'], application_name='PostgreSQL')
        if conn:
            print('connected')
            return conn
        else:
            print('not connected')


def get_data(did, **kwargs):
    engine = create_engine('postgresql+psycopg2://', creator=PostgresConnector.PostgreSQL_connect,
                           pool_pre_ping=True, pool_size=1, max_overflow=1)
    db_conn = engine.connect()
    try:
        return pd.read_sql('SELECT * from {};'.format(did), db_conn)
    except Exception as error:
        print(error)
        return false
    finally:
        db_conn.close()


def add_data(username, mobile, name, email, password, address, firmName, userType, is_archived):
    connection = PostgresConnector.PostgreSQL_connect()
    connection.autocommit = False
    cursor = connection.cursor()
    try:
        query = 'insert into user_data (username, mobile,full_name,email,"password",address,firm_name,user_type,is_archived) values (\'{}\',\'{}\',\'{}\',\'{}\',\'{}\',\'{}\',\'{}\',\'{}\',\'{}\');'.format(
            username, mobile, name, email, password, address, firmName, userType, is_archived)
        print(query)
        cursor.execute(query)
        connection.commit()
        return True
    except Exception as error:
        connection.rollback()
        return False
    finally:
        if connection:
            cursor.close()
            connection.close()


def login(username, password):
    engine = create_engine('postgresql+psycopg2://', creator=PostgresConnector.PostgreSQL_connect,
                           pool_pre_ping=True, pool_size=1, max_overflow=1)
    db_conn = engine.connect()
    try:
        data = pd.read_sql('select * from user_data where username=\'{}\' and password=\'{}\''.format(
            username, password), db_conn)
        if data.empty:
            return False
        else:
            return True
    except Exception as error:
        return False
    finally:
        db_conn.close()


def update_data(id, name, table):
    connection = PostgresConnector.PostgreSQL_connect()
    connection.autocommit = False
    cursor = connection.cursor()
    try:
        query = 'update  {}  set category_name=\'{}\' where category_id={} ;'.format(
            table, name, id)
        print(query)
        cursor.execute(query)
        connection.commit()
        return True
    except Exception as error:
        connection.rollback()
        return False
    finally:
        if connection:
            cursor.close()
            connection.close()


def delete_data(id, table):
    connection = PostgresConnector.PostgreSQL_connect()
    connection.autocommit = False
    cursor = connection.cursor()
    try:
        query = 'delete  from  {} where id={};'.format(table, id)
        print(query)
        cursor.execute(query)
        connection.commit()
        return True
    except Exception as error:
        connection.rollback()
        return False
    finally:
        if connection:
            cursor.close()
            connection.close()
