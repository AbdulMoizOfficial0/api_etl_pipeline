import os
import xml.etree.ElementTree as ET
import requests
import pymysql
import configparser

config = configparser.ConfigParser()
config.read('config.ini')

DB_USER = config['database']['user']
DB_PASSWORD = config['database']['password']
DB_HOST = config['database']['host']
DB_PORT = int(config['database']['port'])
DB_NAME = config['database']['database']


# Making Connection with database
def db_conn():
    conn = pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )
    return conn



