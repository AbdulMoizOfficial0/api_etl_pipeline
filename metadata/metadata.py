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

def metadata_download(source_id):
    conn = db_conn()
    cur = conn.cursor()
    classes = []
    class_data = []

    get_api = '''SELECT auth->>'$.loginUrl' AS api_url FROM sources WHERE source_id = %s;'''
    get_key = '''SELECT auth->>'$.password' AS api_key FROM sources WHERE source_id = %s;'''

    # source_api
    cur.execute(get_api, (source_id,))
    api_url = [row[0] for row in cur.fetchall()]

    # source_key
    cur.execute(get_key, (source_id,))
    api_key = [row[0] for row in cur.fetchall()]
    header = {
        "Authorization": f"Bearer {api_key[0]}"
    }
    resp = requests.get(url=api_url[0], headers=header)
    xml_data = resp.text
    root = ET.fromstring(xml_data)
    entity_types = root.findall('.//{http://docs.oasis-open.org/odata/ns/edm}EntityType')

    for entity_type in entity_types:
        entity_name = entity_type.get('Name')
        classes.append(entity_name)

        entries = root.findall(f'.//{entity_name}')

        for entry in entries:
            for prop in entry:
                prop_name = prop.get('Name')
                prop_value = prop.text
                class_data.append(prop_value)
    print(class_data)



db = db_conn()
metadata_download(897)


