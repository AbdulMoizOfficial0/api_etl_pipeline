import os
import xml.etree.ElementTree as ET
import requests
from dotenv import load_dotenv
import pymysql

load_dotenv()



def db_conn():
    conn = pymysql.connect(
        database=os.getenv("db_name"),
        port=int(os.getenv("db_port")),
        user=os.getenv("db_user"),
        password=os.getenv("db_password")
    )
    return conn


# Function to check and create the table if it doesn't exist
def check_and_create_table(conn):
    cur = conn.cursor()

    # Check if the table exists in the 'dev' schema
    cur.execute("""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'demo'
            AND table_name = 'class_metadata'
        );
    """)

    table_exists = cur.fetchone()[0]

    if not table_exists:
        # Table doesn't exist, so create it
        create_table_sql = '''
            CREATE TABLE class_metadata (
                source_id INT,
                source_name VARCHAR(255),
                resource_name VARCHAR(255),
                class_name VARCHAR(255),
                download_flag BOOLEAN
            );
        '''
        cur.execute(create_table_sql)
        conn.commit()
        print("Table dev.class_metadata created successfully.")
    else:
        print("Table dev.class_metadata already exists.")


# Insert class names into the database
def insertion(conn, class_names, source_id, source_name, resource_name):
    cur = conn.cursor()
    class_names_insert = '''
        INSERT INTO demo.class_metadata (source_id, source_name, resource_name, class_name, download_flag)
        VALUES (%s, %s, %s, %s, %s)
    '''
    for class_name in class_names:
        cur.execute(class_names_insert, (source_id, source_name, resource_name, class_name, True))
    conn.commit()
    print(f"Class names {', '.join(class_names)} inserted successfully with download_flag set to TRUE.")


# Getting Classes from the metadata
def metadata_download():
    class_names = []
    class_data = {}
    api_url = os.getenv("api_url")
    api_key = os.getenv("api_key")
    headers = {
        "Authorization": f"Bearer {api_key}"
    }

    resp = requests.get(url=api_url, headers=headers)
    resp.raise_for_status()

    xml_content = resp.content
    root = ET.fromstring(xml_content)

    namespaces = {
        'edmx': 'http://docs.oasis-open.org/odata/ns/edmx',
        'edm': 'http://docs.oasis-open.org/odata/ns/edm'
    }

    for entity in root.findall('.//edm:EntityType', namespaces):
        entity_name = entity.get('Name')
        class_names.append(entity_name)

        if entity_name not in class_data:
            class_data[entity_name] = []

        for col in entity.findall('.//{*}Property'):
            col_name = col.attrib['Name']
            class_data[entity_name].append(col_name)

    return class_names, class_data

conn = db_conn()

try:
    check_and_create_table(conn)

    class_names, class_data = metadata_download()
    source_id = 1
    source_name = "ODataService"
    resource_name = "API_Metadata"

    insertion(conn, class_names, source_id, source_name, resource_name)

finally:
    conn.close()
