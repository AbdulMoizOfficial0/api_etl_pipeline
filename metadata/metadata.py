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
    entity_data = {}

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
    namespace = {'edm': 'http://docs.oasis-open.org/odata/ns/edm'}
    entity_types = root.findall('.//edm:EntityType', namespace)

    for entity_type in entity_types:
        entity_name = entity_type.get('Name')
        classes.append(entity_name)
        properties = [prop.get('Name') for prop in entity_type.findall('edm:Property', namespace)]
        entity_data[entity_name] = properties

    return classes, entity_data, source_id

def class_insertion(classes, source_id):
    conn = db_conn()
    cur = conn.cursor()

    for class_name in classes:
        query = """
                INSERT INTO demo.class_metadata (source_id, source_name, class_name, download_flag)
                VALUES (%s, %s, %s, %s);
                """
        try:
            cur.execute(query, (source_id, 'demo', class_name, True))
        except pymysql.MySQLError as e:
            print(f"Error inserting into table: {e}")
    conn.commit()
    conn.close()
    print("Data insertion completed.")

def field_insertion(entity_data, source_id):
    conn = db_conn()
    cur = conn.cursor()

    for class_name, properties in entity_data.items():
        for prop_name in properties:
            trimmed_prop_name = prop_name[:100] if len(prop_name) > 100 else prop_name
            query = """
                        INSERT INTO field_metadata (source_id, source_name, property_name, class_name, download_flag)
                        VALUES (%s, %s, %s, %s, %s);
                        """
            try:
                cur.execute(query, (source_id, 'demo', trimmed_prop_name, class_name, True))
            except pymysql.MySQLError as e:
                print(f"Error inserting into field_metadata: {e}")

    conn.commit()
    conn.close()


db = db_conn()
source_id = 897
classes, entity_data, source_id = metadata_download(source_id)
class_insertion(classes, source_id)
field_insertion(entity_data, source_id)





