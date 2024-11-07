import functions_framework
import requests
import json
import os
import sqlalchemy
from google.cloud.sql.connector import Connector
import pymysql
from datetime import datetime

# Configuración de la instancia de Cloud SQL
db_name = os.environ["DB_NAME"]
instance_connection_name = os.environ["INSTANCE_CONNECTION_NAME"]
db_user = os.environ["DB_USER"]
enable_iam_auth = os.environ.get("ENABLE_IAM_AUTH", "True").lower() == "true"

# Inicializar el conector
connector = Connector()

# Función para crear la conexión
def getconn():
    conn = connector.connect(
        instance_connection_name,
        "pymysql",
        user=db_user,
        password=None,
        db=db_name,
        enable_iam_auth=enable_iam_auth
    )
    return conn

# Crear el pool de conexiones
engine = sqlalchemy.create_engine(
    "mysql+pymysql://",
    creator=getconn,
)

def test_db_connection():
    try:
        with engine.connect() as connection:
            result = connection.execute(sqlalchemy.text("SELECT 1"))
            print("Database connection successful")
            return True
    except Exception as e:
        print(f"Database connection failed: {str(e)}")
        return False

def get_microsoft_ip_ranges():
    url = 'https://raw.githubusercontent.com/femueller/cloud-ip-ranges/master/microsoft-azure-ip-ranges.json'
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Error fetching JSON file: HTTP {response.status_code}")
        return None
    try:
        return json.loads(response.text)
    except json.JSONDecodeError:
        print("Error decoding JSON")
        return None

def filter_ip_ranges(ip_ranges):
    microsoft_ips = []
    relevant_services = [
        'PowerBI',
        'Fabric',
        'OneLake'
    ]
    
    for value in ip_ranges['values']:
        if any(service.lower() in value['name'].lower() for service in relevant_services):
            for address_prefix in value.get('properties', {}).get('addressPrefixes', []):
                microsoft_ips.append({
                    'value': address_prefix,
                    'name': f"microsoft-ips-{value['name'][:50]}"
                })
    
    return microsoft_ips

@functions_framework.cloud_event
def update_microsoft_ips_whitelist(cloud_event):
    print(f"Starting whitelist update at {datetime.now()}")
    
    # Probar la conexión a la base de datos
    if not test_db_connection():
        return "Database connection failed", 500

    try:
        # Obtener los rangos de IP de Microsoft
        ip_ranges = get_microsoft_ip_ranges()
        if ip_ranges is None:
            return "Failed to fetch IP ranges from Microsoft", 500

        print(f"Retrieved IP ranges from Microsoft")
        
        # Filtrar las IPs relevantes
        microsoft_ips = filter_ip_ranges(ip_ranges)
        print(f"Filtered {len(microsoft_ips)} relevant IP ranges")

        # Actualizar la whitelist en la base de datos
        if microsoft_ips:
            with engine.connect() as conn:
                conn.execute(sqlalchemy.text("DELETE FROM whitelist WHERE name LIKE 'microsoft-ips-%'"))
                for ip in microsoft_ips:
                    conn.execute(
                        sqlalchemy.text("INSERT INTO whitelist (name, value) VALUES (:name, :value)"),
                        parameters=ip
                    )
            print(f"Whitelist updated with {len(microsoft_ips)} IP ranges")
        else:
            print("No relevant IP ranges found to update")

        # Mostrar un resumen de los servicios encontrados
        services_found = set(ip['name'].split('-')[2] for ip in microsoft_ips)
        print("Services found:")
        for service in services_found:
            print(f"- {service}")

        print(f"Whitelist update completed at {datetime.now()}")
        return f'Whitelist update process completed. {len(microsoft_ips)} IP ranges processed.'
    except Exception as e:
        print(f"Error updating whitelist: {str(e)}")
        import traceback
        print(traceback.format_exc())
        return f"Error updating whitelist: {str(e)}", 500
    finally:
        connector.close()
