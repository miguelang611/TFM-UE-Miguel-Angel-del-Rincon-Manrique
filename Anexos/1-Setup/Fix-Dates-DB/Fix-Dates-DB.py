import mysql.connector
from mysql.connector import Error
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
import uuid
import csv

# Configuración del logging con la fecha y hora de ejecución
execution_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
log_filename = f'log_{execution_time}.txt'
logging.basicConfig(filename=log_filename, level=logging.INFO, format='%(asctime)s - %(message)s')

# Archivo para guardar los cambios en CSV
csv_filename = f'cambios_{execution_time}.csv'

def connect_to_database(host, user, password, database):
    """Conecta a la base de datos y devuelve el objeto de conexión."""
    try:
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            connection_timeout=5,
        )
        logging.info("Conexión establecida con la base de datos.")
        return conn
    except Error as e:
        logging.error(f"Error al conectar a la base de datos: {e}")
        return None

def fetch_datetime_columns(host, user, password, database):
    """Obtiene todas las columnas de tipo datetime, date o timestamp que requieren cambio de valor predeterminado."""
    conn = connect_to_database(host, user, password, database)
    if conn is None:
        return []

    cursor = conn.cursor()
    datetime_columns = []

    try:
        cursor.execute(f"""
            SELECT TABLE_NAME, COLUMN_NAME 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = '{database}' 
            AND DATA_TYPE IN ('date', 'datetime', 'timestamp');
        """)
        datetime_columns = cursor.fetchall()
    except Error as e:
        logging.error(f"Error al obtener columnas de tipo datetime: {e}")
    finally:
        cursor.close()
        conn.close()

    return datetime_columns

def fetch_primary_key(conn, table_name):
    """Obtiene el nombre de la clave primaria de una tabla si existe."""
    cursor = conn.cursor()
    pk_column = None
    try:
        cursor.execute(f"""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{table_name}' AND COLUMN_KEY = 'PRI';
        """)
        result = cursor.fetchone()
        if result:
            pk_column = result[0]
        
        # Leer cualquier resultado no leído para evitar errores
        while cursor.nextset():
            pass
    except Error as e:
        logging.error(f"Error al obtener la clave primaria para la tabla {table_name}: {e}")
    finally:
        cursor.close()
    return pk_column

def prepare_changes_for_table(host, user, password, database, table_name, date_columns):
    """Prepara los cambios en los campos de fecha en una tabla específica y los registra."""
    conn = connect_to_database(host, user, password, database)
    if conn is None:
        return []

    cambios = []
    cursor = conn.cursor()

    pk_column = fetch_primary_key(conn, table_name)
    
    if not pk_column:
        logging.error(f"No se encontró clave primaria para la tabla {table_name}.")
        cursor.close()
        conn.close()
        return []

    query = f"SELECT {pk_column}, " + ", ".join(date_columns) + f" FROM {table_name} WHERE " + " OR ".join(
        [f"{col} = '0000-00-00 00:00:00' OR {col} IS NULL" for col in date_columns]
    )

    try:
        cursor.execute(query)
        rows = cursor.fetchall()
        logging.info(f"Registros candidatos para cambio en {table_name}: {len(rows)} encontrados.")

        for row in rows:
            pk_value = row[0]
            updates = {}
            old_values = {}

            for i, column_name in enumerate(date_columns, start=1):
                current_value = row[i]

                if current_value == '0000-00-00 00:00:00' or current_value is None:
                    old_values[column_name] = current_value
                    
                    if column_name in ['ends_at', 'finished_at']:
                        updates[column_name] = '2099-01-01 00:00:00'
                    elif column_name == 'birth_date':
                        updates[column_name] = '1971-01-01'
                    elif column_name in ['updated_at', 'activated_at', 'services_unlocked_at', 'welcomed_at', 'profile_modified_at']:
                        created_at_value = row[date_columns.index('created_at') + 1] if 'created_at' in date_columns else None
                        updates[column_name] = created_at_value if created_at_value and created_at_value != '0000-00-00 00:00:00' else '1971-01-01 00:00:00'
                    else:
                        updates[column_name] = '1971-01-01 00:00:00'

            if updates:
                id_change = str(uuid.uuid4())
                for col, new_value in updates.items():
                    cambios.append((id_change, pk_column, pk_value, {col: new_value}, {col: old_values[col]}, table_name, col, "Corrección automática"))
                    logging.info(f"Cambio preparado - Tabla: {table_name}, PK: {pk_value}, Columna: {col}, Valor antiguo: {old_values[col]}, Nuevo valor: {new_value}")

    except Error as e:
        logging.error(f"Error al ejecutar la consulta SELECT en {table_name}: {e}")
    finally:
        cursor.close()
        conn.close()

    return cambios

def apply_batch(host, user, password, database, batch):
    """Aplica un lote de cambios a la base de datos de manera eficiente."""
    conn = connect_to_database(host, user, password, database)
    if conn is None:
        return False

    cursor = conn.cursor()
    try:
        update_queries = []
        update_values = []
        table_name = batch[0][5]  # Assuming all items in the batch are for the same table

        for id_change, pk_column, pk_value, updates, old_values, _, col, new_value_origin in batch:
            update_queries.append(f"WHEN %s THEN %s")
            update_values.extend([pk_value, updates[col]])

        if update_queries:
            update_query = f"UPDATE {table_name} SET {col} = CASE {pk_column} " + " ".join(update_queries) + f" END WHERE {pk_column} IN ({', '.join(['%s'] * len(batch))})"
            update_values.extend([pk_value for _, _, pk_value, _, _, _, _, _ in batch])

            cursor.execute(update_query, tuple(update_values))
            conn.commit()
            logging.info(f"Aplicación del lote desde UUID {batch[0][0]} hasta UUID {batch[-1][0]} en la tabla {table_name}: correcta.")
            print(f"Aplicación del lote desde UUID {batch[0][0]} hasta UUID {batch[-1][0]} en la tabla {table_name}: correcta.")
            return True
        else:
            logging.warning(f"No se encontraron actualizaciones válidas en el lote desde UUID {batch[0][0]} hasta UUID {batch[-1][0]} para la tabla {table_name}.")
            return False
    except Error as e:
        conn.rollback()
        logging.error(f"Error al aplicar el lote desde UUID {batch[0][0]} hasta UUID {batch[-1][0]} en la tabla {table_name}: {e}")
        print(f"Error al aplicar el lote desde UUID {batch[0][0]} hasta UUID {batch[-1][0]} en la tabla {table_name}: {e}")
        return False
    finally:
        cursor.close()
        conn.close()

def apply_changes_in_parallel(host, user, password, database, cambios, batch_size=1000):
    """Aplica los cambios en paralelo a la base de datos usando ProcessPoolExecutor."""
    total_cambios = len(cambios)
    print(f"Total de cambios: {total_cambios}")
    logging.info(f"Total de cambios: {total_cambios}")
    batches = [cambios[i:i + batch_size] for i in range(0, total_cambios, batch_size)]

    with ProcessPoolExecutor() as executor:
        futures = [executor.submit(apply_batch, host, user, password, database, batch) for batch in batches]
        for future in as_completed(futures):
            future.result()  # Espera a que cada lote se complete

def update_all_tables(host, user, password, database):
    """Actualiza las fechas para todas las tablas en la base de datos."""
    datetime_columns = fetch_datetime_columns(host, user, password, database)
    if not datetime_columns:
        logging.info("No se encontraron columnas de tipo datetime para actualizar.")
        return

    cambios_totales = []

    with ProcessPoolExecutor() as executor:
        futures = []
        for table_name, column_name in datetime_columns:
            date_columns = ['created_at', 'updated_at'] if column_name in ('created_at', 'updated_at') else [column_name]
            logging.info(f"Preparando cambios para la tabla: {table_name} y columna: {column_name}")
            futures.append(executor.submit(prepare_changes_for_table, host, user, password, database, table_name, date_columns))

        for future in as_completed(futures):
            result = future.result()
            if result:
                cambios_totales.extend(result)

    apply_changes_in_parallel(host, user, password, database, cambios_totales)

    # Crear o actualizar triggers para cada tabla
    conn = connect_to_database(host, user, password, database)
    if conn:
        for table_name, column_name in datetime_columns:
            create_or_update_trigger(conn, table_name, [column_name])
        conn.close()

    logging.info("Actualización y creación de triggers completada para todas las tablas.")

def create_or_update_trigger(conn, table_name, columns):
    """Crea o actualiza los triggers para una tabla específica."""
    cursor = conn.cursor()
    
    trigger_logic = "\n".join([f"""
        IF NEW.{col} IS NULL OR NEW.{col} = '0000-00-00 00:00:00' THEN
            SET NEW.{col} = CASE
                WHEN '{col}' IN ('ends_at', 'finished_at') THEN '2099-12-31 23:59:59'
                WHEN '{col}' = 'birth_date' THEN '1971-01-01'
                WHEN '{col}' IN ('updated_at', 'activated_at', 'services_unlocked_at', 'welcomed_at', 'profile_modified_at') AND NEW.created_at IS NOT NULL AND NEW.created_at != '0000-00-00 00:00:00' THEN NEW.created_at
                ELSE '1971-01-01 00:00:00'
            END;
        END IF;
    """ for col in columns])

    for event in ['INSERT', 'UPDATE']:
        trigger_name = f"{table_name}_{event.lower()}_date_check"
        
        drop_trigger_query = f"DROP TRIGGER IF EXISTS {trigger_name}"
        create_trigger_query = f"""
        CREATE TRIGGER {trigger_name}
        BEFORE {event} ON {table_name}
        FOR EACH ROW
        BEGIN
            {trigger_logic}
        END;
        """
        
        try:
            cursor.execute(drop_trigger_query)
            cursor.execute(create_trigger_query)
            conn.commit()
            logging.info(f"Trigger {trigger_name} creado/actualizado exitosamente para la tabla {table_name}")
        except Error as e:
            logging.error(f"Error al crear/actualizar el trigger {trigger_name} para la tabla {table_name}: {e}")

    cursor.close()
