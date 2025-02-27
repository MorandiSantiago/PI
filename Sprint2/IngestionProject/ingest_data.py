import pyodbc
import pandas as pd
import yaml
import logging
import os
from dotenv import load_dotenv

# Cargar variables de entorno desde el archivo .env
load_dotenv()

# Configuración de Logging
LOG_DIR = os.path.join(os.getcwd(), "logs")
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    filename=os.path.join(LOG_DIR, "ingest.log"),
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

error_logger = logging.getLogger("error_logger")
error_handler = logging.FileHandler(os.path.join(LOG_DIR, "error.log"))
error_handler.setLevel(logging.ERROR)
error_logger.addHandler(error_handler)

def load_config(config_path='config/config.yaml'):
    # Verificar que el archivo de configuración exista
    if not os.path.exists(config_path):
        error_logger.error(f"Archivo de configuración no encontrado: {config_path}")
        raise FileNotFoundError(f"Archivo de configuración no encontrado: {config_path}")
    
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    
    # Reemplazar variables de entorno
    db_config = config['database']
    for key in db_config:
        if isinstance(db_config[key], str) and db_config[key].startswith("${") and db_config[key].endswith("}"):
            env_var = db_config[key][2:-1]
            env_value = os.getenv(env_var)
            if env_value is None:
                error_logger.error(f"Variable de entorno {env_var} no está definida.")
                raise EnvironmentError(f"Variable de entorno {env_var} no está definida.")
            db_config[key] = env_value
    
    config['database'] = db_config
    return config

def connect_to_db(config):
    try:
        conn = pyodbc.connect(
            f"DRIVER={{{config['driver']}}};"
            f"SERVER={config['server']};"
            f"DATABASE={config['database']};"
            f"UID={config['username']};"
            f"PWD={config['password']};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=yes;"
            f"SSLCA="#Copiar el path a la BD"
        )
        logging.info("Conexión a la base de datos establecida exitosamente.")
        return conn
    except Exception as e:
        error_logger.error(f"Error al conectar a la base de datos: {e}")
        raise

def ingest_table(conn, table_name, table_config):
    try:
        file_path = table_config['file_path']
        primary_keys = table_config['primary_key']
        columns = table_config['columns']

        if not os.path.exists(file_path):
            error_logger.error(f"Archivo no encontrado: {file_path} para la tabla {table_name}.")
            return

        # Cargar datos desde CSV usando pandas
        try:
            df = pd.read_csv(file_path, dtype=str)  # Leer todo como string para evitar problemas de formato
            logging.info(f"Datos cargados desde {file_path} para la tabla {table_name}. Filas: {len(df)}")
        except Exception as e:
            error_logger.error(f"Error al leer el archivo {file_path}: {e}")
            return

        cursor = conn.cursor()

        # Inicializar contador de registros procesados
        registros_procesados = 0

        for index, row in df.iterrows():
            try:
                # Recolectar valores para las columnas
                parametros = [row[col] if pd.notna(row[col]) else None for col in columns]
                
                # Construir las condiciones de clave primaria
                pk_conditions = " AND ".join([f"target.{pk} = source.{pk}" for pk in primary_keys])
                
                # Construir las asignaciones para UPDATE
                update_set = ", ".join([f"target.{col} = source.{col}" for col in columns if col not in primary_keys])
                
                # Construir la lista de columnas y valores para INSERT
                insert_cols = ", ".join(columns)
                insert_vals = ", ".join(["source."+col for col in columns])
                
                # Sentencia MERGE
                merge_query = f"""
                MERGE INTO {table_name} AS target
                USING (SELECT {', '.join(['?' for _ in columns])}) AS source ({', '.join(columns)})
                ON {pk_conditions}
                WHEN MATCHED THEN
                    UPDATE SET {update_set}
                WHEN NOT MATCHED THEN
                    INSERT ({insert_cols}) VALUES ({insert_vals});
                """
                
                # Parámetros para la consulta MERGE: solo necesitamos los parámetros una vez
                cursor.execute(merge_query, parametros)
                registros_procesados += 1
            except Exception as e:
                error_logger.error(f"Error al procesar la fila {index} en la tabla {table_name}: {e}")

        # Commit de la transacción
        conn.commit()
        logging.info(f"Ingesta completada para la tabla {table_name}. Registros procesados: {registros_procesados}")
    except Exception as e:
        error_logger.error(f"Error al ingerir la tabla {table_name}: {e}")
        conn.rollback()
        
def main():
    try:
        config = load_config()
        conn = connect_to_db(config['database'])
        for table, table_config in config['tables'].items():
            logging.info(f"Iniciando ingesta para la tabla: {table}")
            ingest_table(conn, table, table_config)
        conn.close()
        logging.info("Proceso de ingesta completado para todas las tablas.")
    except Exception as e:
        error_logger.error(f"Error en el proceso general de ingesta: {e}")

if __name__ == "__main__":
    main()