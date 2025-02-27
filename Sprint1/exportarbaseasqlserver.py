import sqlite3
import pandas as pd
from sqlalchemy import create_engine, text

# Datos de conexión
sqlite_db_path = r'C:\Users\emava\Documents\PLASS\Henry\PF_SPORTS_NBA\nba_cleaned_no_zeros_new.sqlite'  # Ruta completa a tu archivo SQLite
sql_server = 'EMA\SQLEXPRESS'  # Reemplaza con el nombre de tu servidor y instancia, por ejemplo, 'localhost\\SQLEXPRESS'
database_name = 'nba_modified_sql'  # Nombre de la base de datos de destino en SQL Server

# Conexión a SQLite
sqlite_conn = sqlite3.connect(sqlite_db_path)
sqlite_cursor = sqlite_conn.cursor()

# Conexión a SQL Server usando SQLAlchemy con pyodbc
connection_string = (
    f"mssql+pyodbc://@{sql_server}/{database_name}"
    "?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
)
engine = create_engine(connection_string)

# Función para crear la base de datos si no existe con autocommit
def create_database_if_not_exists(engine, db_name):
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(text(f"IF DB_ID('{db_name}') IS NULL CREATE DATABASE [{db_name}]"))

# Crear la base de datos
base_engine = create_engine(
    f"mssql+pyodbc://@{sql_server}/master"
    "?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
)
create_database_if_not_exists(base_engine, database_name)
base_engine.dispose()

# Actualizar el engine para la base de datos creada
engine = create_engine(connection_string)

# Obtener las tablas de SQLite
sqlite_cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = sqlite_cursor.fetchall()

for table_name_tuple in tables:
    table_name = table_name_tuple[0]
    print(f"Migrando tabla: {table_name}")
    
    # Leer la tabla en un DataFrame de pandas
    df = pd.read_sql_query(f"SELECT * FROM {table_name}", sqlite_conn)
    
    # Escribir la tabla en SQL Server
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)
    print(f"Tabla {table_name} migrada correctamente.")

# Cerrar las conexiones
sqlite_conn.close()
engine.dispose()

print("Migración completada exitosamente.")