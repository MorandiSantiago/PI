import sqlite3
import sys
import os

def obtener_tablas_y_shape(conn):
    cursor = conn.cursor()
    
    # Obtener los nombres de todas las tablas
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tablas = cursor.fetchall()
    
    if not tablas:
        print("No se encontraron tablas en la base de datos.")
        return []
    
    tablas_info = []
    
    print("Información de las tablas en la base de datos:")
    print("---------------------------------------------")
    
    for tabla in tablas:
        nombre_tabla = tabla[0]
        
        # Obtener información de las columnas
        cursor.execute(f"PRAGMA table_info(\"{nombre_tabla}\");")
        columnas_info = cursor.fetchall()
        num_columnas = len(columnas_info)
        
        # Obtener el número de filas
        cursor.execute(f"SELECT COUNT(*) FROM \"{nombre_tabla}\";")
        num_filas = cursor.fetchone()[0]
        
        print(f"Tabla: {nombre_tabla}")
        print(f"Shape: ({num_filas}, {num_columnas})\n")
        
        tablas_info.append({
            'nombre': nombre_tabla,
            'filas': num_filas,
            'columnas': num_columnas,
            'columnas_nombres': [col[1] for col in columnas_info]
        })
    
    return tablas_info

def copiar_base_de_datos(src_db_path, dest_db_path):
    if not os.path.exists(src_db_path):
        print(f"Error: La base de datos fuente '{src_db_path}' no existe.")
        sys.exit(1)
    
    if os.path.exists(dest_db_path):
        respuesta = input(f"El archivo de destino '{dest_db_path}' ya existe. ¿Deseas sobrescribirlo? (s/n): ")
        if respuesta.lower() != 's':
            print("Operación cancelada por el usuario.")
            sys.exit(0)
        else:
            os.remove(dest_db_path)
    
    try:
        # Conectar a la base de datos fuente
        src_conn = sqlite3.connect(src_db_path)
        src_cursor = src_conn.cursor()
        
        # Conectar a la base de datos destino
        dest_conn = sqlite3.connect(dest_db_path)
        dest_cursor = dest_conn.cursor()
        
        # Usar el comando .backup para copiar la base de datos
        with dest_conn:
            src_conn.backup(dest_conn)
        
        print(f"\nLa base de datos ha sido copiada exitosamente a '{dest_db_path}'.")
    
    except sqlite3.Error as e:
        print(f"Error al copiar la base de datos: {e}")
    
    finally:
        if src_conn:
            src_conn.close()
        if dest_conn:
            dest_conn.close()

def main():
    # Ruta de la base de datos original
    ruta_db_original = "nba_cleaned_no_zeros.sqlite"
    
    # Ruta para la nueva base de datos
    ruta_db_nueva = "nba_cleaned_no_zeros_new.sqlite"
    
    # Verificar que la base de datos original existe
    if not os.path.exists(ruta_db_original):
        print(f"Error: La base de datos '{ruta_db_original}' no se encontró en el directorio actual.")
        sys.exit(1)
    
    # Conectar a la base de datos original
    try:
        conn_original = sqlite3.connect(ruta_db_original)
    except sqlite3.Error as e:
        print(f"Error al conectar a la base de datos original: {e}")
        sys.exit(1)
    
    # Obtener y mostrar las tablas y sus shapes
    tablas_info = obtener_tablas_y_shape(conn_original)
    
    # Cerrar la conexión original temporalmente
    conn_original.close()
    
    # Copiar la base de datos a un nuevo archivo
    copiar_base_de_datos(ruta_db_original, ruta_db_nueva)

if __name__ == "__main__":
    main()