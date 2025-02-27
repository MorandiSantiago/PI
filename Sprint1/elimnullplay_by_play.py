import sqlite3
import os
import sys
import shutil

def obtener_columnas_a_eliminar(db_path, table_name, umbral_porcentaje=20.0):
    """
    Identifica las columnas de una tabla que tienen más del porcentaje de valores nulos especificado.

    :param db_path: Ruta a la base de datos SQLite.
    :param table_name: Nombre de la tabla a analizar.
    :param umbral_porcentaje: Porcentaje máximo permitido de valores nulos por columna.
    :return: Lista de nombres de columnas a eliminar.
    """
    if not os.path.exists(db_path):
        print(f"Error: La base de datos en {db_path} no existe.")
        sys.exit(1)

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Obtener el número total de filas
        cursor.execute(f"SELECT COUNT(*) FROM '{table_name}'")
        total_filas = cursor.fetchone()[0]
        print(f"Total de filas en la tabla '{table_name}': {total_filas}\n")

        # Obtener información de las columnas
        cursor.execute(f"PRAGMA table_info('{table_name}')")
        columnas_info = cursor.fetchall()

        columnas_a_eliminar = []
        print(f"Calculando valores nulos por columna en la tabla '{table_name}':\n")

        for columna in columnas_info:
            nombre_columna = columna[1]
            # Contar los valores nulos en la columna
            cursor.execute(f"SELECT COUNT(*) FROM '{table_name}' WHERE \"{nombre_columna}\" IS NULL")
            contador_nulos = cursor.fetchone()[0]
            porcentaje_nulos = (contador_nulos / total_filas) * 100

            print(f"Columna '{nombre_columna}': {contador_nulos} valores nulos ({porcentaje_nulos:.2f}%)")

            if porcentaje_nulos > umbral_porcentaje:
                columnas_a_eliminar.append(nombre_columna)

        print(f"\nColumnas a ELIMINAR (más del {umbral_porcentaje}% de nulos):")
        for col in columnas_a_eliminar:
            print(f"- {col}")

        return columnas_a_eliminar

    except sqlite3.Error as e:
        print(f"Error al procesar la base de datos: {e}")
        sys.exit(1)
    finally:
        if conn:
            conn.close()

def copiar_tablas_original_a_nueva(original_db, nueva_db, exclude_tables=None):
    """
    Copia todas las tablas desde la base de datos original a la nueva base de datos, excluyendo ciertas tablas.

    :param original_db: Ruta a la base de datos original.
    :param nueva_db: Ruta a la nueva base de datos.
    :param exclude_tables: Lista de tablas a excluir de la copia.
    """
    if exclude_tables is None:
        exclude_tables = []

    try:
        conn_origen = sqlite3.connect(original_db)
        conn_nueva = sqlite3.connect(nueva_db)
        cursor_origen = conn_origen.cursor()
        cursor_nueva = conn_nueva.cursor()

        # Obtener todas las tablas en la base de datos original
        cursor_origen.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tablas = cursor_origen.fetchall()

        for tabla in tablas:
            nombre_tabla = tabla[0]
            if nombre_tabla in exclude_tables:
                print(f"Excluyendo la tabla '{nombre_tabla}' de la copia.")
                continue

            # Obtener el esquema de la tabla
            cursor_origen.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{nombre_tabla}'")
            esquema = cursor_origen.fetchone()[0]

            # Crear la tabla en la nueva base de datos
            cursor_nueva.execute(esquema)
            conn_nueva.commit()
            print(f"Tabla '{nombre_tabla}' creada en la nueva base de datos.")

            # Copiar los datos
            cursor_origen.execute(f"SELECT * FROM '{nombre_tabla}'")
            filas = cursor_origen.fetchall()

            placeholders = ", ".join(["?"] * len(filas[0])) if filas else ""
            insert_sql = f"INSERT INTO '{nombre_tabla}' VALUES ({placeholders})" if placeholders else ""

            if filas:
                cursor_nueva.executemany(insert_sql, filas)
                conn_nueva.commit()
                print(f"{len(filas)} filas copiadas a la tabla '{nombre_tabla}'.")
            else:
                print(f"No hay datos para copiar en la tabla '{nombre_tabla}'.")

        conn_origen.close()
        conn_nueva.close()

    except sqlite3.Error as e:
        print(f"Error al copiar tablas: {e}")
        sys.exit(1)

def recrear_play_by_play(original_db, nueva_db, table_name, columnas_a_eliminar):
    """
    Recrea la tabla 'play_by_play' en la nueva base de datos sin las columnas especificadas.

    :param original_db: Ruta a la base de datos original.
    :param nueva_db: Ruta a la nueva base de datos.
    :param table_name: Nombre de la tabla a recrear.
    :param columnas_a_eliminar: Lista de columnas a eliminar.
    """
    try:
        conn_origen = sqlite3.connect(original_db)
        cursor_origen = conn_origen.cursor()

        conn_nueva = sqlite3.connect(nueva_db)
        cursor_nueva = conn_nueva.cursor()

        # Obtener información de las columnas
        cursor_origen.execute(f"PRAGMA table_info('{table_name}')")
        columnas_info = cursor_origen.fetchall()
        todas_columnas = [col[1] for col in columnas_info]
        columnas_a_mantener = [col for col in todas_columnas if col not in columnas_a_eliminar]

        if not columnas_a_mantener:
            print("Error: Todas las columnas serán eliminadas. No se creará la tabla.")
            return

        print(f"\nColumnas a mantener en '{table_name}': {columnas_a_mantener}")

        # Obtener los tipos de datos de las columnas a mantener
        columnas_info_dict = {col[1]: col[2] for col in columnas_info if col[1] in columnas_a_mantener}

        # Construir el esquema para la nueva tabla
        columnas_definiciones = ", ".join([f"\"{col}\" {tipo}" for col, tipo in columnas_info_dict.items()])
        nueva_tabla = f"{table_name}_temp"

        create_table_sql = f"CREATE TABLE '{nueva_tabla}' ({columnas_definiciones})"
        cursor_nueva.execute(create_table_sql)
        conn_nueva.commit()
        print(f"Tabla temporal '{nueva_tabla}' creada en la nueva base de datos.")

        # Insertar datos en la nueva tabla
        columnas_encoded = ", ".join([f"\"{col}\"" for col in columnas_a_mantener])
        select_columns = ", ".join([f"\"{col}\"" for col in columnas_a_mantener])
        cursor_origen.execute(f"SELECT {select_columns} FROM '{table_name}'")
        filas = cursor_origen.fetchall()

        insert_sql = f"INSERT INTO '{nueva_tabla}' ({columnas_encoded}) VALUES ({', '.join(['?'] * len(columnas_a_mantener))})"
        cursor_nueva.executemany(insert_sql, filas)
        conn_nueva.commit()
        print(f"{len(filas)} filas copiadas a la tabla temporal '{nueva_tabla}'.")

        # Eliminar la tabla original en la nueva base de datos si existe
        cursor_nueva.execute(f"DROP TABLE IF EXISTS '{table_name}'")
        conn_nueva.commit()
        print(f"Tabla original '{table_name}' eliminada en la nueva base de datos.")

        # Renombrar la tabla temporal a la original
        cursor_nueva.execute(f"ALTER TABLE '{nueva_tabla}' RENAME TO '{table_name}'")
        conn_nueva.commit()
        print(f"Tabla temporal '{nueva_tabla}' renombrada a '{table_name}' en la nueva base de datos.")

        conn_origen.close()
        conn_nueva.close()

    except sqlite3.Error as e:
        print(f"Error al recrear la tabla '{table_name}': {e}")
        sys.exit(1)

def main():
    # Rutas de las bases de datos
    original_db = r"C:\Users\emava\Documents\PLASS\Henry\PF_SPORTS_NBA\nba.sqlite"
    nueva_db = r"C:\Users\emava\Documents\PLASS\Henry\PF_SPORTS_NBA\nba_modified.sqlite"

    # Nombre de la tabla a procesar
    table_name = "play_by_play"

    # Porcentaje de umbral para eliminar columnas
    umbral_porcentaje = 20.0  # 20%

    # Paso 1: Identificar columnas a eliminar
    columnas_a_eliminar = obtener_columnas_a_eliminar(original_db, table_name, umbral_porcentaje)

    if not columnas_a_eliminar:
        print("No hay columnas que eliminar según el umbral especificado.")
        sys.exit(0)

    # Paso 2: Copiar todas las tablas excepto 'play_by_play' a la nueva base de datos
    print("\nCopiando tablas a la nueva base de datos...")
    copiar_tablas_original_a_nueva(original_db, nueva_db, exclude_tables=[table_name])

    # Paso 3: Recrear 'play_by_play' sin las columnas eliminadas
    print("\nRecreando la tabla 'play_by_play' sin las columnas eliminadas...")
    recrear_play_by_play(original_db, nueva_db, table_name, columnas_a_eliminar)

    print(f"\nProceso completado. La nueva base de datos se encuentra en '{nueva_db}'.")

if __name__ == "__main__":
    main()