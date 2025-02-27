import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import os
import pandas as pd
import yaml
import subprocess
import logging
from dotenv import load_dotenv

# Cargar variables de entorno desde el archivo .env
load_dotenv()

# Configuración de Logging
LOG_DIR = os.path.join(os.getcwd(), "logs")
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    filename=os.path.join(LOG_DIR, "ingest_gui.log"),
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

error_logger = logging.getLogger("error_logger")
error_handler = logging.FileHandler(os.path.join(LOG_DIR, "ingest_gui_error.log"))
error_handler.setLevel(logging.ERROR)
error_logger.addHandler(error_handler)

class IngestGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Automatización de Ingesta de Datos")
        self.root.geometry("800x600")  # Tamaño de la ventana

        # Cargar configuración
        self.config = self.load_config()

        # Crear Frame principal
        self.main_frame = ttk.Frame(root, padding="10")
        self.main_frame.pack(fill=tk.BOTH, expand=True)

        # Crear Notebook para pestañas
        self.notebook = ttk.Notebook(self.main_frame)
        self.notebook.pack(fill=tk.BOTH, expand=True)

        # Pestaña de Ingreso de Datos
        self.data_entry_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.data_entry_tab, text="Ingreso de Datos")

        # Pestaña de Configuración
        self.config_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.config_tab, text="Configuración")

        # Pestaña de Logs
        self.logs_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.logs_tab, text="Logs")

        # Ingreso de Datos
        self.setup_data_entry_tab()

        # Configuración
        self.setup_config_tab()

        # Logs
        self.setup_logs_tab()

    def load_config(self):
        config_path = os.path.join(os.getcwd(), "config", "config.yaml")
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
        return config

    def setup_data_entry_tab(self):
        # Selección de tabla
        ttk.Label(self.data_entry_tab, text="Selecciona la Tabla:").grid(row=0, column=0, sticky=tk.W, padx=5, pady=5)
        self.table_var = tk.StringVar()
        self.table_combo = ttk.Combobox(self.data_entry_tab, textvariable=self.table_var, state="readonly", width=30)
        self.table_combo['values'] = list(self.config['tables'].keys())
        self.table_combo.grid(row=0, column=1, sticky=(tk.W, tk.E), padx=5, pady=5)
        self.table_combo.bind("<<ComboboxSelected>>", self.load_table_fields)

        # Frame para campos dinámicos
        self.fields_frame = ttk.Frame(self.data_entry_tab, padding="10")
        self.fields_frame.grid(row=1, column=0, columnspan=2, sticky=(tk.W, tk.E, tk.N, tk.S), padx=5, pady=5)

        # Botón Guardar
        self.save_button = ttk.Button(self.data_entry_tab, text="Guardar", command=self.save_data)
        self.save_button.grid(row=2, column=0, columnspan=2, pady=10)

        # Variables para los campos
        self.entries = {}

    def setup_config_tab(self):
        # Botón para seleccionar archivo de configuración
        ttk.Label(self.config_tab, text="Archivo de Configuración:").grid(row=0, column=0, sticky=tk.W, padx=5, pady=5)
        self.config_file_path = tk.StringVar(value=os.path.join(os.getcwd(), "config", "config.yaml"))
        ttk.Entry(self.config_tab, textvariable=self.config_file_path, width=50).grid(row=0, column=1, sticky=(tk.W, tk.E), padx=5, pady=5)
        ttk.Button(self.config_tab, text="Seleccionar", command=self.select_config_file).grid(row=0, column=2, padx=5, pady=5)

        # Botón para recargar configuración
        ttk.Button(self.config_tab, text="Recargar Configuración", command=self.reload_config).grid(row=1, column=0, columnspan=3, pady=10)

    def setup_logs_tab(self):
        # Área de texto para mostrar logs
        self.log_text = tk.Text(self.logs_tab, wrap=tk.WORD, width=80, height=20)
        self.log_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), padx=5, pady=5)

        # Botón para actualizar logs
        ttk.Button(self.logs_tab, text="Actualizar Logs", command=self.update_logs).grid(row=1, column=0, pady=10)

        # Scrollbar para el área de texto
        scrollbar = ttk.Scrollbar(self.logs_tab, orient=tk.VERTICAL, command=self.log_text.yview)
        scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))
        self.log_text['yscrollcommand'] = scrollbar.set

    def load_table_fields(self, event):
        # Limpiar campos anteriores
        for widget in self.fields_frame.winfo_children():
            widget.destroy()
        self.entries.clear()

        selected_table = self.table_var.get()
        columns = self.config['tables'][selected_table]['columns']

        # Crear campos de entrada
        for idx, col in enumerate(columns):
            ttk.Label(self.fields_frame, text=f"{col}:").grid(row=idx, column=0, sticky=tk.W, pady=2, padx=5)
            entry = ttk.Entry(self.fields_frame, width=50)
            entry.grid(row=idx, column=1, sticky=(tk.W, tk.E), pady=2, padx=5)
            self.entries[col] = entry

    def save_data(self):
        selected_table = self.table_var.get()
        if not selected_table:
            messagebox.showerror("Error", "Por favor, selecciona una tabla.")
            return

        table_config = self.config['tables'][selected_table]
        file_path = os.path.join(os.getcwd(), table_config['file_path'])

        # Recolectar datos de los campos
        data = {}
        for col, entry in self.entries.items():
            value = entry.get().strip()
            if value == "":
                value = None  # Manejar valores nulos si es necesario
            data[col] = value

        # Crear DataFrame con el nuevo registro
        df_new = pd.DataFrame([data])

        try:
            if os.path.exists(file_path):
                # Leer datos existentes
                df_existing = pd.read_csv(file_path, dtype=str)
                # Concatenar y eliminar duplicados basados en claves primarias
                primary_keys = table_config['primary_key']
                df_combined = pd.concat([df_existing, df_new], ignore_index=True)
                df_unique = df_combined.drop_duplicates(subset=primary_keys, keep='last')
            else:
                df_unique = df_new

            # Guardar el archivo CSV
            df_unique.to_csv(file_path, index=False)
            logging.info(f"Datos guardados en {file_path} para la tabla {selected_table}.")
            messagebox.showinfo("Éxito", f"Datos guardados exitosamente en {selected_table}.")

            # Ejecutar el script de ingestión
            self.run_ingestion_script()
        except Exception as e:
            error_logger.error(f"Error al guardar datos en {file_path}: {e}")
            messagebox.showerror("Error", f"Error al guardar datos: {e}")

    def run_ingestion_script(self):
        try:
            # Asumiendo que el script ingest_data.py está en la carpeta scripts
            script_path = os.path.join(os.getcwd(), "scripts", "ingest_data.py")
            subprocess.check_call(['python', script_path])
            logging.info("Script de ingestión ejecutado correctamente.")
            messagebox.showinfo("Ingestión", "Ingestión de datos ejecutada exitosamente.")
        except subprocess.CalledProcessError as e:
            error_logger.error(f"Error al ejecutar el script de ingestión: {e}")
            messagebox.showerror("Ingestión", f"Error al ejecutar la ingestión de datos: {e}")
        except Exception as ex:
            error_logger.error(f"Error inesperado al ejecutar el script de ingestión: {ex}")
            messagebox.showerror("Ingestión", f"Error inesperado: {ex}")

    def select_config_file(self):
        file_path = filedialog.askopenfilename(filetypes=[("YAML files", "*.yaml")])
        if file_path:
            self.config_file_path.set(file_path)

    def reload_config(self):
        try:
            config_path = self.config_file_path.get()
            with open(config_path, 'r') as file:
                self.config = yaml.safe_load(file)
            self.table_combo['values'] = list(self.config['tables'].keys())
            messagebox.showinfo("Configuración", "Configuración recargada exitosamente.")
        except Exception as e:
            error_logger.error(f"Error al recargar la configuración: {e}")
            messagebox.showerror("Error", f"Error al recargar la configuración: {e}")

    def update_logs(self):
        try:
            with open(os.path.join(LOG_DIR, "ingest_gui.log"), 'r') as file:
                logs = file.read()
            self.log_text.delete(1.0, tk.END)
            self.log_text.insert(tk.END, logs)
        except Exception as e:
            error_logger.error(f"Error al actualizar los logs: {e}")
            messagebox.showerror("Error", f"Error al actualizar los logs: {e}")

def main():
    root = tk.Tk()
    gui = IngestGUI(root)
    root.mainloop()

if __name__ == "__main__":
    main()