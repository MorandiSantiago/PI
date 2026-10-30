# Datos de la NBA — proyecto colaborativo de formación

Proyecto de formación en análisis de datos desarrollado en equipo en 2025. Reúne datos de jugadores, equipos, partidos, draft y salarios, notebooks de carga a Firestore y un archivo de Power BI.

El objetivo original incluía explorar información para analizar equipos y traspasos. El repositorio conserva entregas de distintas etapas: **no constituye un pipeline único con actualización automática del dashboard verificada**.

> **Seguridad pendiente:** las claves retiradas de la versión actual no deben reutilizarse. No se pudo verificar su revocación en Google Cloud. El historial conserva versiones anteriores; consultar [SECURITY.md](./SECURITY.md).

## Qué se puede revisar

| Componente | Evidencia disponible | Alcance |
|---|---|---|
| Datos locales | 17 CSV en [Dashboard/csv](./Dashboard/csv) | Inventario reproducible sin conexión ni credenciales. Una tabla solo tiene encabezados. |
| Carga de datos | [index.ipynb](./index.ipynb) | Alternativas de descarga desde Kaggle y carga de listas de CSV locales a Firestore. Escriben en una base remota. |
| Monitor de archivos | [automatizacion.ipynb](./automatizacion.ipynb) | Reacciona a modificaciones de CSV y vuelve a cargarlos. Requiere ejecución continua y escribe en Firestore. |
| Consulta de ejemplo | [instrucciones.ipynb](./instrucciones.ipynb) | Configuración ADC y lectura limitada de la colección `player`. |
| Visualización | [Dashboard/ProyectoHenry.pbix](./Dashboard/ProyectoHenry.pbix) | Artefacto Power BI del equipo; apertura y actualización pendientes de validar en Desktop. |
| SQL Server y otros experimentos | Ramas históricas del equipo | No están integrados como una instalación reproducible de `main`. Ver [mapa técnico](./docs/ARQUITECTURA.md). |

**Tecnologías presentes en main:** Python, Pandas, Firebase Admin SDK/Firestore, kagglehub, watchdog, Jupyter y Power BI. La presencia de notebooks de ML o un borrador de IA en otra rama no acredita una integración de IA operativa.

## Primera prueba: explorar datos sin Firebase

Requisito: Python 3.10 o posterior. El inventario usa únicamente la biblioteca estándar.

```sh
git clone https://github.com/MorandiSantiago/PI.git
cd PI
python scripts/inspeccionar_datos.py
```

También funciona al invocarlo desde otra carpeta: resuelve el directorio de datos respecto de su propia ubicación.

```sh
python scripts/inspeccionar_datos.py --json
python scripts/inspeccionar_datos.py --carpeta Dashboard/csv
```

La salida muestra nombre, número de registros, cantidad de columnas y registros con un ancho distinto del encabezado. No imprime filas de jugadores, no descarga datos, no necesita paquetes externos y no escribe archivos.

Extracto verificado el 30 de octubre de 2026:

```text
Archivo                             Filas  Columnas  Otro ancho
game.csv                            17121        55           0
player.csv                           4815         5           0
team.csv                               30         7           0
team_info_common.csv                    0        26           0
Archivos: 17
```

El [inventario completo](./docs/DATOS.md) describe cantidades, duplicados y procedencia conocida. Estos conteos describen los archivos incluidos; no prueban calidad analítica, cobertura temporal ni actualidad.

## Preparar los notebooks: opcional y con efectos remotos

No hace falta instalar estas dependencias para ejecutar el inventario anterior.

El notebook principal registra Python 3.12.6 en sus metadatos. [requirements.txt](./requirements.txt) declara las dependencias directas; no conserva las versiones históricas ni un entorno congelado. La instalación de esas dependencias no se verificó en esta revisión.

Ejemplo en Windows, si se dispone de Python 3.12:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m jupyterlab
```

En Linux/macOS, crear el entorno con un Python compatible:

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m jupyterlab
```

Iniciar Jupyter desde la raíz de PI. Antes de ejecutar celdas que acceden a Firestore:

1. Usar un proyecto de pruebas propio y autenticación autorizada mediante ADC, siguiendo [SECURITY.md](./SECURITY.md).
2. Definir las variables en la terminal que iniciará Jupyter. [.env.example](./.env.example) es una referencia, no se carga automáticamente.
3. Para datos locales, usar `Dashboard/csv` o configurar `PI_CSV_DIR`. Las rutas relativas se resuelven desde la carpeta de trabajo del kernel.
4. En [index.ipynb](./index.ipynb), elegir **una alternativa de carga**. No ejecutar `Run All`: algunas celdas vuelven a cargar las mismas tablas. La variante local se divide en una celda que define la función y otra que usa su segunda lista.
5. Leer el [mapa de operaciones y limitaciones](./docs/ARQUITECTURA.md) antes de iniciar el monitor o una carga.

`play_by_play.csv` no está incluido en `main`. Se retiró de la selección local que lo solicitaba; las opciones de Kaggle dependen del contenido descargado. El monitor no instala tareas programadas ni actualiza Power BI.

## Dashboard

Ver [instrucciones y alcance del PBIX](./Dashboard/README.md). Su estructura interna contiene las páginas `General`, `Página 1` y `Traspasos`. Esto se verificó leyendo metadatos del archivo, sin abrir Power BI ni comprobar sus resultados.

## Organización y trabajo en equipo

- [docs/ARQUITECTURA.md](./docs/ARQUITECTURA.md): flujos presentes, operaciones y mapa de ramas.
- [docs/DATOS.md](./docs/DATOS.md): tablas disponibles y procedencia.
- [docs/VALIDACION.md](./docs/VALIDACION.md): comprobaciones realizadas y pendientes.
- [SECURITY.md](./SECURITY.md): credenciales y límites del retiro de claves.

Es un trabajo colaborativo. Las ramas con nombres de participantes permiten ubicar entregas, pero no bastan para atribuir toda su autoría a una persona. La revisión de octubre de 2026 incorpora documentación, inventario local y ajustes de configuración; no representa una nueva implementación de los componentes históricos de los demás integrantes.

## Límites actuales

- No se validó una ejecución completa Kaggle → Firestore → Power BI. No hay evidencia de ese flujo integrado en `main`.
- Los identificadores en Firestore se basan en el índice de fila: una nueva carga puede sobrescribir documentos y no elimina registros que desaparecen del CSV.
- No hay carga por lotes con reintentos, transacciones, control de costos ni un servicio desplegado del monitor.
- La fuente de datos externa no está fijada a una versión; las copias locales pueden diferir del dataset disponible hoy.
- Faltan capturas verificadas del dashboard y una descripción acordada del aporte individual antes de destacarlo como proyecto principal.
- La revocación de las claves publicadas anteriormente y cualquier limpieza del historial siguen siendo trabajos separados.

