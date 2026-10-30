# Validación de la revisión de octubre de 2026

Fecha: 30 de octubre de 2026. Entorno local: Windows y Python 3.15.2.

## Comprobaciones realizadas

| Comprobación | Resultado |
|---|---|
| Inventario local desde fuera de la raíz de PI | 17 CSV leídos; resolución de ruta independiente del directorio actual. |
| Salida JSON del inventario | JSON válido con 17 tablas. |
| Cantidad de campos por registro | Ninguna fila con ancho distinto de su encabezado en esos 17 archivos. No es una validación integral de calidad de datos. |
| Tabla sin registros | `team_info_common.csv`: 0 filas de datos y 26 columnas. |
| Carpeta inexistente | El comando termina con código 2 y un mensaje de error. |
| Selección de carga local del notebook | Los 12 archivos solicitados existen en `Dashboard/csv`. |
| Lista local incompleta, función aislada | Rechaza la lista antes de leer DataFrames o acceder a Firestore. Se ejecutó solo la función extraída, sin imports del notebook ni servicios externos. |
| Sintaxis de notebooks | 7 celdas Python analizadas con AST sin errores. La celda mágica `%pip` se revisó como instrucción, sin ejecutarla. |
| Salidas de notebooks | Sin salidas guardadas ni contadores de ejecución. |
| Copias CSV de la raíz | Tres pares idénticos por SHA-256 respecto de la carpeta del dashboard. |
| PBIX | ZIP legible; `Report/Layout` contiene 3 páginas. Solo se inspeccionaron metadatos. |

Comandos del inventario:

```sh
python scripts/inspeccionar_datos.py
python scripts/inspeccionar_datos.py --json
```

La inspección de sintaxis no ejecuta los notebooks ni importa sus dependencias. El inventario usa únicamente módulos estándar de Python y no llama a Firebase, Kaggle ni otro servicio.

## Qué cambió

- Se documentaron el alcance real, la estructura de datos, las ramas del equipo y el artefacto Power BI.
- Se agregó una herramienta de inventario local de solo lectura y un manifiesto de dependencias directas.
- Se reemplazaron rutas personales por `PI_CSV_DIR` o la carpeta local `Dashboard/csv`.
- La carga local comprueba que existan todos los archivos de su lista antes de escribir esa lista. No implica validación transaccional del contenido.
- Se retiró `play_by_play.csv` de la selección local porque no está incluido en main.
- Se corrigió la consulta de ejemplo para usar `player` y limitarla a cinco documentos.
- Se retiraron instrucciones que dependían de un archivo `monitor_csv.py` inexistente.
- No se movieron CSV, no se fusionaron ramas ni se reescribió el historial.

## Pendientes funcionales

No se instalaron dependencias, descargaron datasets ni ejecutaron notebooks. No se utilizaron credenciales, no se consultó ni modificó Firestore y no se inició el monitor.

Queda pendiente validar un entorno de notebooks con dependencias versionadas, un ensayo pequeño de carga/lectura en un proyecto autorizado, los eventos del monitor y la apertura/actualización del PBIX en Power BI Desktop. Tampoco se verificó un vínculo automático entre Firestore y el dashboard.

La falta de una comprobación actual no invalida el trabajo histórico, pero no permite presentarlo como una instalación reproducida en esta revisión.

## Seguridad pendiente

Los archivos de claves se retiraron de la versión actual en una etapa anterior. No se verificó la revocación en Google Cloud y el historial conserva versiones anteriores. Ver [SECURITY.md](../SECURITY.md).
