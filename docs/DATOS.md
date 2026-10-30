# Inventario de datos locales

Snapshot del 30 de octubre de 2026: **17 CSV** en la carpeta de referencia [Dashboard/csv](../Dashboard/csv). Conteos obtenidos leyendo registros CSV con Python, sin mostrar filas ni consultar servicios externos.

| Archivo | Registros sin encabezado | Columnas |
|---|---:|---:|
| [common_player_info.csv](../Dashboard/csv/common_player_info.csv) | 3632 | 33 |
| [draft_combine_stats.csv](../Dashboard/csv/draft_combine_stats.csv) | 1633 | 47 |
| [draft_history.csv](../Dashboard/csv/draft_history.csv) | 8257 | 14 |
| [game.csv](../Dashboard/csv/game.csv) | 17121 | 55 |
| [game_info.csv](../Dashboard/csv/game_info.csv) | 58053 | 4 |
| [game_summary.csv](../Dashboard/csv/game_summary.csv) | 58110 | 14 |
| [inactive_players.csv](../Dashboard/csv/inactive_players.csv) | 110191 | 9 |
| [line_score.csv](../Dashboard/csv/line_score.csv) | 15591 | 43 |
| [nba_data_processed.csv](../Dashboard/csv/nba_data_processed.csv) | 624 | 29 |
| [nba_salaries.csv](../Dashboard/csv/nba_salaries.csv) | 446 | 8 |
| [officials.csv](../Dashboard/csv/officials.csv) | 70971 | 5 |
| [other_stats.csv](../Dashboard/csv/other_stats.csv) | 28271 | 26 |
| [player.csv](../Dashboard/csv/player.csv) | 4815 | 5 |
| [team.csv](../Dashboard/csv/team.csv) | 30 | 7 |
| [team_details.csv](../Dashboard/csv/team_details.csv) | 27 | 14 |
| [team_history.csv](../Dashboard/csv/team_history.csv) | 50 | 5 |
| [team_info_common.csv](../Dashboard/csv/team_info_common.csv) | 0 | 26 |

## Hallazgos

- `team_info_common.csv` tiene encabezados y cero registros.
- Las copias de `common_player_info.csv`, `draft_combine_stats.csv` y `draft_history.csv` de la raíz son idénticas por SHA-256 a las de [Dashboard/csv](../Dashboard/csv). Se conservaron sus ubicaciones; la carpeta del dashboard es la referencia para explorar.
- `play_by_play.csv` no está incluido en main. Puede aparecer en otras ramas o en una descarga externa, pero no se presupone disponible localmente.
- La presencia de una columna, el conteo de registros o un nombre de tabla no verifica exactitud, fechas de cobertura, limpieza ni relaciones entre conjuntos.

Para actualizar los conteos sin credenciales:

```sh
python scripts/inspeccionar_datos.py
python scripts/inspeccionar_datos.py --json
```

## Procedencia conocida y límites

Los notebooks de descarga identifican [NBA Database, de Wyatt Walsh en Kaggle](https://www.kaggle.com/datasets/wyattowalsh/basketball), mediante el identificador `wyattowalsh/basketball`. La descarga no fija una versión ni conserva un manifiesto que vincule cada archivo local a una versión de la fuente.

No quedó documentada la procedencia específica de `nba_salaries.csv` y `nba_data_processed.csv`. Sus nombres y columnas no bastan para atribuirles una fuente o licencia.

No se verificaron en esta revisión las condiciones de redistribución de cada conjunto. Antes de reutilizarlos en otro producto o publicar una entrega de datos, revisar las condiciones de la fuente y documentar transformaciones y versión. No se incorporaron datasets nuevos durante esta actualización.
