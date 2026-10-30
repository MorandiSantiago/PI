"""Inventario local de CSV: solo lectura, sin dependencias externas ni red."""

import argparse
import csv
import json
from pathlib import Path


DEFAULT_DIRECTORY = Path(__file__).resolve().parents[1] / "Dashboard" / "csv"


def inspeccionar(directorio):
    """Devuelve estructura y cantidades; no imprime registros de jugadores."""
    directorio = Path(directorio)
    if not directorio.is_dir():
        raise ValueError("No existe la carpeta CSV indicada.")
    archivos = sorted(directorio.glob("*.csv"))
    if not archivos:
        raise ValueError("La carpeta indicada no contiene archivos CSV.")
    tablas = []
    for archivo in archivos:
        with archivo.open(encoding="utf-8-sig", newline="") as entrada:
            lector = csv.reader(entrada, strict=True)
            encabezados = next(lector, None)
            if not encabezados:
                raise ValueError(f"{archivo.name}: falta el encabezado.")
            filas = 0
            inconsistentes = 0
            for fila in lector:
                filas += 1
                if len(fila) != len(encabezados):
                    inconsistentes += 1
        tablas.append({
            "archivo": archivo.name,
            "filas": filas,
            "columnas": len(encabezados),
            "filas_con_otro_ancho": inconsistentes,
            "encabezados": encabezados,
        })
    return tablas


def main():
    parser = argparse.ArgumentParser(
        description="Explorar los CSV de PI sin Firebase, credenciales ni escrituras."
    )
    parser.add_argument(
        "--carpeta", type=Path, default=DEFAULT_DIRECTORY,
        help="Carpeta de CSV; por defecto Dashboard/csv del repositorio.",
    )
    parser.add_argument("--json", action="store_true", help="Mostrar el inventario en JSON.")
    argumentos = parser.parse_args()
    try:
        tablas = inspeccionar(argumentos.carpeta)
    except (OSError, UnicodeError, csv.Error, ValueError) as error:
        parser.error(str(error))
    if argumentos.json:
        print(json.dumps({"archivos": len(tablas), "tablas": tablas}, ensure_ascii=False, indent=2))
    else:
        print("Inventario CSV local (solo lectura)")
        print(f"{'Archivo':<30} {'Filas':>10} {'Columnas':>9} {'Otro ancho':>11}")
        for tabla in tablas:
            print(
                f"{tabla['archivo']:<30} {tabla['filas']:>10} "
                f"{tabla['columnas']:>9} {tabla['filas_con_otro_ancho']:>11}"
            )
        print(f"Archivos: {len(tablas)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
