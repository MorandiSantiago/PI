# Dashboard del proyecto NBA

[ProyectoHenry.pbix](./ProyectoHenry.pbix) es el artefacto de Power BI conservado en main. Forma parte de la entrega colaborativa de formación de 2025.

## Qué se verificó

La lectura del componente interno `Report/Layout` identificó estas páginas y contenedores visuales:

| Página | Contenedores visuales |
|---|---:|
| General | 12 |
| Página 1 | 4 |
| Traspasos | 12 |

Es una inspección de metadatos del PBIX. No se abrió Power BI Desktop, no se renderizaron las páginas ni se validaron medidas, filtros, conexiones o resultados. Los contenedores pueden incluir títulos e imágenes; su cantidad no equivale a gráficos analíticos funcionales.

## Cómo revisar el artefacto

1. Abrir una copia local del PBIX con Power BI Desktop.
2. Revisar los orígenes y consultas antes de actualizar. Si dependen de rutas locales anteriores, ajustarlas a la ubicación de los CSV disponibles.
3. Usar [csv](./csv) como referencia de archivos incluidos, sin asumir que todos forman parte del modelo ni que reemplazan los orígenes configurados.
4. Comprobar consultas, relaciones, medidas, filtros y páginas. Registrar errores o requisitos faltantes.
5. Exportar capturas o un PDF únicamente después de comprobar que no incluyan datos privados, rutas personales o información de cuentas.

La actualización automática y la conexión con Firestore no están verificadas. La presencia del PBIX no demuestra un reporte publicado en Power BI Service.

Volver al [README principal](../README.md) o consultar el [inventario de datos](../docs/DATOS.md).
