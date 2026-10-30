# Credenciales y uso seguro de Firebase

## Credenciales externas al repositorio

Los notebooks usan **Application Default Credentials (ADC)** del SDK de Google.
No necesitan un archivo de credenciales en la carpeta del proyecto.

En un entorno de Google Cloud con identidad asociada, ADC puede obtener las
credenciales de ese entorno. Para desarrollo local con un JSON autorizado:

1. Usar una cuenta de servicio de un proyecto de pruebas, con los permisos mínimos
   necesarios. Las operaciones de carga de estos notebooks escriben en Firestore.
2. Guardar el JSON en una carpeta privada fuera del repositorio y de carpetas
   compartidas. No reutilizar claves que hayan sido expuestas.
3. Definir la ruta antes de iniciar Jupyter. Ejemplo para PowerShell:

   ```powershell
   $env:GOOGLE_APPLICATION_CREDENTIALS = 'C:/Users/TU_USUARIO/.config/pi/firebase-service-account.json'
   $env:GOOGLE_CLOUD_PROJECT = 'tu-proyecto-de-pruebas'
   ```

   Ejemplo para Bash:

   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS="$HOME/.config/pi/firebase-service-account.json"
   export GOOGLE_CLOUD_PROJECT='tu-proyecto-de-pruebas'
   ```

4. Iniciar Jupyter desde esa misma terminal. El archivo [.env.example](./.env.example)
   es una referencia; este proyecto no carga archivos `.env` automáticamente.
5. Revisar el proyecto de destino antes de ejecutar una celda. Reiniciar el kernel
   si se cambia de cuenta o de proyecto para no reutilizar una aplicación Firebase
   inicializada anteriormente.

El SDK resuelve ADC con `credentials.ApplicationDefault()`. Las celdas reutilizan
la aplicación predeterminada si ya existe. La carga local y el monitor usan
`PI_CSV_DIR` o `Dashboard/csv` desde el directorio de trabajo del kernel. Iniciar
Jupyter desde la raíz del repositorio. Las dependencias directas y sus límites
se describen en [README.md](./README.md) y [requirements.txt](./requirements.txt).

Si se usa otro mecanismo ADC, no hace falta definir una ruta a un JSON. Nunca
copiar credenciales al código, a los notebooks, a sus salidas o a los ejemplos.

## Claves expuestas e historial

Se retiraron dos archivos de credenciales de la versión actual del repositorio.
Esta eliminación **no revoca las claves en Google Cloud ni las borra de commits
anteriores, otras referencias, clones o copias**.

Al 30 de octubre de 2026 no se pudo verificar la revocación en el proveedor.
La revisión documental no cambia ese estado ni autoriza a reutilizar las claves.

El administrador del proyecto de Google Cloud debe:

1. Identificar y revocar las claves expuestas en IAM. Si existen consumidores
   legítimos, actualizar su autenticación de manera controlada.
2. Revisar actividad de la cuenta, permisos, recursos y consumo ante posibles usos
   inesperados. La publicación de una clave no prueba por sí sola que se haya usado.
3. Considerar las credenciales temporales ya emitidas al evaluar la respuesta:
   eliminar una clave no invalida automáticamente esos tokens.
4. Coordinar cualquier limpieza de historial con los colaboradores. Esa operación
   cambia referencias y se trata por separado del retiro de archivos actuales.

## Antes de compartir cambios

- Revisar los archivos preparados con `git diff --cached --name-only`.
- Comprobar que no se incluyan JSON reales de cuentas de servicio, archivos `.env`,
  claves privadas ni tokens, aunque tengan otro nombre.
- Limpiar salidas y contadores de ejecución de notebooks antes de compartirlos.
- Usar datos y configuraciones ficticias en los ejemplos.

Las reglas de [.gitignore](./.gitignore) son una prevención básica; no detectan
todos los secretos ni eliminan archivos que ya estén versionados.

## Referencias

- [Configuración del SDK Admin de Firebase y ADC](https://firebase.google.com/docs/admin/setup).
- [Gestión segura de claves de cuentas de servicio](https://docs.cloud.google.com/iam/docs/best-practices-for-managing-service-account-keys).
- [Respuesta a credenciales comprometidas](https://docs.cloud.google.com/docs/security/compromised-credentials).
- [Retiro de información sensible en GitHub](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/removing-sensitive-data-from-a-repository).
