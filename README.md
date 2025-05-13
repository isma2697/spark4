# Apache Spark Bitcoin Price Analysis Practice

Este proyecto es una solución para una práctica de Apache Spark que implica la recopilación de precios de Bitcoin en tiempo real y su posterior procesamiento y análisis utilizando PySpark.

## Objetivos del Proyecto

1.  **Casos de Uso 1, 2 y 3:** Completar los ejercicios especificados en [aitor-medrano.github.io/iabd/spark/streaming.html](https://aitor-medrano.github.io/iabd/spark/streaming.html) (los scripts para estos casos de uso deben incluirse por separado si forman parte de la entrega, por ejemplo, como `use_cases_1_2_3.py`).
2.  **Recopilación de Datos de Bitcoin (`bitcoin_price.py`):**
    *   Crear un programa Python que consulte el precio de Bitcoin cada 10 segundos utilizando la API de CoinGecko.
    *   Escribir los datos recopilados en un fichero JSON (`bitcoin_price.json`) con el formato:
        ```json
        {"Timestamp": "YYYY-MM-DD HH:MM:SS", "Behaviour": "Up/Down/Same", "Price": XXXXX}
        ```
3.  **Transformación de Datos con Spark (`process_bitcoin_data.py`):**
    *   Utilizar Apache Spark para leer el fichero `bitcoin_price.json`.
    *   Transformar los datos para generar un resumen agregado en ventanas de tiempo, mostrando:
        ```
        | StartTime | EndTime | Average price | #Ups | #Downs |
        |-----------|---------|---------------|------|--------|
        | HH:mm     | HH:mm   | X.XX          | Y    | Z      |
        ...
        ```

## Estructura de Ficheros

    

IGNORE_WHEN_COPYING_START
Use code with caution. Markdown
IGNORE_WHEN_COPYING_END

.
├── bitcoin_price.py # Script Python para obtener precios de Bitcoin
├── process_bitcoin_data.py # Script PySpark para analizar los precios
├── commands.txt # Fichero con los comandos para ejecutar los scripts
├── README.md # Este fichero
└── (otros scripts como use_cases_1_2_3.py si aplica)

      
## Prerrequisitos

*   Python 3.7+
*   Apache Spark (probado con Spark 3.x)
*   `pip` (gestor de paquetes de Python)
*   Librería Python `requests`

## Configuración

1.  **Clona el repositorio (si aplica):**
    ```bash
    git clone <tu-repositorio-url>
    cd <nombre-del-repositorio>
    ```

2.  **(Recomendado) Crea y activa un entorno virtual de Python:**
    ```bash
    python -m venv venv
    # En Windows
    venv\Scripts\activate
    # En macOS/Linux
    source venv/bin/activate
    ```

3.  **Instala las dependencias de Python:**
    ```bash
    pip install requests
    ```
    (Si tuvieras un `requirements.txt`, podrías usar `pip install -r requirements.txt`)

## Cómo Ejecutar

Sigue estos pasos en orden:

1.  **Generar Datos de Precios de Bitcoin:**
    Ejecuta el script `bitcoin_price.py`. Este script comenzará a obtener precios de Bitcoin cada 10 segundos y los guardará en `bitcoin_price.json`.
    ```bash
    python bitcoin_price.py
    ```
    Deja este script ejecutándose durante unos minutos (por ejemplo, 2-5 minutos) para recopilar suficientes datos. Luego, **detenlo presionando `CTRL+C`** en la terminal.

    *Nota: Cada vez que ejecutes este script, añadirá datos al final de `bitcoin_price.json` si ya existe. Para una prueba limpia, considera borrar o renombrar `bitcoin_price.json` antes de ejecutarlo.*

2.  **Procesar Datos con Apache Spark:**
    Una vez que tengas datos en `bitcoin_price.json`, ejecuta el script de PySpark `process_bitcoin_data.py` usando `spark-submit`.
    ```bash
    spark-submit process_bitcoin_data.py
    ```
    Si `spark-submit` no está en tu PATH, puede que necesites usar la ruta completa a `spark-submit` (ej. `$SPARK_HOME/bin/spark-submit`).

    El script procesará los datos y mostrará una tabla con los precios promedio y los conteos de subidas/bajadas por ventanas de tiempo. La duración de la ventana por defecto es de "10 minutos" en `process_bitcoin_data.py` y puede ser ajustada.

## Comandos Resumidos

Un resumen de los comandos se encuentra también en `commands.txt`.

```bash
# (Opcional) Activar entorno virtual
# source venv/bin/activate # o venv\Scripts\activate

# 1. Instalar dependencias (si es la primera vez o en un nuevo entorno)
pip install requests

# 2. Generar datos (dejar correr por unos minutos y luego detener con CTRL+C)
python bitcoin_price.py

# 3. Procesar datos con Spark
spark-submit process_bitcoin_data.py

    

IGNORE_WHEN_COPYING_START
Use code with caution.
IGNORE_WHEN_COPYING_END
Notas Adicionales

    El script bitcoin_price.py utiliza la API pública de CoinGecko, que no requiere clave para consultas simples de precios.

    El script process_bitcoin_data.py utiliza ventanas de tiempo móviles (tumbling windows) de duración fija para la agregación. El ejemplo de salida en el enunciado de la práctica sugiere ventanas de duración variable, lo cual es más complejo de implementar directamente con las funciones de ventana estándar de Spark. Esta solución utiliza ventanas de duración fija (ej. 10 minutos) para simplicidad y uso estándar de Spark.

    Asegúrate de que Apache Spark esté correctamente instalado y configurado en tu sistema.

Este proyecto fue desarrollado como parte de la asignatura [Nombre de la asignatura/curso, si aplica].

      
**Personalización:**

*   **`<tu-repositorio-url>` y `<nombre-del-repositorio>`:** Si estás subiendo esto a GitHub, reemplaza estos placeholders.
*   **`use_cases_1_2_3.py`:** Si los casos de uso 1, 2 y 3 son un script separado, menciónalo o ajústalo según sea necesario.
*   **Nombre de la asignatura/curso:** Puedes añadirlo al final si es relevante.
*   **Ventanas de tiempo:** La nota sobre las ventanas de tiempo es importante para aclarar cualquier diferencia entre la salida del script y el ejemplo exacto del enunciado si este implicaba ventanas irregulares.

Este README debería dar una buena visión general de tu proyecto y cómo ejecutarlo.

    