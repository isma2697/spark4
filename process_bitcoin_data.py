# process_bitcoin_data.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, window, avg, sum as spark_sum, when, date_format, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

def main():
    spark = SparkSession.builder.appName("BitcoinPriceAnalysis").getOrCreate()

    # Definir el esquema para asegurar la correcta inferencia de tipos
    schema = StructType([
        StructField("Timestamp", StringType(), True),
        StructField("Behaviour", StringType(), True),
        StructField("Price", IntegerType(), True) # O DoubleType() si el precio puede tener decimales
    ])

    # Leer el fichero JSON. Spark espera un JSON por línea.
    # Asegúrate de que bitcoin_price.py escribe un JSON por línea.
    try:
        raw_df = spark.read.schema(schema).json("bitcoin_price.json")
    except Exception as e:
        print(f"Error al leer el archivo bitcoin_price.json: {e}")
        print("Asegúrate de que el archivo existe y contiene datos JSON válidos, uno por línea.")
        print("Ejecuta primero el script bitcoin_price.py para generar datos.")
        spark.stop()
        return

    # Convertir la columna Timestamp de String a TimestampType
    df_with_timestamp = raw_df.withColumn("event_time", to_timestamp(col("Timestamp"), "yyyy-MM-dd HH:mm:ss"))

    # ---- Agregación con Ventanas de Tiempo ----
    # El ejemplo de salida tiene ventanas irregulares.
    # Spark es mejor con ventanas regulares (tumbling/sliding).
    # Vamos a usar una ventana de ejemplo de 10 minutos. Puedes cambiarla.
    # Ejemplos: "10 minutes", "30 minutes", "1 hour"
    window_duration = "10 minutes" # Puedes cambiar esto a "30 minutes" etc.
    # slide_duration = "5 minutes" # Para ventanas deslizantes, si no se especifica, es igual a window_duration (tumbling)

    print(f"Procesando con ventanas de {window_duration}...")

    aggregated_df = df_with_timestamp.groupBy(
        window(col("event_time"), window_duration)
    ).agg(
        avg("Price").alias("Average_price"),
        spark_sum(when(col("Behaviour") == "Up", 1).otherwise(0)).alias("#Ups"),
        spark_sum(when(col("Behaviour") == "Down", 1).otherwise(0)).alias("#Downs")
        # Si quieres contar "Same" también:
        # spark_sum(when(col("Behaviour") == "Same", 1).otherwise(0)).alias("#Sames")
    ).orderBy("window.start")

    # Formatear la salida para que coincida con el ejemplo
    # La columna 'window' contiene 'start' y 'end'
    result_df = aggregated_df.select(
        date_format(col("window.start"), "HH:mm").alias("StartTime"),
        date_format(col("window.end"), "HH:mm").alias("EndTime"),
        col("Average_price"),
        col("#Ups"),
        col("#Downs")
    )

    print("Resultado de la agregación:")
    result_df.show(truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()