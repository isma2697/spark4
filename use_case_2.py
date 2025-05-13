from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

spark = SparkSession.builder \
    .appName("Streaming de Ficheros") \
    .master("local[2]") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.sql.shuffle.partitions", 3) \
    .config("spark.sql.streaming.schemaInference", "true") \
    .getOrCreate()

raw_df = spark.readStream \
    .format("json") \
    .option("path", "entrada") \
    .option("maxFilesPerTrigger", 1) \
    .option("cleanSource", "delete") \
    .load()

explode_df = raw_df.selectExpr("InvoiceNumber", "CreatedTime", "StoreID", "PosID",
    "CustomerType", "PaymentMethod", "DeliveryType",
    "DeliveryAddress.City", "DeliveryAddress.State", "DeliveryAddress.PinCode",
    "explode(InvoiceLineItems) as LineItem")

limpio_df = explode_df \
    .withColumn("ItemCode", expr("LineItem.ItemCode")) \
    .withColumn("ItemDescription", expr("LineItem.ItemDescription")) \
    .withColumn("ItemPrice", expr("LineItem.ItemPrice")) \
    .withColumn("ItemQty", expr("LineItem.ItemQty")) \
    .withColumn("TotalValue", expr("LineItem.TotalValue")) \
    .drop("LineItem")

query = limpio_df.writeStream \
    .format("json") \
    .queryName("Facturas Writer") \
    .outputMode("append") \
    .option("path", "salida") \
    .option("checkpointLocation", "chk-point-dir-caso2") \
    .trigger(processingTime="1 minute") \
    .start()

query.awaitTermination()
