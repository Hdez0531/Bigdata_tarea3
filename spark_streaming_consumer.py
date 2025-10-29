from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("KafkaSparkPromedios") \
    .master("local[*]") \
    .getOrCreate()

# Definir el esquema del JSON recibido
schema = StructType([
    StructField("temperatura", DoubleType()),
    StructField("humedad", DoubleType()),
    StructField("timestamp", StringType())
])

# Leer datos desde Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor_data") \
    .load()

# Extraer el campo 'value' y convertir a JSON
data = df.selectExpr("CAST(value AS STRING)") \
          .select(from_json(col("value"), schema).alias("data")) \
          .select("data.*")

# Convertir el timestamp a tipo fecha-hora reconocible
data = data.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))

# Calcular promedios en ventanas de 10 segundos con watermark de 30 s
promedios = data \
    .withWatermark("timestamp", "30 seconds") \
    .groupBy(
        window(col("timestamp"), "10 seconds")
    ) \
    .agg(
        avg("temperatura").alias("promedio_temp"),
        avg("humedad").alias("promedio_hum")
    ) \
    .select(
        col("window.start").alias("inicio_ventana"),
        col("window.end").alias("fin_ventana"),
        col("promedio_temp"),
        col("promedio_hum")
    )

# Mostrar resultados en consola
console_query = promedios.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

# Guardar resultados en CSV
csv_query = promedios.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "/home/bigdatavm/spark_output_promedios") \
    .option("checkpointLocation", "/home/bigdatavm/spark_output_promedios/checkpoint") \
    .option("truncate", False) \
    .start()

# Mantener ambos flujos activos
console_query.awaitTermination()
csv_query.awaitTermination()
