from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as Fsum
from pyspark.sql.types import StructType, StringType, IntegerType

spark = SparkSession.builder.appName("SparkKafkaStreaming").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType() \
    .add("codigo", StringType()) \
    .add("descripcion", StringType()) \
    .add("cantidad", IntegerType())

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "infracciones_data") \
    .load()

parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

aggregated = parsed_df.groupBy("descripcion").agg(Fsum("cantidad").alias("total_infracciones"))

query = aggregated.writeStream.outputMode("complete").format("console").start()

print("Spark UI activa en: http://192.168.1.109:4040")
query.awaitTermination()
