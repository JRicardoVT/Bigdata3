from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, sum as Fsum, round

spark = SparkSession.builder.appName("EDA_Infracciones").getOrCreate()

data_path = "/home/vboxuser/bigdata_proyecto/datasets/processed_infracciones"
df = spark.read.option("header", True).option("inferSchema", True).csv(data_path)

print("Vista general de datos:")
df.show(5)

total = df.agg(Fsum("cantidad").alias("total")).collect()[0]["total"]

top10 = df.groupBy("descripcion").agg(Fsum("cantidad").alias("total_infracciones")) \
    .orderBy(desc("total_infracciones")).limit(10)

print("Top 10 Infracciones más comunes:")
top10.show(truncate=False)

top10.write.mode("overwrite").option("header", True).csv("/home/vboxuser/bigdata_proyecto/output/top10_infracciones")
print("Resultados guardados en /output/top10_infracciones")

spark.stop()
