from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("BatchProcessingInfracciones").getOrCreate()

data_path = "/home/vboxuser/bigdata_proyecto/datasets/infracciones_sincelejo.csv"
df = spark.read.option("header", True).option("inferSchema", True).csv(data_path)

print("Datos cargados correctamente")
df.show(5)

df_clean = df.na.drop() \
    .withColumnRenamed("c_digo_de_la_infracci_n", "codigo") \
    .withColumnRenamed("descripci_n_de_la_infracci", "descripcion") \
    .withColumnRenamed("cantidad_de_infracciones", "cantidad") \
    .withColumn("cantidad", col("cantidad").cast("int"))

print("Datos transformados")
df_clean.printSchema()

output_path = "/home/vboxuser/bigdata_proyecto/datasets/processed_infracciones"
df_clean.write.mode("overwrite").option("header", True).csv(output_path)

print(f"Datos procesados guardados en: {output_path}")
spark.stop()
