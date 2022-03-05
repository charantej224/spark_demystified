import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[1]") \
    .appName("SparkByExamples.com") \
    .getOrCreate()
df = spark.range(500).toDF("number").filter("number < 600").withColumn("mod_val", F.col("number") % 100)
df = df.select(F.expr("mod_val as modulas"),"number")
df.show(3)
df = df.select(F.col("modulas").alias("mod_val"),F.col("number"))
df.show(4)

df.select(F.expr("*"),F.lit("One").alias("new_column")).show()