from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("test_spark_cluster")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

df = spark.range(0, 1000000).repartition(4)

print("Nombre de partitions :", df.rdd.getNumPartitions())
print("Nombre de lignes :", df.count())

spark.stop()