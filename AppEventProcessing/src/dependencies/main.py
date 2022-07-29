from pyspark.sql import SparkSession


spark = SparkSession.getOrCreate()

df = spark.read.csv(".dataset/connectivity_status.csv")