

from pyspark.sql import SparkSession
# from pyspark import SparkContext as sc

# sc.setLogLevel(logLevel="ERROR")
spark = SparkSession.builder.appName('1. Cleanup') .getOrCreate()

df = spark.read.csv('./data/DataSample.csv', header = True)

df.write.csv('mycsv.csv')

# df.printSchema()