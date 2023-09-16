from pyspark.sql import SparkSession

spark = SparkSession.Builder().appName('test').getOrCreate()

dataframe = spark.read.csv('dataframe_instituciones.csv', header=True, inferSchema=True)

dataframe.printSchema()


dataframe.filter()