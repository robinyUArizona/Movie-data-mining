

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("P4Trends").getOrCreate()



movies1M = spark.read.option("delimiter", "::").csv("movies-data/ml-1m/movies.dat") \
    .withColumnRenamed("_c0", "movieID") \
    .withColumnRenamed("_c1", "MovieName") \
    .withColumnRenamed("_c2", "Genre")

ratings1M = spark.read.option("delimiter", "::").csv("movies-data/ml-1m/ratings.dat")\
    .withColumnRenamed("_c0", "userID") \
    .withColumnRenamed("_c1", "movieID") \
    .withColumnRenamed("_c2", "Rating") \
    .withColumnRenamed("_c3", "Timestamp")

users1M = spark.read.option("delimiter", "::").csv("movies-data/ml-1m/users.dat")\
    .withColumnRenamed("_c0", "userID") \
    .withColumnRenamed("_c1", "Gender") \
    .withColumnRenamed("_c2", "Age") \
    .withColumnRenamed("_c3", "Occupation") \
    .withColumnRenamed("_c4", "Zip-Code")



