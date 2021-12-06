
# Reference: https://medium.com/analytics-vidhya/movie-lens-data-analysis-using-pyspark-for-beginners-9c0f5f21eaf5
# Reference: https://towardsdatascience.com/data-science-over-the-movies-dataset-with-spark-scala-and-some-sql-and-some-python-part-1-f5fd4ee8509e
# Reference: https://www.ijrte.org/wp-content/uploads/papers/v7i5s2/ES2097017519.pdf
# Reference: 

import time
import datetime
from pyspark import sql
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, BooleanType, DateType, StringType
from pyspark.sql.functions import lit, unix_timestamp
from pyspark.sql.functions import year, month, dayofmonth


spark = SparkSession.builder.appName("MoviesDataMining").getOrCreate()


movies_file_location = "movies-data/ml-1m/movies.dat"
ratings_file_location = "movies-data/ml-1m/ratings.dat"
users_file_location = "movies-data/ml-1m/users.dat"

movies1M = spark.read.option("delimiter", "::").csv(movies_file_location) \
    .withColumnRenamed("_c0", "movieID") \
    .withColumnRenamed("_c1", "MovieName") \
    .withColumnRenamed("_c2", "Genre")

movies1M = movies1M.withColumn("movieID", movies1M.movieID.cast(IntegerType()))
# movies1M.show(5)
# movies1M.printSchema()


ratings1M = spark.read.option("delimiter", "::").csv(ratings_file_location)\
    .withColumnRenamed("_c0", "userID") \
    .withColumnRenamed("_c1", "movieID") \
    .withColumnRenamed("_c2", "Rating") \
    .withColumnRenamed("_c3", "Timestamp")



timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

ratings1M = ratings1M.withColumn("userID", col("userID").cast(IntegerType())) \
        .withColumn("movieID", col("movieID").cast(IntegerType())) \
        .withColumn("Rating", col("Rating").cast(IntegerType())) \
        .withColumn("Timestamp", unix_timestamp(lit(timestamp),'yyyy-MM-dd HH:mm:ss').cast("timestamp"))

# ratings1M.show(5)
# ratings1M.printSchema()


users1M = spark.read.option("delimiter", "::").csv(users_file_location)\
    .withColumnRenamed("_c0", "userID") \
    .withColumnRenamed("_c1", "Gender") \
    .withColumnRenamed("_c2", "Age") \
    .withColumnRenamed("_c3", "Occupation") \
    .withColumnRenamed("_c4", "Zip-Code")

users1M = users1M.withColumn("userID", col("userID").cast(IntegerType())) \
        .withColumn("Gender", col("Gender").cast(StringType())) \
        .withColumn("Age", col("Age").cast(IntegerType())) \
        .withColumn("Occupation", col("Occupation").cast(StringType())) \
        .withColumn("Zip-Code", col("Zip-Code").cast(StringType()))
# users1M.show(5)
# users1M.printSchema()




## Join movies and ratings, key is movieID
movies_ratings = movies1M.join(ratings1M, movies1M.movieID == ratings1M.movieID, "outer") \
    .drop(ratings1M.movieID)
# movies_ratings.show(truncate=False)


## Join ratings and users, key is userID
users_ratings = users1M.join(ratings1M, users1M.userID == ratings1M.userID, "outer") \
    .drop(ratings1M.userID)
# users_ratings.show(truncate=False)


# What are k most popular movies of all time?
movies_ratings_avg = movies_ratings.groupBy(['movieID', 'MovieName'])\
    .avg('Rating')\
    .sort(col("avg(Rating)") \
    .desc())
print("==== k most popular movies of all time ====")
# movies_ratings_avg.show()


# What are k most popular movies for a particular year?
movies_ratings = movies_ratings.withColumn("Year", year(col("Timestamp")))

print("==== k most popular movies for a particular year ====")
movies_ratings_year = movies_ratings.groupBy(['Year', 'movieID', 'MovieName']) \
    .avg('Rating')\
    .sort(col("avg(Rating)") \
    .desc())

# movies_ratings_year.show()

movies_ratings_year_2021 = movies_ratings_year[movies_ratings_year["Year"] == 2021]
# movies_ratings_year_2021.show()


# What are k most popular movies for a certain age group?
users_ratings_movies = users_ratings.join(movies1M, users_ratings.movieID == movies1M.movieID, \
    "outer").drop(movies1M.movieID)
# users_ratings_movies.show()

print("==== k most popular movies for a certain age group ====")
movies_ratings_age = users_ratings_movies.groupBy(['Age', 'movieID', 'MovieName']) \
    .avg('Rating')\
    .sort(col("avg(Rating)") \
    .desc())

# movies_ratings_age.show()

movies_ratings_age_25 = movies_ratings_age[movies_ratings_age["Age"] == 25]
# movies_ratings_age_25.show()


# What are k most popular movies for particular season (summer, fall, winter, spring)?

movies_ratings = movies_ratings.withColumn("Month", month(col("Timestamp")))
# movies_ratings.show()

import calendar
from pyspark.sql import functions as F
month_name = F.udf(lambda x: calendar.month_name[int(x)])

movies_ratings = movies_ratings.withColumn("Month_Name", month_name(F.col("Month")))
# movies_ratings.show()



print("==== k most popular movies for a particular month - season ====")




# What are the top k movies with the most ratings (presumably most popular) that have the lowest ratings?



