

import time
import datetime
from pyspark import sql
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.sql import types as t
from pyspark.sql.types import DoubleType, IntegerType, BooleanType, DateType, StringType
from pyspark.sql.functions import lit, unix_timestamp
from pyspark.sql.functions import year, month, dayofmonth


spark = SparkSession.builder.appName("MoviesDataMining").getOrCreate()


movies_file_location = "movies-data/ml-1m/movies.dat"
ratings_file_location = "movies-data/ml-1m/ratings.dat"
users_file_location = "movies-data/ml-1m/users.dat"

# Reading movies spark dataframe
movies1M = spark.read.option("delimiter", "::").csv(movies_file_location) \
    .withColumnRenamed("_c0", "movieID") \
    .withColumnRenamed("_c1", "MovieName") \
    .withColumnRenamed("_c2", "Genre")
movies1M = movies1M.withColumn("movieID", movies1M.movieID.cast(IntegerType()))

# Reading ratings spark dataframe
ratings1M = spark.read.option("delimiter", "::").csv(ratings_file_location)\
    .withColumnRenamed("_c0", "userID") \
    .withColumnRenamed("_c1", "movieID") \
    .withColumnRenamed("_c2", "Rating") \
    .withColumnRenamed("_c3", "Timestamp")

ratings1M = ratings1M.withColumn("userID", col("userID").cast(IntegerType())) \
        .withColumn("movieID", col("movieID").cast(IntegerType())) \
        .withColumn("Rating", col("Rating").cast(IntegerType())) \
        .withColumn("Timestamp", col("Timestamp").cast(DoubleType()))
ratings1M = ratings1M.withColumn('epoch', F.date_format(ratings1M.Timestamp.cast(dataType=t.TimestampType()), "yyyy-MM-dd"))


# Reading users spark dataframe
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

## Join movies and ratings, key is movieID
movies_ratings = movies1M.join(ratings1M, movies1M.movieID == ratings1M.movieID, "outer") \
    .drop(ratings1M.movieID)

## Join ratings and users, key is userID
users_ratings = users1M.join(ratings1M, users1M.userID == ratings1M.userID, "outer") \
    .drop(ratings1M.userID)


# Question 1. k highest average rated movies of all time
print("==== k highest average rated movies of all time ====")
movies_ratings_avg = movies_ratings.groupBy(['movieID', 'MovieName'])\
    .avg('Rating')\
    .sort(col("avg(Rating)") \
    .desc())
movies_ratings_avg.show(10)

# Question 2. k highest averaged rated movies for a particular year
print("==== k highest averaged rated movies for a particular year ====")
movies_ratings = movies_ratings.withColumn("Year", year(col("epoch")))
movies_ratings_year = movies_ratings.groupBy(['Year', 'movieID', 'MovieName']) \
    .avg('Rating')\
    .sort(col("avg(Rating)") \
    .desc())

movies_ratings_year_2000 = movies_ratings_year[movies_ratings_year["Year"] == 2000]
movies_ratings_year_2000.show(10)


# Question 3. k highest averaged rated movies for a certain age group
print("==== k highest averaged rated movies for a certain age group ====")
users_ratings_movies = users_ratings.join(movies1M, users_ratings.movieID == movies1M.movieID, \
    "outer").drop(movies1M.movieID)

movies_ratings_age = users_ratings_movies.groupBy(['Age', 'movieID', 'MovieName']) \
    .avg('Rating')\
    .sort(col("avg(Rating)") \
    .desc())

movies_ratings_age_25 = movies_ratings_age[movies_ratings_age["Age"] == 25]
movies_ratings_age_25.show(10)


# Question 4. Age group that is most likely to post a review for a movie, for Males/Females?
users_ratings_age = users_ratings.groupBy(['Age', 'Gender']) \
    .count() \
    .sort(col('count') \
    .desc())
users_ratings_age = users_ratings_age.withColumn(
    'Rating_ratio', users_ratings_age['count'] / users_ratings.count())
users_ratings_age = users_ratings_age.withColumn('Percentage', F.round(users_ratings_age['Rating_ratio'], 2))
users_ratings_age.show(10)


# Question 5. k highest averaged rated movies for a particular month - season
print("==== k highest averaged rated movies for a particular month - season ====")
movies_ratings = movies_ratings.withColumn("Month", month(col("epoch")))
movies_ratings = movies_ratings.fillna(12, subset=['Month'])

""" Function to group by season """
def season(x):
    if x == 3 or x == 4 or x == 5:
        return 'spring'
    elif x == 6 or x == 7 or x == 8:
        return 'summer'
    elif x == 9 or x == 10 or x == 11:
        return 'fall'
    else:
        return 'winter' 
season_udf = F.udf(season)
movies_ratings = movies_ratings.withColumn("Season", season_udf(col("Month")))

movies_ratings_season = movies_ratings.groupBy(['Season', 'movieID', 'MovieName']) \
    .avg('Rating')\
    .sort(col("avg(Rating)") \
    .desc())

movies_ratings_season_winter = movies_ratings_season[movies_ratings_season["Season"] == 'winter']
movies_ratings_season_winter.show(10)
