import time
import datetime
from pyspark import sql
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType, IntegerType, BooleanType, DateType, StringType
from pyspark.sql.functions import lit, unix_timestamp
from pyspark.sql.functions import year, month, dayofmonth


spark = SparkSession.builder.appName("MoviesDataMining").getOrCreate()


movies_file_location = "movies-data/ml-10M100K/movies.dat"
ratings_file_location = "movies-data/ml-10M100K/ratings.dat"
users_file_location = "movies-data/ml-10M100K/tags.dat"

movies10M = spark.read.option("delimiter", "::").csv(movies_file_location) \
    .withColumnRenamed("_c0", "movieID") \
    .withColumnRenamed("_c1", "MovieName") \
    .withColumnRenamed("_c2", "Genre")

movies10M = movies10M.withColumn("movieID", movies10M.movieID.cast(IntegerType()))
# movies10M.show(5)
# movies10M.printSchema()


ratings10M = spark.read.option("delimiter", "::").csv(ratings_file_location)\
    .withColumnRenamed("_c0", "userID") \
    .withColumnRenamed("_c1", "movieID") \
    .withColumnRenamed("_c2", "Rating") \
    .withColumnRenamed("_c3", "Timestamp")

ratings10M = ratings10M.withColumn("userID", col("userID").cast(IntegerType())) \
        .withColumn("movieID", col("movieID").cast(IntegerType())) \
        .withColumn("Rating", col("Rating").cast(IntegerType())) \
        .withColumn("Timestamp", col("Timestamp").cast(DoubleType()))

from pyspark.sql import functions as f
from pyspark.sql import types as t
ratings10M = ratings10M.withColumn('epoch', f.date_format(ratings10M.Timestamp.cast(dataType=t.TimestampType()), "yyyy-MM-dd"))

# ratings10M.show(5)
# ratings10M.printSchema()

tags10M = spark.read.option("delimiter", "::").csv(users_file_location)\
    .withColumnRenamed("_c0", "userID") \
    .withColumnRenamed("_c1", "movieID") \
    .withColumnRenamed("_c2", "Tag") \
    .withColumnRenamed("_c3", "Timestamp") 

tags10M = tags10M.withColumn("userID", col("userID").cast(IntegerType())) \
        .withColumn("movieID", col("movieID").cast(IntegerType())) \
        .withColumn("Tag", col("Tag").cast(StringType())) \
        .withColumn("Timestamp", col("Timestamp").cast(DoubleType())) 
tags10M = tags10M.withColumn('epoch', f.date_format(tags10M.Timestamp.cast(dataType=t.TimestampType()), "yyyy-MM-dd"))
# tags10M.show(5)
# tags10M.printSchema()

## Join movies and ratings, key is movieID
movies_ratings = movies10M.join(ratings10M, movies10M.movieID == ratings10M.movieID, "outer") \
    .drop(ratings10M.movieID)
# movies_ratings.show(truncate=False)



# What are k most popular movies of all time?
movies_ratings_avg = movies_ratings.groupBy(['movieID', 'MovieName'])\
    .avg('Rating')\
    .sort(col("avg(Rating)") \
    .desc())
print("==== k highest average rated movies of all time ====")
# movies_ratings_avg.show()

# What are k most popular movies for a particular year?
movies_ratings = movies_ratings.withColumn("Year", year(col("epoch")))

print("==== k highest averaged rated movies for a particular year ====")
movies_ratings_year = movies_ratings.groupBy(['Year', 'movieID', 'MovieName']) \
    .avg('Rating')\
    .sort(col("avg(Rating)") \
    .desc())

# movies_ratings_year.show()
movies_ratings_year_2000 = movies_ratings_year[movies_ratings_year["Year"] == 2000]
# movies_ratings_year_2000.show()


# What are k most popular movies for particular season (summer, fall, winter, spring)?
print("==== k highest averaged rated movies for a particular month - season ====")

movies_ratings = movies_ratings.withColumn("Month", month(col("epoch")))
# movies_ratings.show()

from pyspark.sql import functions as F
# movies_ratings.select(F.col('Month')).distinct().show()
movies_ratings = movies_ratings.fillna(12, subset=['Month'])
# movies_ratings.select(F.col('Month')).distinct().show()

"""  function to group by season """
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
## check for season category
# movies_ratings.select(F.col('Season')).distinct().show()

movies_ratings_season = movies_ratings.groupBy(['Season', 'movieID', 'MovieName']) \
    .avg('Rating')\
    .sort(col("avg(Rating)") \
    .desc())

# movies_ratings_season.show()
movies_ratings_season_winter = movies_ratings_season[movies_ratings_season["Season"] == 'winter']
movies_ratings_season_winter.show()