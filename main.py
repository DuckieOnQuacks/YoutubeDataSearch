from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import corr, col

#Schema of items 
schema = StructType([
    StructField("video_id", StringType(), True),
    StructField("uploader", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("category", StringType(), True),
    StructField("length", IntegerType(), True),
    StructField("views", IntegerType(), True),
    StructField("rating", FloatType(), True),
    StructField("ratings", IntegerType(), True),
    StructField("comments", IntegerType(), True),
])

# Initialize Spark Session
spark = SparkSession.builder.appName("YouTubeDataAnalyzer").getOrCreate()
# Load data from a text file
df = spark.read.format("csv").option("delimiter", "\t").schema(schema).load("youtubeData/3.txt")
dfCleaned = df.na.drop()
dfCleaned.show()


#Part 1
# Top k categories by the number of videos uploaded
print("Top 5 categories in which the most number of videos are uploaded.") 
dfCleaned.groupBy("category").count().orderBy("count", ascending=False).show(5)
# Top k rated videos
print("Top 5 rated videos.")
dfCleaned.orderBy("rating", ascending=False).show(5)
# Top k most popular videos by views
print("Top 5 most popular videos.") 
dfCleaned.orderBy("views", ascending=False).show(5)


#Part 2
# Find videos in a specific category with duration within [t1, t2]
print("All videos in categorie Comedy with duration within a range [10, 100]")
dfCleaned.filter((dfCleaned.category == 'Comedy') & (dfCleaned.length.between(10, 100))).show()
print("All videos with size in a range [t1, t2]")
dfCleaned.filter((dfCleaned.length.between(10,20))).show()

#Part 3
# 1. Correlation between views and ratings
print(f"Correlation between views and ratings:")
correlation = dfCleaned.select(corr("views", "rating").alias("correlation_coefficient")).collect()[0]["correlation_coefficient"]


# 2. Viewer Engagement Analysis
dfEngagement = dfCleaned.withColumn("comments_per_view", col("comments") / col("views")).withColumn("ratings_per_view", col("ratings") / col("views"))
# Display top videos by engagement
print("Top videos by engagement.")
dfEngagement.select("video_id", "comments_per_view", "ratings_per_view").orderBy("comments_per_view", ascending=False).show()

# 3. Trend Analysis Over Time
# Analyzing the average views and ratings by age of video
print("Average views and ratings by age of video.")
dfCleaned.groupBy("age").avg("views", "rating").orderBy("age").show()
