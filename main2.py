from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split

# Function to load and clean data
def load_and_clean_data(file_path, schema):
    # Load the data with the provided schema
    df = spark.read.format("csv").option("delimiter", "\t").schema(schema).load(file_path)
    # Drop rows with any missing values in the columns that shouldn't be null
    df_cleaned = df.na.drop()
    return df_cleaned

# Top k categories by the number of videos uploaded
def top_k_categories(df, k_categories):
    print(f"Top {k_categories} categories with the most videos uploaded:")
    df.groupBy("category").count().orderBy("count", ascending=False).show(k_categories)

# Top k rated videos
def top_k_rated_videos(df, k_rated):
    print(f"Top {k_rated} rated videos:")
    df.orderBy("rating", ascending=False).show(k_rated)

# Top k most popular videos by views
def top_k_popular_videos(df, k_views):
    print(f"Top {k_views} most popular videos:")
    df.orderBy("views", ascending=False).show(k_views)

# Videos in a specific category and duration range
def videos_in_category_duration(df, category, min_length, max_length):
    print(f"All videos in category '{category}' with duration/length within range [{min_length}, {max_length}]:")
    df.filter((df.category == category) & (df.length.between(min_length, max_length))).show()

def videos_in_duration(df, min_length, max_length):
    print(f"All videos with duration/length within range [{min_length}, {max_length}]:")
    df.filter((df.length.between(min_length, max_length))).show()

def find_subgraph_patterns(df, uploader_id, category, min_views):
    """
    Filter videos based on uploader, category, and minimum views, then find and display 
    related videos along with their stats.
    
    Args:
    df (DataFrame): DataFrame containing video data.
    uploader_id (str): The uploader's ID to filter by.
    category (str): The category to filter by.
    min_views (int): Minimum number of views to filter by.
    
    Returns:
    DataFrame: A DataFrame showing the related videos for each video that meets the specified criteria
               along with detailed statistics.
    """

    # Filter videos by uploader, category, and minimum views
    filtered_df = df.filter(
        (col("uploader") == uploader_id) &
        (col("category") == category) &
        (col("rating") >= min_views)
    )
    
    # Convert the comma-separated string of related video IDs into an array
    filtered_df = filtered_df.withColumn("relatedIDs", split(col("relatedIDs"), ","))

    # Normalize the data by exploding the array to rows
    exploded_df = filtered_df.select(
        col("videoId").alias("source_videoId"),
        col("uploader").alias("source_uploader"),
        col("category").alias("source_category"),
        col("views").alias("source_views"),
        col("relatedIDs").alias("related_video_ids")).withColumn("related_video_id", explode(col("related_video_ids")))

    # Join with original DataFrame to validate and retrieve stats for related video IDs
    valid_related_df = exploded_df.alias("exploded").join(df.alias("original"),col("exploded.related_video_id") == col("original.videoId"),"inner")

    # Selecting specific columns to simplify the output, using aliases
    result_df = valid_related_df.select(
        col("exploded.source_videoId").alias("Source Video ID"),
        col("original.videoId").alias("Related Video ID"),
        col("original.uploader").alias("Uploader"),
        col("original.category").alias("Category"),
        col("original.views").alias("Views"),
        col("original.rating").alias("Rating"),
        col("original.ratings").alias("Ratings"),
        col("original.comments").alias("Comments")
    )

    return result_df


# Initialize Spark Session
spark = SparkSession.builder.appName("YouTubeDataAnalyzer").getOrCreate()

# Define the schema of the dataset
schema = StructType([
    StructField("videoId", StringType(), True),
    StructField("uploader", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("category", StringType(), True),
    StructField("length", IntegerType(), True),
    StructField("views", IntegerType(), True),
    StructField("rating", FloatType(), True),
    StructField("ratings", IntegerType(), True),
    StructField("comments", IntegerType(), True),
    StructField("relatedIDs", StringType(), True)
])
# Load data
df_cleaned = load_and_clean_data("youtubeData/2.txt", schema)

# Usage of functions
k_categories = int(input("Enter the number of top categories to display: "))
k_rated = int(input("Enter the number of top rated videos to display: "))
k_views = int(input("Enter the number of top popular videos to display: "))
top_k_categories(df_cleaned, k_categories)
top_k_rated_videos(df_cleaned, k_rated)
top_k_popular_videos(df_cleaned, k_views)


category = input("Enter a video category (i.e Comedy, Entertainment, News & Politics, Film and Animation, Music, Peoples And Blogs): ")
min_length = int(input("Enter the minimum video length you'd like to see: "))
max_length = int(input("Enter the maximum video length you'd like to see: "))
videos_in_category_duration(df_cleaned, category, min_length, max_length)

min_length = int(input("Enter the minimum video size you'd like to see: "))
max_length = int(input("Enter the maximum video size you'd like to see: "))
videos_in_duration(df_cleaned, min_length, max_length)


# Example usage of the function
user_id = input("Enter in a user id (e.g. machinima): ")  # Example user ID
category = input("Enter in a catagory (e.g. Entertainment): ") # Example category
min_interaction = float(input("Enter minimum rating threshold (e.g., 4.5, 2.2, 5.0): "))

# Find patterns:
subgraph_patterns_df = find_subgraph_patterns(df_cleaned, user_id, category, min_interaction)
subgraph_patterns_df.show()      
     