from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast

# Function to load and clean data
def load_and_clean_data(file_path, schema):
    df = spark.read.format("csv").option("delimiter", "\t").schema(schema).load(file_path)
    return df.na.drop()

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
    print(f"All videos in category '{category}' with duration within range [{min_length}, {max_length}]:")
    df.filter((df.category == category) & (df.length.between(min_length, max_length))).show()

def find_subgraph_patterns(df, user_id, category, min_interaction):
    # Filter the DataFrame to get videos uploaded by the user in the specific category and with minimum interactions
    filtered_df = df.filter((col("uploader") == user_id) & (col("category") == category) & (col("age") >= min_interaction))

    return filtered_df


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

# Example usage of the function
user_id = input("Enter in a user id (e.g. thecrashguy): ")  # Example user ID
category = input("Enter in a catagory (e.g. Music): ") # Example category
min_interaction = int(input("Enter minimum interaction threshold (e.g., age): "))

# Find patterns:
subgraph_patterns_df = find_subgraph_patterns(df_cleaned, user_id, category, min_interaction)
subgraph_patterns_df.show()               