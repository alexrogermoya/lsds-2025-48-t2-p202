import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("Tweet Language Filter") \
    .getOrCreate()

def filter_tweets_by_language(input_path, output_path, language):
    tweets_df = spark.read.json(input_path)
    filtered_df = tweets_df.filter(col("lang") == language)
    filtered_df.write.mode("overwrite").json(output_path)

if __name__ == "__main__":
    language = sys.argv[1]
    input_path = sys.argv[2]
    output_path = sys.argv[3]
    filter_tweets_by_language(input_path, output_path, language)
    spark.stop()