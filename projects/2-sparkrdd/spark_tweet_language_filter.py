import sys
import shutil
import os
from pyspark import SparkConf, SparkContext
from tweet_parser import parse_tweet

# Function to filter tweets by language and save the output
def filter_tweets_by_language(input_path, output_path, language):
    if os.path.exists(output_path):
        shutil.rmtree(output_path)  

    conf = SparkConf().setAppName("spark-language-filter")
    sc = SparkContext(conf=conf)

    # Add the 'tweet_parser.py'
    sc.addPyFile("/opt/bitnami/spark/app/tweet_parser.py")
    
    # Read the tweets from the file, use 'parse_tweet' and filter by language
    tweets_rdd = sc.textFile(input_path)
    parsed_rdd = tweets_rdd.map(parse_tweet).filter(lambda tweet: tweet is not None)
    filtered_rdd = parsed_rdd.filter(lambda tweet: tweet.language == language and tweet.tweet_id != 0)
    
    # Save results to the output file
    result_rdd = filtered_rdd.map(lambda tweet: tweet.__dict__)
    result_rdd.coalesce(1).saveAsTextFile(output_path)

    sc.stop()  

# Main script execution
if __name__ == "__main__":
    language = sys.argv[1]  # Language to filter
    input_path = sys.argv[2]  # Input path
    output_path = sys.argv[3]  # Output path
    filter_tweets_by_language(input_path, output_path, language)