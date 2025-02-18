import sys
import json
import shutil
import os
from pyspark import SparkConf, SparkContext

# Function to safely parse JSON
def parse_json_safe(line):
    try:
        return json.loads(line)
    except json.JSONDecodeError:
        return None  

# Function to filter tweets by language and save the output
def filter_tweets_by_language(input_path, output_path, language):
    if os.path.exists(output_path):
        shutil.rmtree(output_path)  # Remove output directory if it exists 

    conf = SparkConf().setAppName("spark-language-filter")
    sc = SparkContext(conf=conf)

    tweets_rdd = sc.textFile(input_path)
    parsed_rdd = tweets_rdd.map(parse_json_safe).filter(lambda tweet: tweet is not None)
    filtered_rdd = parsed_rdd.filter(lambda tweet: tweet.get("lang") == language)

    result_rdd = filtered_rdd.map(json.dumps)
    result_rdd.coalesce(1).saveAsTextFile(output_path)

    sc.stop()  

# Main script execution
if __name__ == "__main__":
    language = sys.argv[1]
    input_path = sys.argv[2]
    output_path = sys.argv[3]
    filter_tweets_by_language(input_path, output_path, language)