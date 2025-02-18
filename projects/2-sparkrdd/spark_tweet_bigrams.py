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

# Function to extract bigrams from a given tweet's text
def extract_bigrams(text):
    words = text.split()
    return [(words[i], words[i + 1]) for i in range(len(words) - 1)]

# Function to filter tweets by language and process bigrams
def find_most_repeated_bigrams(input_path, output_path, language):
    if os.path.exists(output_path):
        shutil.rmtree(output_path) # Remove output directory if it exists
        
    conf = SparkConf().setAppName("spark-tweet-bigrams")
    sc = SparkContext(conf=conf)

    tweets_rdd = sc.textFile(input_path)
    parsed_rdd = tweets_rdd.map(parse_json_safe).filter(lambda tweet: tweet is not None)

    bigrams_rdd = parsed_rdd.filter(lambda tweet: tweet.get("lang") == language) \
                            .flatMap(lambda tweet: extract_bigrams(tweet.get("text", "")))
    
    bigram_counts_rdd = bigrams_rdd.map(lambda bigram: (bigram, 1)) \
                                   .reduceByKey(lambda a, b: a + b)
    
    filtered_bigrams_rdd = bigram_counts_rdd.filter(lambda x: x[1] > 1) \
                                            .sortBy(lambda x: x[1], ascending=False)
    
    filtered_bigrams_rdd.map(lambda x: f"{x[0][0]} {x[0][1]}\t{x[1]}") \
                        .coalesce(1) \
                        .saveAsTextFile(output_path)
    
    sc.stop()

# Main script execution
if __name__ == "__main__":
    language = sys.argv[1]
    input_path = sys.argv[2]
    output_path = sys.argv[3]
    find_most_repeated_bigrams(input_path, output_path, language)