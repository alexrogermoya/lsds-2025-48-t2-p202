import sys
import json
import shutil
import os
from pyspark import SparkConf, SparkContext


def parse_json_safe(line):
    try:
        return json.loads(line)
    except json.JSONDecodeError:
        return None


# Function to filter tweets by language and process retweets
def find_most_repeated_retweets(input_path, language):

    conf = SparkConf().setAppName("spark-tweet-retweets")
    sc = SparkContext(conf=conf)

    tweets_rdd = sc.textFile(input_path)
    parsed_rdd = tweets_rdd.map(parse_json_safe).filter(lambda tweet: tweet is not None)

    filtered_tweets_rdd = parsed_rdd.filter(
        lambda tweet: tweet.get("lang") == language and "retweeted_status" in tweet
    )

    retweet_info_rdd = filtered_tweets_rdd.map(
        lambda tweet: (
            tweet["retweeted_status"]["id"],  # Retweeted tweet ID (key)
            (
                tweet["retweeted_status"]["user"][
                    "screen_name"
                ],  # Username of the original poster
                tweet["retweeted_status"]["text"],  # Tweet text
                tweet["retweeted_status"]["retweet_count"],  # Retweet count
            ),
        )
    )

    # Remove duplicates by tweet ID (keep the first occurrence)
    # unique_retweets_rdd = retweet_info_rdd.reduceByKey(lambda x, y: x)

    # Extract only the (username, retweet_count, text) tuples
    # retweet_data_rdd = unique_retweets_rdd.map(lambda x: x[1])

    # Aggregate retweets by summing the counts for each unique tweet ID
    aggregated_rdd = retweet_info_rdd.reduceByKey(
        lambda a, b: (
            a[0],
            a[1],
            a[2] + b[2],
        )  # Keep username & text, sum retweet counts
    )

    # Sort by retweet count in descending order and take top 10
    top_retweets = (
        aggregated_rdd.map(lambda x: x[1])
        .sortBy(lambda x: x[2], ascending=False)
        .take(10)
    )

    # Print the results
    print(f"Top 10 most retweeted tweets in {language}:")
    for i, (username, text, retweet_count) in enumerate(top_retweets, 1):
        print(f"{i}. User: {username}, Retweets: {retweet_count}, Tweet: {text}")

    sc.stop()


# Main script execution
if __name__ == "__main__":
    language = sys.argv[1]
    input_path = sys.argv[2]
    find_most_repeated_retweets(input_path, language)
