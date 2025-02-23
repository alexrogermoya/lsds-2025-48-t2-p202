import sys
import json
import os
from pyspark import SparkConf, SparkContext
from tweet_parser import parse_tweet  # Import the parse_tweet function


# Function to find the most retweeted tweets in a specific language
def find_most_repeated_retweets(input_path, language):
    conf = SparkConf().setAppName("spark-tweet-retweets")
    sc = SparkContext(conf=conf)

    # Add the 'tweet_parser.py'
    sc.addPyFile("/opt/bitnami/spark/app/tweet_parser.py")

    # Read and parse tweets
    tweets_rdd = sc.textFile(input_path)
    parsed_rdd = tweets_rdd.map(parse_tweet).filter(lambda tweet: tweet is not None)

    # Filter tweets by language and check if they are retweets
    filtered_tweets_rdd = parsed_rdd.filter(
        lambda tweet: tweet.language == language and tweet.retweeted_status
    )

    # Extract retweet information
    retweet_info_rdd = filtered_tweets_rdd.map(
        lambda tweet: (
            tweet.retweeted_status["id"],  # Retweeted tweet ID (key)
            (
                tweet.retweeted_status["user"][
                    "screen_name"
                ],  # Original poster username
                tweet.retweeted_status["text"],  # Tweet text
                tweet.retweeted_status["retweet_count"],  # Retweet count
            ),
        )
    )

    # Use 'reduceByKey' only keeping the max retweet count
    aggregated_rdd = retweet_info_rdd.reduceByKey(lambda a, b: a if a[2] > b[2] else b)

    # Sort by retweet count in descending order and take the top 10
    top_retweets = (
        aggregated_rdd.map(lambda x: x[1])
        .sortBy(lambda x: x[2], ascending=False)
        .take(10)
    )

    # Print the results
    print(f"\nTop 10 most retweeted tweets in '{language}':")
    for i, (username, text, retweet_count) in enumerate(top_retweets, 1):
        print(f"{i}. User: {username}, Retweets: {retweet_count}, Tweet: {text}")

    sc.stop()


# Main script execution
if __name__ == "__main__":
    language = sys.argv[1]
    input_path = sys.argv[2]
    find_most_repeated_retweets(input_path, language)
