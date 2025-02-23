import sys
import json
import shutil
import os
from pyspark import SparkConf, SparkContext
from tweet_parser import parse_tweet

# Function to filter tweets by language and find most retweeted user
def find_most_retweeted_user(input_path, language):

    conf = SparkConf().setAppName("spark-tweet-retweets")
    sc = SparkContext(conf=conf)

    sc.addPyFile("/opt/bitnami/spark/app/tweet_parser.py")

    tweets_rdd = sc.textFile(input_path)
    parsed_rdd = tweets_rdd.map(parse_tweet).filter(lambda tweet: tweet is not None)

    filtered_tweets_rdd = parsed_rdd.filter(lambda tweet: tweet.language == language)

    retweet_info_rdd = filtered_tweets_rdd.map(
        lambda tweet: (
            tweet.retweeted_id if tweet.retweeted_id != 0 else tweet.tweet_id,
            (
                tweet.user_name,
                tweet.retweet_count,
            )
        )
    )

    # Sum, the retweet counts for each user
    total_user_retweet_count_rdd = retweet_info_rdd.reduceByKey(
        lambda a, b: (
            a[0],
            max(a[1], b[1]),
        ) # (Username, Total retweet count)
    )
    
    # Sort by retweet count in descending order and take top 10
    top_users = (
        total_user_retweet_count_rdd.map(lambda x: (x[0], x[1][0], x[1][1]))
        .sortBy(lambda x: x[2], ascending=False)
        .take(10)
    )

    # Print the results
    print(f"Top 10 users with the most retweeted tweets in {language}:")
    for i, (user_id, username, retweet_count) in enumerate(top_users, 1):
        print(f"{i}. User: {username} ({user_id}) ---- {retweet_count}")

    sc.stop()


# Main script execution
if __name__ == "__main__":
    language = sys.argv[1]
    input_path = sys.argv[2]
    find_most_retweeted_user(input_path, language)
