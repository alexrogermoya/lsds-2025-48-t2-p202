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


# Function to filter tweets by language and find msot retweeted user
def find_most_retweeted_user(input_path, language):

    conf = SparkConf().setAppName("spark-tweet-retweets")
    sc = SparkContext(conf=conf)

    tweets_rdd = sc.textFile(input_path)
    parsed_rdd = tweets_rdd.map(parse_json_safe).filter(lambda tweet: tweet is not None)

    filtered_tweets_rdd = parsed_rdd.filter(
        lambda tweet: tweet.get("lang") == language and "retweeted_status" in tweet
    )

    retweet_info_rdd = filtered_tweets_rdd.map(
        lambda tweet: (
            tweet["retweeted_status"]["user"]["id"],  # User ID (key)
            (
                tweet["retweeted_status"]["user"][
                    "screen_name"
                ],  # Username of the original poster
                tweet["retweeted_status"]["retweet_count"],  # Retweet count
            ),
        )
    )

    # Map each user to the retweet count of their tweets
    user_retweet_count_rdd = retweet_info_rdd.map(
        lambda user_info: (
            user_info[0],
            (user_info[1][0], user_info[1][1]),
        )  # (User ID, (Username, Retweet count))
    )

    # Su, the retweet counts for each user
    total_user_retweet_count_rdd = user_retweet_count_rdd.reduceByKey(
        lambda a, b: (a[0], a[1] + b[1])  # (Username, Total retweet count)
    )
    # Sort by retweet count in descending order and take top 10
    top_users = (
        total_user_retweet_count_rdd.map(lambda x: (x[0], x[1][0], x[1][1]))
        .sortBy(lambda x: x[1], ascending=False)
        .take(10)
    )

    # Print the results
    print(f"Top 10 users with the most retweeted tweets in {language}:")
    for i, (user_id, username, retweet_count) in enumerate(top_users, 1):
        print(f"{i}. User: {username}, Total retweets: {retweet_count}")

    sc.stop()


# Main script execution
if __name__ == "__main__":
    language = sys.argv[1]
    input_path = sys.argv[2]
    find_most_retweeted_user(input_path, language)
