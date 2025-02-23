import sys
from pyspark import SparkConf, SparkContext
from tweet_parser import parse_tweet

# Function to find the most retweeted user
def find_most_retweeted_user(input_path, language):

    # Set up the Spark configuration and context
    conf = SparkConf().setAppName("spark-tweet-retweets")
    sc = SparkContext(conf=conf)

    # Add the tweet_parser.py file to the Spark context
    sc.addPyFile("/opt/bitnami/spark/app/tweet_parser.py")

    # Load and parse the tweet data
    tweets_rdd = sc.textFile(input_path)
    parsed_rdd = tweets_rdd.map(parse_tweet).filter(lambda tweet: tweet is not None)

    # Filter tweets by the selected language
    filtered_tweets_rdd = parsed_rdd.filter(lambda tweet: tweet.language == language)

    # Extract the retweet information (or the original tweet information)
    retweet_info_rdd = filtered_tweets_rdd.flatMap(
        lambda tweet: [
            (tweet.retweeted_status["id"] if tweet.retweeted_status else tweet.tweet_id, (
                tweet.retweeted_status["user"]["screen_name"] if tweet.retweeted_status else tweet.user_name,
                tweet.retweet_count if not tweet.retweeted_status else tweet.retweeted_status["retweet_count"],
            ))
        ]
    )

    # Sum the retweets of the original tweets (only count once)
    total_user_retweet_count_rdd = retweet_info_rdd.reduceByKey(
        lambda a, b: (
            a[0],
            max(a[1], b[1]),  
        )
    )

    # Sort by the total number of retweets and take the top 10
    top_users = (
        total_user_retweet_count_rdd.map(lambda x: (x[1][0], x[1][1]))  # (Username, Total retweets)
        .sortBy(lambda x: x[1], ascending=False)
        .take(10)
    )

    # Display the results
    print(f"Top 10 most retweeted users in {language}:")
    for i, (username, retweet_count) in enumerate(top_users, 1):
        print(f"{i}. User: {username}, Total retweets: {retweet_count}")

    sc.stop()


# Main execution
if __name__ == "__main__":
    language = sys.argv[1]
    input_path = sys.argv[2]
    find_most_retweeted_user(input_path, language)
