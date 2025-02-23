import json
from dataclasses import dataclass


# Define the Tweet class
@dataclass
class Tweet:
    tweet_id: int
    text: str
    user_id: int
    user_name: str
    language: str
    timestamp_ms: int
    retweet_count: int
    retweeted_status: dict  # To manage retweets at L4Q2 and L4Q4


# Function to parse a tweet
def parse_tweet(tweet: str) -> Tweet:
    if not tweet.strip():
        return None
    try:
        tweet_json = json.loads(tweet)
    except json.JSONDecodeError:
        return None

    return Tweet(
        tweet_id=tweet_json.get("id", 0),
        text=tweet_json.get("text", ""),
        user_id=tweet_json.get("user", {}).get("id", 0),
        user_name=tweet_json.get("user", {}).get("name", ""),
        language=tweet_json.get("lang", ""),
        timestamp_ms=tweet_json.get("timestamp_ms", 0),
        retweet_count=tweet_json.get("retweet_count", 0),
        retweeted_status=tweet_json.get(
            "retweeted_status", {}
        ),  # Used in L4Q2 and L4Q3
    )


# Read the first tweet from Eurovision3.json and print
with open("/opt/bitnami/spark/app/data/Eurovision3.json", "r", encoding="utf-8") as f:
    first_line = f.readline().strip()
    tweet = parse_tweet(first_line)
    print(
        f"\nTweet(tweet_id={tweet.tweet_id}, text='{tweet.text}', user_id={tweet.user_id}, user_name='{tweet.user_name}', language='{tweet.language}', timestamp='{tweet.timestamp_ms}', retweet_count={tweet.retweet_count})\n"
    )
