from dataclasses import dataclass
import json
import sys

_, source = sys.argv


@dataclass
class Tweet:
    tweet_id: int
    text: str
    user_id: int
    user_name: str
    language: str
    timestamp_ms: int
    retweet_count: int


def parse_tweet(tweet: str) -> Tweet:
    tweet_json = json.loads(tweet)
    return Tweet(
        tweet_id=tweet_json["id"],
        text=tweet_json["text"],
        user_id=tweet_json["user"]["id"],
        user_name=tweet_json["user"]["name"],
        language=tweet_json["lang"],
        timestamp_ms=tweet_json["timestamp_ms"],
        retweet_count=tweet_json["retweet_count"],
    )


with open(source, "r", encoding="utf-8") as f:
    first_line = f.readline().strip()
    tweet = parse_tweet(first_line)
    print(f"\n{tweet}\n")
