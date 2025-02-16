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
        tweet_id=tweet_json.get("id", 0),
        text=tweet_json.get("text", ""),
        user_id=tweet_json.get("user", {}).get("id", 0),
        user_name=tweet_json.get("user", {}).get("name", ""),
        language=tweet_json.get("lang", ""),
        timestamp_ms=tweet_json.get("timestamp_ms", 0),
        retweet_count=tweet_json.get("retweet_count", 0),
    )


with open(source, "r", encoding="utf-8") as f:
    first_line = f.readline().strip()
    tweet = parse_tweet(first_line)
    print(f"\n{tweet}\n")
