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

# Function to parse a tweet
def parse_tweet(tweet: str) -> Tweet:
    if not tweet.strip(): 
        return None
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

# Read the first tweet from Eurovision3.json and print
with open("/opt/bitnami/spark/app/data/Eurovision3.json", "r", encoding="utf-8") as f:
    first_line = f.readline().strip()  
    tweet = parse_tweet(first_line)  
    print(f"\n{tweet}\n")  