import json
import sys
from tweet_parser import parse_tweet

_, source = sys.argv

languages = {}

with open(source, "r", encoding="utf-8") as f:
    for line in f:
        if not line.strip():  # Skip empty lines
            continue
        try:
            parsed_tweet = parse_tweet(line)
            language = parsed_tweet.language
            if language in languages:
                languages[language] += 1
            else:
                languages[language] = 1
        except json.decoder.JSONDecodeError:
            pass
        except KeyError as e:
            print(f"KeyError: {e}")
            continue

print(f"\n{languages}\n")
