import os
import json
import sys


def split_wikipedia_dump(input_file, output_dir):
    os.makedirs(output_dir, exist_ok=True)  # Create output folder if it doesn't exist

    with open(input_file, "r", encoding="utf-8") as infile:
        for line in infile:
            try:
                article = json.loads(line)  # Convert line to JSON
                identifier = str(article.get("identifier", "unknown"))  # Get article ID
                output_path = os.path.join(output_dir, f"{identifier}.json")

                with open(output_path, "w", encoding="utf-8") as outfile:
                    json.dump(article, outfile, indent=4)  # Save JSON to file

            except json.JSONDecodeError:
                print(f"Error processing line: {line[:100]}...")  # Print error preview

    print(f"Done. Files saved in {output_dir}")


# Usage: python split_wikipedia.py wikipedia10.json output_10
if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python split_wikipedia.py <json_file> <output_dir>")
        sys.exit(1)

    input_file = sys.argv[1]
    output_dir = sys.argv[2]

    split_wikipedia_dump(input_file, output_dir)
