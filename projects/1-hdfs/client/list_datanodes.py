import json
from pathlib import Path

config_path = Path("namenode/config.json")

# Check if config.json exists
if not config_path.exists():
    print("Error: config.json not found")
    exit(1)

# Load the configuration data from the file
with open(config_path, "r") as file:
    config = json.load(file)

# Get the list of datanodes from the configuration
datanodes = config.get("datanodes", [])

print("Datanodes:")
for node in datanodes:
    print(f"- {node['host']}:{node['port']}")
