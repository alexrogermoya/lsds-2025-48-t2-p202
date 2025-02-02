import requests
import json
from pathlib import Path

# Ask the user for a path and for a filename using input
file_path = Path(input("Enter the file path: ").strip())
filename = input("Enter the filename to store in SSHDFS: ").strip()

# Verify that the file exists
if not file_path.exists():
    print("Error: File does not exist.")
    exit(1)

# Obtain  the file size
file_size = file_path.stat().st_size

# Create the file at the namenode
namenode_url = "http://localhost:8000/files"
response = requests.post(namenode_url, json={"file_name": filename, "size": file_size})

if response.status_code != 200:
    print("Error creating file in namenode:", response.text)
    exit(1)

file_metadata = response.json()  # Obtain file metadata (blocks and replicas)
block_size = 1000000

# Read the file and upload blocks
with open(file_path, "rb") as f:
    for block in file_metadata["blocks"]:
        block_data = f.read(block["size"])

        for replica in block["replicas"]:
            datanode_url = f"http://localhost:{replica['port']}/files/{filename}/blocks/{block['number']}/content"
            response = requests.put(datanode_url, files={"file": block_data})

            if response.status_code == 200:
                print(
                    f"Block {block['number']} stored in {replica['host']}:{replica['port']}"
                )
            else:
                print(
                    f"Error uploading block {block['number']} to {replica['host']}:{replica['port']}"
                )
