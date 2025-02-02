import requests
from pathlib import Path

# Ask the user for the filename and for the destination path using input 
filename = input("Enter the filename to download: ").strip()
destination_path = Path(input("Enter the destination path: ").strip())

# Verify if the path is valid
if not destination_path.parent.exists():
    print("Error: Destination directory does not exist.")
    exit(1)

# # Obtain file metadata from the namenode 
namenode_url = f"http://localhost:8000/files/{filename}"
response = requests.get(namenode_url)

if response.status_code != 200:
    print(f"Error retrieving file metadata: {response.text}")
    exit(1)

file_metadata = response.json()
blocks = file_metadata["blocks"]

# Create the file at the destination path 
with open(destination_path, "wb") as f:
    for block in blocks:
        for replica in block["replicas"]:
            datanode_url = f"http://localhost:{replica['port']}/files/{filename}/blocks/{block['number']}/content"
            block_data = requests.get(datanode_url).content

            if block_data:
                f.write(block_data)
                print(f"Block {block['number']} downloaded from {replica['host']}:{replica['port']}")
                break
        else:
            print(f"Error downloading block {block['number']}. No replicas available.")
            exit(1)

print(f"File {filename} downloaded successfully to {destination_path}")