import json
from fastapi import FastAPI
from fastapi import HTTPException
from pydantic import BaseModel

# Data model for the POST /files request
class FileRequest(BaseModel):
    file_name: str
    size: int

# Initialize the FastAPI application
app = FastAPI()

# Load configuration from config.json 
with open("config.json") as f:
    config = json.load(f)

# Load file metadata from files.json 
with open("files.json") as f:
    files_data = json.load(f)

# Endpoint to retrieve the available datanodes
@app.get("/datanodes")
def get_datanodes():
    return {"datanodes": config["datanodes"]}

# Endpoint to create a new file in the system
@app.post("/files")
def create_file(file: FileRequest):
    file_name = file.file_name
    size = file.size
    blocks = []

    # Calculate the number of blocks needed to store the file
    num_blocks = (size + config["block_size"] - 1) // config["block_size"]

    # Loop through all blocks and assign datanodes and replicas
    for block_num in range(num_blocks):
        # Determine the size of the current block (the last block may be smaller)
        block_size = config["block_size"] if block_num < num_blocks - 1 else size % config["block_size"]
        block_replicas = []

        # Assign replicas to datanodes using the modulo-based placement policy
        for replica_num in range(config["replication_factor"]):
            datanode_index = (block_num + replica_num) % len(config["datanodes"])
            block_replicas.append(config["datanodes"][datanode_index])

        # Add the block with its number, size, and replicas to the block list
        blocks.append({
            "number": block_num,
            "size": block_size,
            "replicas": block_replicas
        })

    # Add the file's metadata to files.json
    files_data[file_name] = {
        "file_name": file_name,
        "size": size,
        "blocks": blocks
    }

    # Write the updated file metadata to files.json
    with open("files.json", "w") as f:
        json.dump(files_data, f, indent=2)

    # Return the response to the client with block and replica details
    return {"file_name": file_name, "size": size, "blocks": blocks}

# Endpoint to retrieve the available datanodes
@app.get("/files/{filename}")
def get_file_metadata(filename: str):
    if filename in files_data:
        return files_data[filename]
    else:
        raise HTTPException(status_code=404, detail="File not found")