from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.responses import FileResponse
from pathlib import Path

app = FastAPI()

STORAGE_DIR = Path("storage") # Folder where the blocks will be stored

@app.put("/files/{filename}/blocks/{block_number}/content")
async def upload_block(filename: str, block_number: int, file: UploadFile = File(...)):
    # Create the directory if it doesn't exist
    file_dir = STORAGE_DIR / filename
    file_dir.mkdir(parents=True, exist_ok=True)

    # Path where the block will be stored
    file_path = file_dir / str(block_number)

    # Save the file content into the block
    with open(file_path, "wb") as f:
        f.write(await file.read())

    return {"message": f"Block {block_number} of {filename} stored successfully"}

@app.get("/files/{filename}/blocks/{block_number}/content")
def get_block(filename: str, block_number: int):
    # Path of the block inside the storage folder
    block_path = STORAGE_DIR / filename / str(block_number)

    # Check if the file exists
    if not block_path.exists():
        raise HTTPException(status_code=404, detail="Block not found")

    # Return the file as the response
    return FileResponse(block_path, media_type="application/octet-stream")