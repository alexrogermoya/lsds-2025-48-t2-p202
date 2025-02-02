#!/bin/bash

set -e

docker compose down
docker compose up --detach --build

echo "Waiting for service to be ready..."
until docker compose logs | grep -q "Uvicorn running on"; do
    sleep 2
done

python3 client/list_datanodes.py
python3 client/upload.py <<< "test_files/cat.jpg
cute-cat.jpg"
python3 client/download.py <<< "cute-cat.jpg
cute-cat.jpg"

if cmp -s "test_files/cat.jpg" "cute-cat.jpg"; then
    echo "✅ Test passed."
else
    echo "Downloaded file is different from uploaded file"
    echo "❌ Test failed."
fi
