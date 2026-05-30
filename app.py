from flask import Flask, request
import os

app = Flask(__name__)

@app.route("/", methods=["GET"])
def health():
    return "OK", 200

@app.route("/", methods=["POST"])
def ingest():
    event = request.get_json(silent=True)

    if not event:
        return "Bad Request: no JSON body", 400

    bucket = event.get("bucket")
    name = event.get("name")

    path_parts = name.split("/") if name else []

    # Example path: uploads/us/provider1/file.csv
    zone = path_parts[1] if len(path_parts) > 1 else "unknown"
    provider = path_parts[2] if len(path_parts) > 2 else "unknown"
    file_name = path_parts[-1] if path_parts else "unknown"

    print("File received")
    print("Bucket:", bucket)
    print("Zone:", zone)
    print("Provider:", provider)
    print("File path:", name)
    print("File name:", file_name)

    return "OK", 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)