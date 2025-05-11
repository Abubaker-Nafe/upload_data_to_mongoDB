import argparse
import json
import csv
import os
from pymongo import MongoClient

# Goal: being able to upload files to mongoDB, whether these files are ndjson or csv files

def load_ndjson(path):
    # Read a newline-delimited JSON file into a list of dicts.
    docs = []
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            docs.append(json.loads(line))
    return docs

def load_csv(path):
    # Read a CSV file into a list of dicts (keys = column names).
    docs = []
    with open(path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            docs.append(row)
    return docs

def main():
    parser = argparse.ArgumentParser(
        description="Import a .ndjson/.jsonl or .csv file into MongoDB"
    )
    parser.add_argument("file",
                    default= "receipts.ndjson",
                    help="Path to .ndjson or .csv file (default: %(default)s)")
    parser.add_argument("--collection",
                    default="receipts",
                    help="Collection name (default: %(default)s)")
    parser.add_argument("--uri",
                    default="mongodb://localhost:27017/",
                    help="MongoDB URI (default: %(default)s)")
    parser.add_argument("--db",
                    default="coffeeshop",
                    help="Database name (default: %(default)s)")
    

    args = parser.parse_args()

    # Connect
    client = MongoClient(args.uri) # connect to the uri (localhost:27017)
    db = client[args.db] # choose the database
    coll = db[args.collection] # choose the collection

    # Load docs based on extension
    extension = os.path.splitext(args.file)[1].lower()
    if extension == ".ndjson":
        docs = load_ndjson(args.file)
    elif extension == ".csv":
        docs = load_csv(args.file)
    else:
        print(f"ERROR: Unsupported file type '{extension}'. Use .ndjson or .csv.")
        return

    if not docs:
        print("No documents found to import.")
        return

    # Bulk insert
    result = coll.insert_many(docs)
    print(f"Inserted {len(result.inserted_ids)} documents into {args.db}.{args.collection}")

if __name__ == "__main__":
    main()
