import pandas as pd
from pymongo import MongoClient

PROCESSED_FILE = "/opt/airflow/data/processed_reviews.csv"

def load_to_mongo():
    df = pd.read_csv(PROCESSED_FILE)

    client = MongoClient("mongodb://mongodb:27017")
    db = client["tiktok_reviews"]
    collection = db["reviews"]

    records = df.to_dict(orient='records')
    if records:
        collection.insert_many(records)

if __name__ == "__main__":
    load_to_mongo()