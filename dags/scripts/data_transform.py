import pandas as pd
import re
import os

DATA_FILE = "/opt/airflow/data/tiktok_google_play_reviews.csv"
PROCESSED_FILE = "/opt/airflow/data/processed_reviews.csv"


def replace_nulls():
    if not os.path.exists(DATA_FILE):
        raise FileNotFoundError(f"Source file not found at {DATA_FILE}")

    df = pd.read_csv(DATA_FILE)
    # Fill empty reviews or names with a dash
    df.fillna("-", inplace=True)
    df.to_csv(PROCESSED_FILE, index=False)
    print("Step 1: Nulls replaced.")


def sort_by_date():
    df = pd.read_csv(PROCESSED_FILE)


    if 'at' in df.columns:
        df.rename(columns={'at': 'created_date'}, inplace=True)


    df['created_date'] = pd.to_datetime(df['created_date'], dayfirst=True, errors='coerce')


    df = df.dropna(subset=['created_date'])

    df.sort_values(by='created_date', inplace=True, ascending=True)
    df.to_csv(PROCESSED_FILE, index=False)
    print("Step 2: Data sorted by date.")


def clean_content():
    df = pd.read_csv(PROCESSED_FILE)


    df['content'] = df['content'].apply(lambda x: re.sub(r"[^A-Za-z0-9\s.,!?'-]", '', str(x)))

    df.to_csv(PROCESSED_FILE, index=False)
    print("Step 3: Content cleaned (emojis removed).")