import pandas as pd
from pymongo import MongoClient
import os
import time
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor

load_dotenv()

client = MongoClient(os.getenv("MONGO_SERVER"))
db = client["US_Airbnb"]
accommodation_collection = db["accommodation"]

df = pd.read_csv("AB_US_2023.csv", low_memory=False)

df["price"] = pd.to_numeric(df["price"], errors="coerce")
df["minimum_nights"] = df["minimum_nights"].astype(int)
df["id"] = df["id"].astype(int)
df["name"] = df["name"].astype(str)
df["room_type"] = df["room_type"].astype(str)
df["availability_365"] = df["availability_365"].astype(int)
df["neighbourhood"] = df["neighbourhood"].astype(str)
df["latitude"] = df["latitude"].astype(float)
df["longitude"] = df["longitude"].astype(float)
df["city"] = df["city"].astype(str)
df["host_name"] = df["host_name"].astype(str)
df["calculated_host_listings_count"] = df["calculated_host_listings_count"].astype(int)
df["number_of_reviews"] = df["number_of_reviews"].astype(int)
df["last_review"] = df["last_review"].fillna("")
df["reviews_per_month"] = df["reviews_per_month"].fillna(0)
df["number_of_reviews_ltm"] = df["number_of_reviews_ltm"].fillna(0)

documents = []
for _, row in df.iterrows():
    doc = {
        "id": row["id"],
        "name": row["name"],
        "price": row["price"],
        "minimum_nights": row["minimum_nights"],
        "room_type": row["room_type"],
        "availability_365": row["availability_365"],
        "host": {
            "host_name": row["host_name"],
            "calculated_host_listings_count": row["calculated_host_listings_count"]
        },
        "location": {
            "neighbourhood": row["neighbourhood"],
            "latitude": row["latitude"],
            "longitude": row["longitude"],
            "city": row["city"]
        },
        "review": {
            "number_of_reviews": row["number_of_reviews"],
            "last_review": row["last_review"],
            "reviews_per_month": row["reviews_per_month"],
            "number_of_reviews_ltm": row["number_of_reviews_ltm"]
        }
    }
    documents.append(doc)

batch_size = 20000

def insert_batch(batch):
    accommodation_collection.insert_many(batch, ordered=False)

start_time = time.time()

with ThreadPoolExecutor(max_workers=4) as executor:
    for i in range(0, len(documents), batch_size):
        batch = documents[i:i + batch_size]
        executor.submit(insert_batch, batch)

end_time = time.time()

print(f"Execution time: {end_time - start_time:.4f} seconds") # 31.0748 seconds