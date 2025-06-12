import pandas as pd
from pymongo import MongoClient
import os
import time
from dotenv import load_dotenv

load_dotenv()

# Connect to MongoDB
client = MongoClient(os.getenv("MONGO_SERVER"))

db = client["US_Airbnb"]          
accommodation_collection = db["accommodation"]     
location_collection = db["location"]
host_collection = db["host"]
review_collection = db["review"]

df = pd.read_csv("AB_US_2023.csv", low_memory=False)  

df["price"] = pd.to_numeric(df["price"], errors="coerce")        
df["minimum_nights"] = df["minimum_nights"].astype(int)
df["id"] = df["id"].astype(int)
df["name"] = df["name"].astype(str)
df["room_type"] = df["room_type"].astype(str)
df["availability_365"] = df["availability_365"].astype(str)
df["neighbourhood"] = df["neighbourhood"].astype(str)
df["latitude"] = df["latitude"].astype(str)
df["longitude"] = df["longitude"].astype(str)
df["city"] = df["city"].astype(str)
df["host_name"] = df["host_name"].astype(str)
df["calculated_host_listings_count"] = df["calculated_host_listings_count"].astype(int)
df["number_of_reviews"] = df["number_of_reviews"].astype(int)

columns_accommodation = ['id', 'price', 'minimum_nights', 'name', 'room_type', 'availability_365']
columns_location = ['id', 'neighbourhood', 'latitude', 'longitude', 'city']
columns_host = ['id', 'host_name', 'calculated_host_listings_count']
columns_review = ['id', 'number_of_reviews', 'last_review', "reviews_per_month", "number_of_reviews_ltm"]
df_accommodation = df[columns_accommodation]
df_location = df[columns_location]
df_host = df[columns_host]
df_review = df[columns_review]

accommodation_data = df_accommodation.to_dict(orient="records")
location_data = df_location.to_dict(orient="records")
host_data = df_host.to_dict(orient="records")
review_data = df_review.to_dict(orient="records")

start_time = time.time()
accommodation_collection.insert_many(accommodation_data)
location_collection.insert_many(location_data)
host_collection.insert_many(host_data)
review_collection.insert_many(review_data)

end_time = time.time()

print("Data inserted successfully.")
# 134.9417 seconds
print(f"Query executed in {end_time - start_time:.4f} seconds")
