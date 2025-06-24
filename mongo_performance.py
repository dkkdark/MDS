from pymongo import MongoClient
from dotenv import load_dotenv
import os
import time

# Load environment variables
load_dotenv()

# Connect to MongoDB
client = MongoClient(os.getenv("MONGO_SERVER"))
db = client["US_Airbnb"]
collection = db["accommodation"]

# ✅ Indexes to optimize query performance
collection.create_index("location.neighbourhood")
collection.create_index("minimum_nights")
collection.create_index("price")
collection.create_index("room_type")
collection.create_index("review.number_of_reviews")
collection.create_index("availability_365")
collection.create_index("host.host_name")

# Compound indexes for combined filtering and sorting
collection.create_index([("location.neighbourhood", 1), ("minimum_nights", 1)])
collection.create_index([("availability_365", -1), ("price", 1)])
collection.create_index([("host.host_name", 1)])

# a. Most popular neighbourhoods grouped by min nights (filtered by price > 300)
def get_popular_neighbourhoods_grouped_by_min_nights(min_price=300):
    start = time.time()
    result = list(collection.aggregate([
        {"$match": {"price": {"$gt": min_price}}},
        {
            "$group": {
                "_id": {
                    "neighbourhood": "$location.neighbourhood",
                    "minimum_nights": "$minimum_nights"
                },
                "total_listings": {"$sum": 1},
                "avg_price": {"$avg": "$price"}
            }
        },
        {"$sort": {"total_listings": -1}}
    ]))
    end = time.time()
    print(f"get_popular_neighbourhoods_grouped_by_min_nights executed in {end - start:.4f} seconds")
    return result

# b. Most booked room type (by count of listings)
def get_most_booked_room_type():
    start = time.time()
    result = list(collection.aggregate([
        {
            "$group": {
                "_id": "$room_type",
                "total_bookings": {"$sum": 1}
            }
        },
        {"$sort": {"total_bookings": -1}}
    ]))
    end = time.time()
    print(f"get_most_booked_room_type executed in {end - start:.4f} seconds")
    return result

# c. Highest and lowest reviewed listings overall
def get_highest_reviewed_listing():
    start = time.time()
    result = list(collection.find().sort("review.number_of_reviews", -1).limit(1))
    end = time.time()
    print(f"get_highest_reviewed_listing executed in {end - start:.4f} seconds")
    return result

def get_lowest_reviewed_listing():
    start = time.time()
    result = list(collection.find().sort("review.number_of_reviews", 1).limit(1))
    end = time.time()
    print(f"get_lowest_reviewed_listing executed in {end - start:.4f} seconds")
    return result

# d. Available accommodations with fields and sorting
def get_available_accommodations():
    start = time.time()
    result = list(collection.find(
        {"availability_365": {"$gt": 0}},
        {"_id": 0, "accommodation_id": 1, "name": 1, "availability_365": 1, "price": 1}
    ).sort("availability_365", -1))
    end = time.time()
    print(f"get_available_accommodations executed in {end - start:.4f} seconds")
    return result

# e. Most active hosts (based on number of listings)
def get_most_active_host():
    start = time.time()
    result = list(collection.aggregate([
        {
            "$group": {
                "_id": "$host.host_name",
                "calculated_host_listings_count": {"$sum": 1}
            }
        },
        {"$sort": {"calculated_host_listings_count": -1}}
    ]))
    end = time.time()
    print(f"get_most_active_host executed in {end - start:.4f} seconds")
    return result

# Sample execution
if __name__ == "__main__":
    print("\n🔹 a. Most Popular Neighbourhoods (Grouped by Min Nights, Price > 300):")
    print(get_popular_neighbourhoods_grouped_by_min_nights()[:1])

    print("\n🔹 b. Most Booked Room Type:")
    print(get_most_booked_room_type()[:1])

    print("\n🔹 c. Highest Reviewed Listing:")
    print(get_highest_reviewed_listing())

    print("\n🔹 c. Lowest Reviewed Listing:")
    print(get_lowest_reviewed_listing())

    print("\n🔹 d. Available Accommodations (Top 3):")
    print(get_available_accommodations()[:1])

    print("\n🔹 e. Most Active Host:")
    print(get_most_active_host()[:1])