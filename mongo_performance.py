from pymongo import MongoClient
from dotenv import load_dotenv
import os
import time

load_dotenv()

client = MongoClient(os.getenv("MONGO_SERVER"))

db = client["US_Airbnb"]          
collection = db["accommodation"]  

# 1. Most Popular Neighborhoods in the US
def get_most_popular_neighborhoods(limit=10):
    start = time.time()
    result = list(collection.aggregate([
        {
            "$group": {
                "_id": "$location.neighbourhood",
                "total_reviews": {"$sum": "$review.number_of_reviews"}
            }
        },
        {"$sort": {"total_reviews": -1}},
        {"$limit": limit}
    ]))
    end = time.time()
    print(f"get_most_popular_neighborhoods executed in {end - start:.4f} seconds")
    return result

# 2. Most Booked Room Type in a Specific Neighborhood
def get_most_booked_room_type(neighborhood):
    start = time.time()
    result = list(collection.aggregate([
        {"$match": {"location.neighbourhood": neighborhood}},
        {
            "$group": {
                "_id": "$room_type",
                "total_reviews": {"$sum": "$review.number_of_reviews"}
            }
        },
        {"$sort": {"total_reviews": -1}},
        {"$limit": 1}
    ]))
    end = time.time()
    print(f"get_most_booked_room_type executed in {end - start:.4f} seconds")
    return result

# 3. Highest and Lowest Reviewed Listings in a Specific Neighborhood
def get_highest_reviewed_listing(neighborhood):
    start = time.time()
    result = list(collection.find(
        {"location.neighbourhood": neighborhood}
    ).sort("review.number_of_reviews", -1).limit(1))
    end = time.time()
    print(f"get_highest_reviewed_listing executed in {end - start:.4f} seconds")
    return result

def get_lowest_reviewed_listing(neighborhood):
    start = time.time()
    result = list(collection.find(
        {"location.neighbourhood": neighborhood}
    ).sort("review.number_of_reviews", 1).limit(1))
    end = time.time()
    print(f"get_lowest_reviewed_listing executed in {end - start:.4f} seconds")
    return result

# 4. Available Accommodations
def get_available_accommodations():
    start = time.time()
    result = list(collection.find({"availability_365": {"$gt": 0}}))
    end = time.time()
    print(f"get_available_accommodations executed in {end - start:.4f} seconds")
    return result

# 5. Most Active Host
def get_most_active_host():
    start = time.time()
    result = list(collection.aggregate([
        {
            "$group": {
                "_id": "$host.host_name",
                "listings_count": {"$sum": 1}
            }
        },
        {"$sort": {"listings_count": -1}},
        {"$limit": 1}
    ]))
    end = time.time()
    print(f"get_most_active_host executed in {end - start:.4f} seconds")
    return result

# Example usage
if __name__ == "__main__":
    print("Most Popular Neighborhoods:")
    print(get_most_popular_neighborhoods())

    print("\nMost Booked Room Type in 'Western Addition':")
    print(get_most_booked_room_type("Western Addition"))

    print("\nHighest Reviewed Listing in 'Western Addition':")
    print(get_highest_reviewed_listing("Western Addition"))

    print("\nLowest Reviewed Listing in 'Western Addition':")
    print(get_lowest_reviewed_listing("Western Addition"))

    print("\nAvailable Accommodations (top 3 shown):")
    print(get_available_accommodations()[:3])

    print("\nMost Active Host:")
    print(get_most_active_host())

