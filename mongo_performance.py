from pymongo import MongoClient
from dotenv import load_dotenv
import os
import time

load_dotenv()

client = MongoClient(os.getenv("MONGO_SERVER"))

db = client["US_Airbnb"]          
collection = db["accommodation"]  

# pipeline = [
#     {
#         "$lookup": {
#             "from": "review",        
#             "localField": "reviewId",   
#             "foreignField": "_id",
#             "as": "reviews"
#         }
#     },
#     {
#         "$match": {
#             "room_type": {"$ne": None},
#             "reviews": {"$ne": []}
#         }
#     },
#     {
#         "$addFields": {
#             "number_of_reviews": {"$size": "$reviews"}
#         }
#     },
#     {
#         "$group": {
#             "_id": "$room_type",
#             "total_bookings": {"$sum": "$number_of_reviews"}
#         }
#     },
#     {
#         "$sort": {"total_bookings": -1}
#     },
#     {
#         "$limit": 1
#     }
# ]

pipeline = [
    {
        "$match": {
            "price": {"$lte": 300},
            "minimum_nights": {"$gte": 2}
        }
    },
    {
        "$lookup": {
            "from": "review",
            "localField": "reviewId",
            "foreignField": "_id",
            "as": "review"
        }
    },
    {
        "$lookup": {
            "from": "host",
            "localField": "id",
            "foreignField": "id",
            "as": "host"
        }
    },
    {
        "$lookup": {
            "from": "location",
            "localField": "locationId",
            "foreignField": "_id",
            "as": "location"
        }
    },
    { "$unwind": "$review" },
    { "$unwind": "$host" },
    { "$unwind": "$location" },
    {
        "$group": {
            "_id": "$location.neighbourhood",
            "total_reviews": { "$sum": "$review.number_of_reviews" }
        }
    },
    {
        "$project": {
            "_id": 0,
            "neighborhood": "$_id",
            "total_reviews": 1
        }
    },
    { "$sort": { "total_reviews": -1 } },
    { "$limit": 10 }
]

start_time = time.time()
result = list(collection.aggregate(pipeline))
end_time = time.time()

print(result)
print(f"Query executed in {end_time - start_time:.4f} seconds")
