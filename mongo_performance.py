from pymongo import MongoClient
from dotenv import load_dotenv
import os
load_dotenv()

client = MongoClient(os.getenv("MONGO_SERVER"))

db = client["US_Airbnb"]          
collection = db["accommodation"]  

pipeline = [
    {
        "$lookup": {
            "from": "review",        
            "localField": "reviewId",   
            "foreignField": "_id",
            "as": "reviews"
        }
    },
    {
        "$match": {
            "room_type": {"$ne": None},
            "reviews": {"$ne": []}
        }
    },
    {
        "$addFields": {
            "number_of_reviews": {"$size": "$reviews"}
        }
    },
    {
        "$group": {
            "_id": "$room_type",
            "total_bookings": {"$sum": "$number_of_reviews"}
        }
    },
    {
        "$sort": {"total_bookings": -1}
    },
    {
        "$limit": 1
    }
]

result = list(collection.aggregate(pipeline))
print(result)