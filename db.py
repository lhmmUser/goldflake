from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime

#MONGO_URL = "mongodb+srv://haripriya:u82Ks33hD4OAz6tH@user-data.bxrdzzx.mongodb.net/?retryWrites=true&w=majority&appName=user-data"
MONGO_URL = "mongodb://haripriya:zdxpx3TWVWZ8zuz5@ac-1srhb4y-shard-00-00.bxrdzzx.mongodb.net:27017,ac-1srhb4y-shard-00-01.bxrdzzx.mongodb.net:27017,ac-1srhb4y-shard-00-02.bxrdzzx.mongodb.net:27017/?ssl=true&replicaSet=atlas-1yr3fd-shard-0&authSource=admin&retryWrites=true&w=majority&appName=user-data"


client = AsyncIOMotorClient(MONGO_URL)
db =client["df-db"]
users_collection = db["user-data"]