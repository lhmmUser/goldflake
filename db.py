from motor.motor_asyncio import AsyncIOMotorClient
from config import settings

# DarkFantasy DB
_client_df = AsyncIOMotorClient(settings.MONGO_URL_DF)
_db_df = _client_df["df-db"]
users_collection = _db_df["user-data"]

# Sunfeast DB
_client_sf = AsyncIOMotorClient(settings.MONGO_URL_YIPPEE)
_db_sf = _client_sf["yippee-db"]
users_collection_yippee = _db_sf["user-data"]
