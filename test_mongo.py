from pymongo import MongoClient
from dotenv import load_dotenv
import os
from datetime import datetime, timezone
import certifi

# Load environment variables from .env file
load_dotenv()

# Get MongoDB URI and DB name from env
#MONGO_URI = os.getenv("MONGO_URI")
MONGO_URI = "mongodb+srv://haripriya:u82Ks33hD4OAz6tH@user-data.bxrdzzx.mongodb.net/?retryWrites=true&w=majority&appName=user-data"

#DB_NAME = os.getenv("DB_NAME", "df-db")
DB_NAME="df-db"

# Print to confirm what's being used (optional)
print("📡 Connecting to MongoDB...")
print("🔗 URI:", MONGO_URI)
print("📁 DB Name:", DB_NAME)

# Setup client
client = MongoClient(MONGO_URI, tls=True, tlsCAFile=certifi.where())
db = client[DB_NAME]
collection = db["test_connection"]

def check_db_connection():
    try:
        client.admin.command("ping")
        print("✅ Successfully connected to MongoDB!")
        return True
    except Exception as e:
        print(f"❌ Failed to connect to MongoDB: {e}")
        return False

def insert_test_doc():
    try:
        doc = {
            "status": "connected",
            "timestamp": datetime.now(timezone.utc)
        }
        result = collection.insert_one(doc)
        print(f"✅ Inserted test doc with ID: {result.inserted_id}")
    except Exception as e:
        print(f"❌ Failed to insert test doc: {e}")

if __name__ == "__main__":
    if check_db_connection():
        insert_test_doc()
