from fastapi import FastAPI, BackgroundTasks, Request, APIRouter
from app1 import run_comfy_workflow_and_send_image
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.responses import FileResponse
import os
import boto3
import io
from db import users_collection
from datetime import datetime

app = FastAPI()

s3 = boto3.client("s3")
S3_BUCKET = "diffrun-generated"

@app.post("/chat360/webhook")
async def webhook(request: Request, tasks: BackgroundTasks):
    try:
        payload = await request.json()
        print("📩 Received payload:", payload)

        # Validate required fields
        required = ["name", "gender", "archetype", "selfie", "room_id"]
        if not all(field in payload for field in required):
            return {"error": "Missing one or more required fields."}

        # Background task to generate image
        tasks.add_task(
            run_comfy_workflow_and_send_image,
            sender=payload["room_id"],
            name=payload["name"],
            gender=payload["gender"],
            final_profile=payload["archetype"],
            image_url=payload["selfie"]
        )

        user_entry = {
            "name": payload["name"],
            "gender": payload["gender"],
            "archetype":payload["archetype"],
            "image_url":payload["selfie"],
            "room_id":payload["room_id"],
            "time_req_recieved":datetime.utcnow(),
            "instance":"df_2"
        }

        await users_collection.insert_one(user_entry)

        return {"status": "200 OK"}
    except Exception as e:
        print("❌ Error in /chat360/webhook:", e)
        return JSONResponse(content={"error": "Invalid request"}, status_code=400)
    
@app.get("/chat360/image/{room_id}")
async def get_generated_image(room_id: str):
    print("get request started")
    prefix = "chat360/generated/"
    try:
        # List all objects under prefix
        response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    
        if "Contents" not in response:
            return JSONResponse(content={"error": "No files found"}, status_code=404)

        # Match files with room_id
        matched_files = [
            obj for obj in response["Contents"]
            if obj["Key"].startswith(f"{prefix}{room_id}_") and obj["Key"].endswith(".jpg")
        ]

        if not matched_files:
            return JSONResponse(content={"error": "Image not found"}, status_code=404)
        print("✅ Matched files:", len(matched_files), matched_files)

        # Pick the most recent one
        latest = sorted(matched_files, key=lambda x: x["LastModified"], reverse=True)[0]
        print("latest image is generated")
        image_data = s3.get_object(Bucket=S3_BUCKET, Key=latest["Key"])["Body"].read()

        return StreamingResponse(io.BytesIO(image_data), media_type="image/jpeg")

    except Exception as e:
        print("❌ Error fetching from S3:", e)
        return JSONResponse(content={"error": "Internal server error"}, status_code=500)

@app.api_route("/health", methods=["GET", "HEAD"])
async def health_check():
    return {"status": "ok"}

@app.api_route("/", methods=["GET", "HEAD"])
def index():
    return {"message": "DarkFantasy backend is live"}

