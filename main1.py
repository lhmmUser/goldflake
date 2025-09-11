# main1.py
from fastapi import FastAPI, BackgroundTasks, Request
from fastapi.responses import JSONResponse, StreamingResponse
import io
from datetime import datetime, timezone, timedelta
from typing import Literal
from pydantic import BaseModel

from config import settings
from s3_client import get_s3_client
from db import users_collection, users_collection_yippee
from app1 import run_comfy_workflow_and_send_image, run_comfy_workflow_and_send_image_sf

# --- Timezone setup (works even on Windows without tzdata) ---
try:
    from zoneinfo import ZoneInfo
    IST = ZoneInfo("Asia/Kolkata")
except Exception:
    IST = timezone(timedelta(hours=5, minutes=30), name="IST")

def now_utc_and_ist():
    now_utc = datetime.now(timezone.utc)   # timezone-aware UTC
    now_ist = now_utc.astimezone(IST)      # convert to IST
    return now_utc, now_ist

# --- FastAPI app & S3 ---
app = FastAPI()
s3 = get_s3_client()
S3_BUCKET   = settings.S3_BUCKET_DF
S3_BUCKET_2 = settings.S3_BUCKET_YIPPEE

# --- Models ---
class MarkUploadedPayload(BaseModel):
    room_id: str
    campaign: Literal["darkfantasy", "sunfeast"]

# --- Endpoints ---

@app.post("/chat360/webhook")
async def webhook_df(request: Request, tasks: BackgroundTasks):
    try:
        payload = await request.json()
        print("📩 Received payload (DF):", payload)

        required = ["name", "gender", "archetype", "selfie", "room_id"]
        if not all(field in payload for field in required):
            return JSONResponse({"error": "Missing one or more required fields."}, status_code=400)

        # Time stamps
        now_utc, now_ist = now_utc_and_ist()

        # Background image generation
        tasks.add_task(
            run_comfy_workflow_and_send_image,
            sender=payload["room_id"],
            name=payload["name"],
            gender=payload["gender"],
            final_profile=payload["archetype"],
            image_url=payload["selfie"],
        )

        # Persist request
        user_entry = {
            "name": payload["name"],
            "gender": payload["gender"],
            "archetype": payload["archetype"],
            "image_url": payload["selfie"],
            "room_id": payload["room_id"],
            "time_req_recieved": now_utc,          # UTC (aware)
            "time_req_recieved_ist": now_ist.isoformat(),      # IST (aware)
            "instance": "df_2",
        }
        await users_collection.insert_one(user_entry)
        return {"status": "200 OK"}

    except Exception as e:
        print("❌ Error in /chat360/webhook:", e)
        return JSONResponse(content={"error": "Invalid request"}, status_code=400)


@app.post("/api/yippee")
async def webhook_sf(request: Request, tasks: BackgroundTasks):
    try:
        payload = await request.json()
        print("📩 Received payload (SF):", payload)

        required = ["name", "gender", "archetype", "selfie", "room_id", "age"]
        if not all(field in payload for field in required):
            return JSONResponse({"error": "Missing one or more required fields."}, status_code=400)

        # Time stamps
        now_utc, now_ist = now_utc_and_ist()

        # Background image generation
        tasks.add_task(
            run_comfy_workflow_and_send_image_sf,
            sender=payload["room_id"],
            name=payload["name"],
            gender=payload["gender"],
            final_profile=payload["archetype"],
            image_url=payload["selfie"],
            age=payload["age"],
        )

        # Persist request
        user_entry = {
            "name": payload["name"],
            "gender": payload["gender"],
            "archetype": payload["archetype"],
            "image_url": payload["selfie"],
            "room_id": payload["room_id"],
            "age": payload["age"],
            "time_req_recieved": now_utc,          # UTC (aware)
            "time_req_recieved_ist": now_ist.isoformat(),      # IST (aware)
            "instance": "df_2",
        }
        await users_collection_yippee.insert_one(user_entry)
        return {"status": "200 OK"}

    except Exception as e:
        print("❌ Error in /api/sunfeast:", e)
        return JSONResponse(content={"error": "Invalid request"}, status_code=400)


@app.post("/internal/mark-uploaded")
async def internal_mark_uploaded(p: MarkUploadedPayload):
    coll = users_collection if p.campaign == "darkfantasy" else users_collection_yippee

    # Find the latest doc for this room_id
    doc = await coll.find_one({"room_id": p.room_id}, sort=[("time_req_recieved", -1)])
    if not doc:
        return JSONResponse({"ok": False, "error": "No matching record for room_id"}, status_code=404)

    # Always use aware UTC now + IST now
    t_uploaded_utc, t_uploaded_ist = now_utc_and_ist()

    # Normalize stored request time to aware UTC
    t_req = doc.get("time_req_recieved")
    if t_req is None:
        t_req = t_uploaded_utc
    else:
        if getattr(t_req, "tzinfo", None) is None:
            # historical naive -> treat as UTC
            t_req = t_req.replace(tzinfo=timezone.utc)
        else:
            t_req = t_req.astimezone(timezone.utc)

    total_secs = (t_uploaded_utc - t_req).total_seconds()
    lag_minutes = int(total_secs // 60) if total_secs > 0 else 0
    total_secs_int = int(total_secs)
    await coll.update_one(
        {"_id": doc["_id"]},
        {"$set": {
            "time_image_uploaded": t_uploaded_utc,   # UTC (aware)
            "time_image_saved_ist": t_uploaded_ist.isoformat(),  # IST (aware)
            "lag": total_secs_int,
        }}
    )
    return {"ok": True, "lag": lag_minutes, "room_id": p.room_id}


@app.get("/chat360/image/{room_id}")
async def get_generated_image_df(room_id: str):
    print("get request started for df")

    base_prefix = "chat360/generated/"
    # ✅ Only list keys for this room_id to avoid 1000-key page limit issues
    search_prefix = f"{base_prefix}{room_id}_"

    # --- Log the GET call (same fields as before) ---
    try:
        now_utc, now_ist = now_utc_and_ist()
        latest = await users_collection.find_one(
            {"room_id": room_id},
            sort=[("time_req_recieved", -1)],
        )
        if latest:
            await users_collection.update_one(
                {"_id": latest["_id"]},
                {
                    "$inc": {"get_count": 1},
                    "$push": {
                        "time_get_req": now_utc,      # UTC (aware)
                        "time_get_req_ist": now_ist.isoformat(),  # IST (aware)
                    },
                },
            )
        else:
            await users_collection.insert_one({
                "room_id": room_id,
                "get_count": 1,
                "time_get_req": [now_utc],
                "time_get_req_ist": [now_ist],
            })
    except Exception as e:
        print("⚠️ Failed to log GET request (DF):", e)

    # --- Fetch the latest image from S3 (paginate + newest by LastModified) ---
    try:
        paginator = s3.get_paginator("list_objects_v2")
        latest_obj = None

        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=search_prefix):
            for obj in page.get("Contents", []):
                if latest_obj is None or obj["LastModified"] > latest_obj["LastModified"]:
                    latest_obj = obj

        if not latest_obj:
            return JSONResponse(content={"error": "Image not found"}, status_code=404)

        print("latest image is generated")
        body = s3.get_object(Bucket=S3_BUCKET, Key=latest_obj["Key"])["Body"].read()
        return StreamingResponse(io.BytesIO(body), media_type="image/jpeg")

    except Exception as e:
        print("❌ Error fetching from S3 (DF):", e)
        return JSONResponse(content={"error": "Internal server error"}, status_code=500)

@app.get("/yippee/image/{room_id}")
async def get_generated_image_sf(room_id: str):
    print("get request started for sf")

    # --- Log the GET call ---
    doc_latest = None
    try:
        now_utc, now_ist = now_utc_and_ist()
        doc_latest = await users_collection_yippee.find_one(
            {"room_id": room_id},
            sort=[("time_req_recieved", -1)],
        )
        if doc_latest:
            await users_collection_yippee.update_one(
                {"_id": doc_latest["_id"]},
                {
                    "$inc": {"get_count": 1},
                    "$push": {
                        "time_get_req": now_utc,        # UTC (aware)
                        "time_get_req_ist": now_ist.isoformat(),    # IST (aware)
                    },
                },
            )
        else:
            await users_collection_yippee.insert_one({
                "room_id": room_id,
                "get_count": 1,
                "time_get_req": [now_utc],
                "time_get_req_ist": [now_ist],
                
            })
    except Exception as e:
        print("⚠️ Failed to log GET request (SF):", e)

    

    try:
        for prefix in ("sunfeast/generated/", "chat360/generated/"):
            resp = s3.list_objects_v2(Bucket=S3_BUCKET_2, Prefix=f"{prefix}{room_id}_")
            contents = resp.get("Contents", [])
            if not contents:
                continue

            latest_obj = max(contents, key=lambda o: o["LastModified"])
            key = latest_obj["Key"]
            print(f"✅ Found in {S3_BUCKET_2}/{key}")

            image_data = s3.get_object(Bucket=S3_BUCKET_2, Key=key)["Body"].read()
            return StreamingResponse(io.BytesIO(image_data), media_type="image/jpeg")

        # Nothing found anywhere
        return JSONResponse(content={"error": "Image not found"}, status_code=404)

    except Exception as e:
        print("❌ Error fetching from S3 (SF):", e)
        return JSONResponse(content={"error": "Internal server error"}, status_code=500)


@app.api_route("/health", methods=["GET", "HEAD"])
async def health_check():
    return {"status": "ok"}


@app.api_route("/", methods=["GET", "HEAD"])
def index():
    return {"message": "DarkFantasy backend is live"}
