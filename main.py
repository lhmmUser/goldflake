from fastapi import FastAPI, Request, Query, BackgroundTasks
import httpx
import os
import logging
import uuid
import time
from dotenv import load_dotenv
from typing import Dict, Any, Optional
from app1 import run_comfy_workflow_and_send_image_goldflake
from gender_normalization import normalize_people_dicts
import mimetypes
import re
from fastapi.responses import JSONResponse, StreamingResponse
from main1 import now_utc_and_ist
from urllib.parse import urlsplit, unquote
from urllib.request import url2pathname
from pathlib import Path
import re
import urllib.request
from io import BytesIO
from PIL import Image
import boto3
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Coroutine, Any, Dict, Any, Optional
from app1 import run_comfy_workflow_and_send_image_goldflake, s3_key
from db import users_collection, users_collection_yippee, users_collection_goldflake
app = FastAPI()
load_dotenv()

ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
PHONE_NUMBER_ID = os.getenv("PHONE_NUMBER_ID")
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN")
SESSION_TTL_SECONDS = 30      # 30 minutes since last activity
SESSION_POST_DONE_GRACE = 20    # after we send final image, keep state for 5 minutes
SESSIONS: Dict[str, Dict[str, Any]] = {} 
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
S3_GF_BUCKET = os.getenv("S3_GF_BUCKET") or os.environ["S3_GF_BUCKET"]
GRAPH_BASE = "https://graph.facebook.com/v21.0" 

# in-memory stores (you may replace with persistent DB later)
_sessions_active: Dict[str, Dict[str, Any]] = {}   # phone_key -> active session dict
_sessions_archived: Dict[str, list] = {}          # phone_key -> list of finished sessions
_session_locks: Dict[str, asyncio.Lock] = {}      # phone_key -> asyncio.Lock to avoid races

# --- LOGGING CONFIG ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

_s3 = boto3.client("s3", region_name=AWS_REGION)

def _now() -> float:
    return time.time()

def _new_session() -> Dict[str, Any]:
    # default fresh session
    return {
        "stage": "t_and_c",
        "scene": None,
        "name": None,
        "gender": None,
        "buddy_name": None,
        "buddy_gender": None,
        "room_id": None,
        "created_at": _now(),
        "updated_at": _now(),
        "expires_at": _now() + SESSION_TTL_SECONDS,
        "status": "active",        # "active" | "done" | "expired"
        "end_reason": None,
    }

def get_session(phone: str) -> Dict[str, Any]:
    s = SESSIONS.get(phone)
    now = _now()
    if not s:
        s = _new_session()
        SESSIONS[phone] = s
        return s

    # Expire if needed
    if s.get("expires_at") and now > s["expires_at"]:
        # drop and recreate
        SESSIONS.pop(phone, None)
        s = _new_session()
        SESSIONS[phone] = s
        return s

    # touch
    s["updated_at"] = now
    # extend TTL while active
    if s.get("status") == "active":
        s["expires_at"] = now + SESSION_TTL_SECONDS
    return s

def end_session(phone: str, reason: str = "completed") -> None:
    """
    Mark session as done, and schedule expiry shortly after.
    This keeps a small grace window so you can read state if needed.
    """
    s = SESSIONS.get(phone)
    if not s:
        return
    now = _now()
    s["status"] = "done"
    s["end_reason"] = reason
    s["updated_at"] = now
    s["expires_at"] = now + SESSION_POST_DONE_GRACE
    s["stage"] = "done"

def reset_session(phone: str) -> Dict[str, Any]:
    """
    Hard reset: delete existing and create a brand new session.
    """
    SESSIONS.pop(phone, None)
    s = _new_session()
    SESSIONS[phone] = s
    return s

def normalize_phone_key(phone: str) -> str:
    """Normalize phone to digits-only to use as dictionary keys consistently."""
    if not phone:
        return ""
    digits = re.sub(r"\D", "", phone)
    return digits

def _get_lock(phone_key: str) -> asyncio.Lock:
    """Get (or create) an asyncio.Lock for this phone to avoid concurrent session races."""
    lock = _session_locks.get(phone_key)
    if lock is None:
        lock = asyncio.Lock()
        _session_locks[phone_key] = lock
    return lock

def now_utc_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()

def now_ist_iso() -> str:
    ist = datetime.now(tz=timezone.utc) + timedelta(hours=5, minutes=30)
    return ist.isoformat()

async def start_new_session(from_phone: str, initial_stage: str = "t_and_c", extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Create a new session for from_phone ONLY at the start of a flow.
    Returns the created session dict (and stores it in _sessions_active).
    """
    phone_key = normalize_phone_key(from_phone)
    async with _get_lock(phone_key):
        # If an active session already exists, we will end it and archive it.
        if phone_key in _sessions_active:
            # archive old active session before starting a fresh one
            old = _sessions_active.pop(phone_key)
            _sessions_archived.setdefault(phone_key, []).append(old)

        room_id = uuid.uuid4().hex
        now_utc = now_utc_iso()
        now_ist = now_ist_iso()
        session = {
            "room_id": room_id,
            "wa_phone": from_phone,
            "phone_key": phone_key,
            "stage": initial_stage,
            "created_at_utc": now_utc,
            "created_at_ist": now_ist,
            "updated_at_utc": now_utc,
            "updated_at_ist": now_ist,
            # placeholders for user fields - fill as you collect them
            "person1_name": None,
            "person1_gender": None,
            "person1_selfie": None,
            "person2_name": None,
            "person2_gender": None,
            "person2_selfie": None,
            "archetype": None,
            "combined_gender": None,
        }
        # merge extra if provided
        if extra:
            session.update(extra)

        _sessions_active[phone_key] = session
        # optional: persist snapshot in DB immediately
        # background_tasks.add_task(save_session_snapshot, phone_key, session)
        return session

def get_active_session(from_phone: str) -> Optional[Dict[str, Any]]:
    """Return the active session (in-memory) for from_phone or None."""
    phone_key = normalize_phone_key(from_phone)
    return _sessions_active.get(phone_key)

async def end_session(from_phone: str, reason: str = "completed") -> Optional[Dict[str, Any]]:
    """
    End the active session for a phone.
    - Moves it to archived list and removes from active.
    - Returns the archived session (or None if none existed).
    """
    phone_key = normalize_phone_key(from_phone)
    async with _get_lock(phone_key):
        session = _sessions_active.pop(phone_key, None)
        if not session:
            return None
        session["ended_at_utc"] = now_utc_iso()
        session["ended_at_ist"] = now_ist_iso()
        session["end_reason"] = reason
        session["updated_at_utc"] = session["ended_at_utc"]
        session["updated_at_ist"] = session["ended_at_ist"]
        _sessions_archived.setdefault(phone_key, []).append(session)
        # optional: persist final snapshot to DB
        # await save_session_snapshot(phone_key, session)  # if save_session_snapshot is async
        return session

# Optional utility: find previous sessions (archived)
def list_archived_sessions(from_phone: str) -> list:
    phone_key = normalize_phone_key(from_phone)
    return _sessions_archived.get(phone_key, []).copy()

def guess_ct(path: str) -> str:
    ct, _ = mimetypes.guess_type(path)
    return ct or "application/octet-stream"

def fire_and_forget(coro: Coroutine[Any, Any, Any]) -> None:
    """
    If called on a thread with a running loop, schedule on that loop.
    If called from a worker thread (no loop), run the coroutine to completion
    by creating a private event loop in that thread.
    """
    try:
        loop = asyncio.get_running_loop()   # works in the main ASGI thread
        loop.create_task(coro)
    except RuntimeError:
        # We're in a worker thread (BackgroundTasks). No loop -> make one.
        asyncio.run(coro)

async def send_image_by_link(to_phone: str, url: str, caption: Optional[str] = None) -> None:
    """
    Sends an image by public/presigned URL. WhatsApp will fetch the URL.
    """
    payload = {
        "messaging_product": "whatsapp",
        "to": to_phone,
        "type": "image",
        "image": {"link": url, **({"caption": caption} if caption else {})},
    }
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.post(f"{GRAPH_BASE}/{PHONE_NUMBER_ID}/messages", headers=headers, json=payload)
        r.raise_for_status()

def s3_key_for_user_upload(room_id: str, which: str) -> str:
    # which = "p1" or "p2"
    return f"goldflake/user_uploads/{room_id}/{which}.jpg"

def upload_file_to_s3(local_path: str, key: str, cache_control: Optional[str] = None) -> None:
    extra = {"ContentType": guess_ct(local_path)}
    if cache_control:
        extra["CacheControl"] = cache_control
    _s3.upload_file(local_path, S3_GF_BUCKET, key, ExtraArgs=extra)

def presign_get_url(key: str, expires: int = 3600) -> str:
    return _s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": S3_GF_BUCKET, "Key": key},
        ExpiresIn=expires,
    )



@app.middleware("http")
async def access_log(request: Request, call_next):
    # Minimal access log for every request (GET + POST)
    body = b""
    try:
        body = await request.body()
    except Exception:
        pass
    # logger.info(f"[ACCESS] {request.method} {request.url.path} | headers={dict(request.headers)} | body={body[:500]}")
    resp = await call_next(request)
    # logger.info(f"[ACCESS] {request.method} {request.url.path} -> {resp.status_code}")
    return resp

@app.get("/root")
def root():
    logger.info("Root check — webhook is live.")
    return {"status": "ok", "message": "Webhook server is running"}


@app.get("/webhook")
def verify_webhook(
    hub_mode: str = Query(..., alias="hub.mode"),
    hub_verify_token: str = Query(..., alias="hub.verify_token"),
    hub_challenge: str = Query(..., alias="hub.challenge")
):
    if hub_mode == "subscribe" and hub_verify_token == VERIFY_TOKEN:
        logger.info("Webhook verified successfully.")
        return int(hub_challenge)
    logger.warning("Unauthorized webhook verification attempt.")
    return {"status": "unauthorized"}

def _combined_gender(g1: str | None, g2: str | None) -> str | None:
    g1 = (g1 or "").strip().lower()
    g2 = (g2 or "").strip().lower()
    if not g1 or not g2:
        return None
    # normalize to male_female if pair is mixed; order male_female when mixed
    if {g1, g2} == {"male", "female"}:
        return "male_female"
    if g1 == "male" and g2 == "male":
        return "male_male"
    if g1 == "female" and g2 == "female":
        return "female_female"
    return None


async def save_session_snapshot(wa_phone: str, session: Dict[str, Any]) -> None:
    """
    Upsert a concise interaction snapshot for analytics/audit.
    - Keyed by room_id, so we keep one evolving record per flow.
    - Safe to call frequently; only updates changed fields.
    """
    # ensure we have a room_id early; generate if missing
    room_id = session.get("room_id") or uuid.uuid4().hex
    session["room_id"] = room_id

    # names & genders
    p1_name = session.get("name")
    p2_name = session.get("buddy_name")
    p1_gender = (session.get("gender") or "").strip().lower() or None
    p2_gender = (session.get("buddy_gender") or "").strip().lower() or None

    # selfies (prefer presigned URLs if present)
    p1_selfie = session.get("user_image_url") or session.get("user_image_path")
    p2_selfie = session.get("buddy_image_url") or session.get("buddy_image_path")

    # archetype/scene
    archetype = (session.get("scene") or None)

    # combined gender if both known
    combined = _combined_gender(p1_gender, p2_gender)

     # final image url & status (may be absent until generation completes)
    final_image = session.get("final_image_url") or session.get("final_image")
    status = session.get("status") or None
    end_reason = session.get("end_reason") or None
    ended_at_utc = session.get("ended_at_utc") or None
    ended_at_ist = session.get("ended_at_ist") or None

    # timestamps (reuse your existing helper)
    now_utc, now_ist = now_utc_and_ist()

    doc_set = {
        "campaign": "goldflake",
        "instance": "df_encrypted",
        "wa_phone": wa_phone,
        "room_id": room_id,
        "person1_name": p1_name,
        "person2_name": p2_name,
        "person1_gender": p1_gender,
        "person2_gender": p2_gender,
        "person1_selfie": p1_selfie,  # can be presigned URL or local path (early stages)
        "person2_selfie": p2_selfie,
        "archetype": archetype,       # 'chai' or 'chai_2' as you set today
        "combined_gender": combined,  # male_male | male_female | female_female | None
        "stage": session.get("stage"),
        # final image + status fields (only None if not set)
        "final_image_url": final_image,
        "status": status,
        "end_reason": end_reason,
        "ended_at_utc": ended_at_utc,
        "ended_at_ist": ended_at_ist,
        # keep both the “request received” (first seen) and rolling updated-at
        "time_req_recieved": session.get("time_req_recieved") or now_utc,
        "time_req_recieved_ist": session.get("time_req_recieved_ist") or now_ist.isoformat(),
        "updated_at_utc": now_utc,
        "updated_at_ist": now_ist.isoformat(),

    }

    # set-on-insert once; update the rest each snapshot
    soi = {
        "created_at_utc": session.get("created_at_utc") or now_utc,
        "created_at_ist": session.get("created_at_ist") or now_ist.isoformat(),
    }

    try:
        await users_collection_goldflake.update_one(
            {"room_id": room_id},
            {"$set": doc_set, "$setOnInsert": soi},
            upsert=True,
        )
        logger.debug(f"[snapshot] upserted room_id={room_id} status={status} final_image={'YES' if final_image else 'NO'}")
    except Exception as e:
        logger.exception(f"[snapshot] upsert failed for room_id={room_id}: {e}")

# --- SEND BUTTON MESSAGE ---
async def send_terms_and_conditions_question(to_phone: str):

    url = f"https://graph.facebook.com/v20.0/{PHONE_NUMBER_ID}/messages"
    body_text = (
        "Welcome to the world of Snow Flake 🔮– where every bite is a portal to your wildest & spookiest imaginations\n\n"
        "👩‍🎤 Find your Fantasy avatar and stand a chance to win Amazon vouchers worth Rs. 10,000 every week!🏆\n"
        "Accept terms and conditions to proceed:"
    )

    payload = {
        "messaging_product": "whatsapp",
        "to": to_phone,
        "type": "interactive",
        "interactive": {
            "type": "button",
            "body": {"text": body_text},
            "action": {
                "buttons": [
                    {"type": "reply", "reply": {"id": "agree_yes", "title": "Yes, I agree"}},
                    {"type": "reply", "reply": {"id": "agree_no", "title": "No, I disagree"}},
                ]
            },
        },
    }

    headers = {"Authorization": f"Bearer {ACCESS_TOKEN}", "Content-Type": "application/json"}

    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(url, headers=headers, json=payload)
        if resp.status_code // 100 != 2:
            logger.error(f"Failed to send T&C message to {to_phone}: {resp.text}")
        else:
            logger.info(f"T&C message sent successfully to {to_phone}")


# --- SEND TEXT MESSAGE ---
async def send_text(to_phone: str, text: str):
    url = f"https://graph.facebook.com/v20.0/{PHONE_NUMBER_ID}/messages"
    payload = {
        "messaging_product": "whatsapp",
        "to": to_phone,
        "type": "text",
        "text": {"body": text},
    }
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN}", "Content-Type": "application/json"}

    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(url, headers=headers, json=payload)
        if resp.status_code // 100 != 2:
            logger.error(f"Failed to send text to {to_phone}: {resp.text}")
        else:
            logger.info(f"Sent text to {to_phone}: {text}")


UPLOAD_DIR = os.environ.get("UPLOAD_DIR", r"D:\kush\goldflake-main\user_uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)

def _safe_name(name: str) -> str:
    return re.sub(r'[\\/:*?"<>|]+', "_", name).strip()

async def http_download_to_file(url: str, filename: str) -> str:
    filename = _safe_name(filename)
    out = os.path.abspath(os.path.join(UPLOAD_DIR, filename))
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        r = await client.get(url)
        r.raise_for_status()
        tmp = out + ".part"
        with open(tmp, "wb") as f:
            for chunk in r.iter_bytes():
                if chunk:
                    f.write(chunk)
        os.replace(tmp, out)
    return out

# Download a WhatsApp media by media_id, save to UPLOAD_DIR/filename, return absolute path
_CONTENT_TYPE_EXT = {
    "image/jpeg": ".jpg",
    "image/jpg": ".jpg",
    "image/png": ".png",
    "image/webp": ".webp",
    "image/gif": ".gif",
}

def _ensure_ext(filename: str, content_type: Optional[str]) -> str:
    """
    If filename has no extension, try to add one from content_type,
    falling back to mimetypes and then '.bin'.
    """
    base, ext = os.path.splitext(filename)
    if ext:
        return filename  # already has an extension

    # Prefer our explicit map
    if content_type:
        ct = content_type.split(";")[0].strip().lower()
        if ct in _CONTENT_TYPE_EXT:
            return base + _CONTENT_TYPE_EXT[ct]
        # fallback to mimetypes
        guess = mimetypes.guess_extension(ct)
        if guess:
            return base + guess

    return base + ".bin"


async def download_whatsapp_media_to_file(media_id: str, filename: str) -> str:
    """
    Downloads WhatsApp media by media_id into UPLOAD_DIR/filename and returns the absolute path.
    NOTE: If you later need a file:// URI, use Path(abs_path).as_uri() (this produces file:///D:/... on Windows).
    """
    # --- Step 0: ensure upload dir exists
    try:
        os.makedirs(UPLOAD_DIR, exist_ok=True)
    except Exception as e:
        raise RuntimeError(f"Failed to create UPLOAD_DIR '{UPLOAD_DIR}': {e}")

    # --- Step 1: get CDN URL for media
    meta_url = f"https://graph.facebook.com/v20.0/{media_id}"
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN}"}

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        r1 = await client.get(meta_url, headers=headers)
        if r1.status_code // 100 != 2:
            raise RuntimeError(f"Failed to fetch media URL for {media_id}: {r1.text}")

        media_json = r1.json()
        direct_url = media_json.get("url")
        if not direct_url:
            raise RuntimeError(f"No media url returned for {media_id}")

        # --- Step 2: download the binary (auth header is required by WhatsApp for the CDN)
        # Stream to file to avoid large memory spikes
        r2 = await client.get(direct_url, headers=headers)
        if r2.status_code // 100 != 2:
            raise RuntimeError(f"Failed to download media {media_id}: {r2.text}")

        # Infer extension if missing
        content_type = r2.headers.get("Content-Type")
        safe_filename = _ensure_ext(filename, content_type)

        # Basic filename sanitization (avoid path traversal / odd chars)
        safe_filename = re.sub(r"[\\/:*?\"<>|]+", "_", safe_filename).strip()
        if not safe_filename:
            safe_filename = f"{media_id}.bin"

        abs_path = os.path.abspath(os.path.join(UPLOAD_DIR, safe_filename))

        # Write to a temp file then atomically move
        tmp_path = abs_path + ".part"
        try:
            with open(tmp_path, "wb") as f:
                # chunked write
                for chunk in r2.iter_bytes():
                    if chunk:
                        f.write(chunk)
            os.replace(tmp_path, abs_path)
        finally:
            # Cleanup partial file if something went wrong mid-write
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass

    logger.info(f"Saved media {media_id} -> {abs_path}")
    return abs_path

# Upload image bytes to WhatsApp to get a media_id we can send back
async def upload_image_bytes_to_whatsapp(jpeg_bytes: bytes) -> str:
    url = f"https://graph.facebook.com/v20.0/{PHONE_NUMBER_ID}/media"
    params = {"messaging_product": "whatsapp"}
    files = {"file": ("result.jpg", jpeg_bytes, "image/jpeg")}
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN}"}
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(url, params=params, headers=headers, files=files)
        if resp.status_code // 100 != 2:
            raise RuntimeError(f"Upload to WhatsApp failed: {resp.text}")
        media_id = resp.json().get("id")
        if not media_id:
            raise RuntimeError("Upload to WhatsApp returned no media id")
        return media_id

# Send an image by media_id on WhatsApp
async def send_image_by_media_id(to_phone: str, media_id: str, caption: Optional[str] = None):
    url = f"https://graph.facebook.com/v20.0/{PHONE_NUMBER_ID}/messages"
    payload = {
        "messaging_product": "whatsapp",
        "to": to_phone,
        "type": "image",
        "image": {"id": media_id, **({"caption": caption} if caption else {})},
    }
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN}", "Content-Type": "application/json"}
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(url, headers=headers, json=payload)
        if resp.status_code // 100 != 2:
            raise RuntimeError(f"Send image failed: {resp.text}")
        logger.info(f"Sent image to {to_phone} media_id={media_id}")

# Thin wrapper: translate collected fields to your goldflake function


# def call_goldflake(room_id: str, user_gender: str, scene: str,
#                    user_file_url: str, buddy_file_url: str) -> Optional[bytes]:
#     """
#     Calls your run_comfy_workflow_and_send_image_goldflake with the correct mapping.
#     Returns image bytes (if your function returns bytes). If it returns None, we log and return None.
#     """
#     # Use scene as both archetype and final_profile; adjust if your downstream expects other labels
#     try:
#         result = run_comfy_workflow_and_send_image_goldflake(
#             sender=room_id,
#             gender=(user_gender or "").lower(),
#             final_profile=scene,                  # used in s3_key(...) inside your function
#             person1_input_image=user_file_url,    # user (person1)
#             person2_input_image=buddy_file_url,   # buddy (person2)
#             archetype=scene                       # 'chai' or 'rooftop'
#         )
#         return result  # expected to be image bytes per your note
#     except Exception as e:
#         logger.exception(f"Goldflake run failed: {e}")
#         return None


def _looks_http(s: str) -> bool:
    s = (s or "").lower()
    return s.startswith("http://") or s.startswith("https://")

def _looks_media_id(s: str) -> bool:
    # WhatsApp media_id are numeric-ish strings; this is a permissive check.
    return bool(s) and not _looks_http(s) and not s.lower().startswith("file://") and not re.match(r"^[A-Za-z]:[\\/]", s)

async def _ensure_local(room_id: str, label: str, value: str) -> str:

    filename = f"{room_id}_{label}.jpg"
    # 1) WhatsApp media_id
    if _looks_media_id(value):
        return await download_whatsapp_media_to_file(value, filename)
    # 2) HTTP(S) URL
    if _looks_http(value):
        # Some WA CDN URLs require Authorization: Bearer; if needed, tweak http_download_to_file to accept headers
        return await http_download_to_file(value, filename)

    # Anything else (file://..., local path) is not supported anymore as per our new S3-only policy.
    raise RuntimeError(f"Unsupported input for {label}: must be media_id or http(s) URL.")

def _run(room_id: str,
                combined_gender_folder: str,
                scene: str,
                p1_url: str,
                p2_url: str,
                upload_key: str) -> dict:
            """
            Runs the generator synchronously (in a worker thread) using a deterministic upload_key.
            Returns the dict from run_comfy_workflow_and_send_image_goldflake.
            """
            try:
                return run_comfy_workflow_and_send_image_goldflake(
                    sender=room_id,
                    gender=combined_gender_folder,     # e.g., "male_female"
                    final_profile=scene,               # "chai" or "rooftop"
                    person1_input_image=p1_url,
                    person2_input_image=p2_url,
                    archetype=scene,
                    upload_key=upload_key,             # 🔸 deterministic key chosen by caller
                )
            except Exception as e:
                logger.exception(f"[goldflake] run_comfy_workflow error: {e}")
                return {"success": False, "error": f"run_comfy_workflow error: {e}"}

async def webhook_goldflake(
    room_id: str,
    archetype: str,
    person1_gender: str,
    person2_gender: str,
    person1_selfie: str,
    person2_selfie: str,
    tasks: Optional[BackgroundTasks] = None,   # optional; if provided we schedule, else run inline
) -> Dict[str, Any]:
    """
    Validates inputs, normalizes genders/images, records the request,
    and dispatches the ComfyUI workflow via run_comfy_workflow_and_send_image_goldflake.

    Behavior:
      - If person*_selfie are already http(s) (e.g., S3 presigned URLs), pass through.
      - Else (e.g., WA media_id) -> download locally, upload to S3, presign, pass the URLs.
    """
    # --- small helpers (local to keep your module clean) ---
    def _looks_http(s: str) -> bool:
        s = (s or "").lower().strip()
        return s.startswith("http://") or s.startswith("https://")

    try:
        # ---- Validate required fields ----
        required = {
            "room_id": room_id,
            "archetype": archetype,
            "person1_gender": person1_gender,
            "person2_gender": person2_gender,
            "person1_selfie": person1_selfie,
            "person2_selfie": person2_selfie,
        }
        missing = [k for k, v in required.items() if not v]
        if missing:
            err = f"Missing required fields: {', '.join(missing)}"
            logger.error(f"[goldflake] {err}")
            return {"error": err}

        # ---- Normalize people dictionaries (uses your existing helper) ----
        person1 = {"gender": person1_gender, "image_url": person1_selfie}     # pack p1 for normalizer
        person2 = {"gender": person2_gender, "image_url": person2_selfie}     # pack p2 for normalizer

        norm_p1, norm_p2, swapped, err = normalize_people_dicts(person1, person2)  # your helper
        if err:
            logger.error(f"[goldflake] normalize error: {err}")
            return {"error": err}

        g1 = (norm_p1["gender"] or "").strip().lower()                        # normalized p1 gender
        g2 = (norm_p2["gender"] or "").strip().lower()                        # normalized p2 gender
        p1_in = norm_p1["image_url"]                                          # normalized p1 selfie (raw input shape)
        p2_in = norm_p2["image_url"]                                          # normalized p2 selfie (raw input shape)
        print (g1,g2)
        if g1 == "female" and g2 == "male":
            g1, g2 =g2, g1
            p1_selfie, p2_selfie = p2_selfie, p1_selfie
            combined_gender_folder = f"{g1}_{g2}"
            print ("exchanged",combined_gender_folder)
        else:
            combined_gender_folder = f"{g1}_{g2}"                                         # normalized p2 selfie (raw input shape)
            print ("not exchanged",combined_gender_folder)
                                       # folder key like male_female
        if combined_gender_folder not in {"male_male", "male_female", "female_female"}:
            err = f"Invalid gender combination: {combined_gender_folder}"
            logger.error(f"[goldflake] {err}")
            return {"error": err}

        archetype_norm = (archetype or "chai").strip().lower()                # scene/archetype normalized

        # ---- Record request metadata (non-blocking if it fails) ----
        now_utc, now_ist = now_utc_and_ist()                                  # your time helper
        doc = {
            "room_id": room_id,
            "campaign": "goldflake",
            "instance": "df_encrypted",
            "archetype": archetype_norm,
            "person1_gender": g1,
            "person2_gender": g2,
            "person1_selfie": p1_in,  # store what we received (url or media_id)
            "person2_selfie": p2_in,
            "gender": combined_gender_folder,
            "time_req_recieved": now_utc,
            "time_req_recieved_ist": now_ist.isoformat(),
        }
        try:
            await users_collection_goldflake.insert_one(doc)                   # best-effort log
        except Exception as e:
            logger.exception(f"[goldflake] failed to insert doc: {e}")

        # ---- Resolve inputs to final HTTP(S) URLs (prefer pass-through if already HTTP) ----
        p1_url: Optional[str] = None                                           # final URL for p1
        p2_url: Optional[str] = None                                           # final URL for p2
        tmp_local_paths: list[str] = []                                        # for later cleanup

        # If both are already HTTP(S) (e.g., S3 presigned URLs from /webhook), just pass through
        if _looks_http(p1_in) and _looks_http(p2_in):
            p1_url = p1_in                                                     # use as-is
            p2_url = p2_in                                                     # use as-is
            logger.info("[goldflake] selfie inputs are http(s); skipping local ensure + S3 upload")

        else:
            # Otherwise: resolve each to a LOCAL file (supports media_id or http), then upload to S3, presign
            try:
                # _ensure_local is your existing helper that should:
                #  - treat non-HTTP input as WhatsApp media_id -> download to UPLOAD_DIR
                #  - allow direct HTTP URLs as well -> download to UPLOAD_DIR
                p1_local = await _ensure_local(room_id, "p1", p1_in)           # absolute local path
                p2_local = await _ensure_local(room_id, "p2", p2_in)           # absolute local path
                tmp_local_paths.extend([p1_local, p2_local])                   # for cleanup
            except Exception as e:
                logger.exception(f"[goldflake] failed to resolve inputs to local files: {e}")
                return {"error": "Could not fetch user images"}

            try:
                # Upload both to S3 under stable keys and presign short-lived GET URLs
                key1 = s3_key_for_user_upload(room_id, "p1")                   # goldflake/user_uploads/{room_id}/p1.jpg
                key2 = s3_key_for_user_upload(room_id, "p2")                   # goldflake/user_uploads/{room_id}/p2.jpg

                upload_file_to_s3(p1_local, key1, cache_control="private, max-age=31536000")
                upload_file_to_s3(p2_local, key2, cache_control="private, max-age=31536000")

                p1_url = presign_get_url(key1, expires=3600)                   # presigned HTTPS URL
                p2_url = presign_get_url(key2, expires=3600)                   # presigned HTTPS URL

                logger.info(f"[goldflake] S3 ready: s3://{S3_GF_BUCKET}/{key1} ; s3://{S3_GF_BUCKET}/{key2}")
            except Exception as e:
                logger.exception(f"[goldflake] S3 upload/presign failed: {e}")
                return {"error": "Upload to S3 failed"}
            finally:
                # Best-effort cleanup of temp local files to save disk
                for _p in tmp_local_paths:
                    try:
                        os.remove(_p)
                        logger.debug(f"[goldflake] cleaned temp file: {_p}")
                    except Exception:
                        pass

        # Safety: ensure we have URLs at this point
        if not p1_url or not p2_url:
            logger.error("[goldflake] internal error: missing p1_url/p2_url after resolution")
            return {"error": "Internal image resolution failure"}

        # ---- Dispatch the ComfyUI workflow (background if tasks provided) ----
        def _run(room_id: str,
                combined_gender_folder: str,
                scene: str,
                p1_url: str,
                p2_url: str,
                upload_key: str) -> dict:
            """
            Runs the generator synchronously (in a worker thread) using a deterministic upload_key.
            Returns the dict from run_comfy_workflow_and_send_image_goldflake.
            """
            try:
                return run_comfy_workflow_and_send_image_goldflake(
                    sender=room_id,
                    gender=combined_gender_folder,     # e.g., "male_female"
                    final_profile=scene,               # "chai" or "rooftop"
                    person1_input_image=p1_url,
                    person2_input_image=p2_url,
                    archetype=scene,
                    upload_key=upload_key,             # 🔸 deterministic key chosen by caller
                )
            except Exception as e:
                logger.exception(f"[goldflake] run_comfy_workflow error: {e}")
                return {"success": False, "error": f"run_comfy_workflow error: {e}"}

        if tasks:
            tasks.add_task(_run)                                              # schedule background
            logger.info(f"[goldflake] queued run for room_id={room_id} archetype={archetype_norm}")
        else:
            _run()                                                            # run inline
            logger.info(f"[goldflake] ran immediately for room_id={room_id} archetype={archetype_norm}")

        return {"status": "200 OK"}                                           # final API response

    except Exception as e:
        logger.exception(f"[goldflake] webhook_goldflake error: {e}")         # top-level guard
        return {"error": "Invalid request"}                                    # safe error

async def send_restart_button(to_phone: str):
    url = f"https://graph.facebook.com/v20.0/{PHONE_NUMBER_ID}/messages"
    payload = {
        "messaging_product": "whatsapp",
        "to": to_phone,
        "type": "interactive",
        "interactive": {
            "type": "button",
            "body": {"text": "Start over?"},
            "action": {
                "buttons": [
                    {"type": "reply", "reply": {"id": "restart_flow", "title": "Restart"}},
                ]
            },
        },
    }
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN}", "Content-Type": "application/json"}
    async with httpx.AsyncClient(timeout=15) as client:
        await client.post(url, headers=headers, json=payload)

# --- RECEIVE WHATSAPP WEBHOOK ---
@app.post("/webhook")
async def receive_webhook(request: Request, background_tasks: BackgroundTasks):
    # ---- Parse payload safely ----
    try:
        data = await request.json()  # parse incoming JSON body
    except Exception as e:
        logger.error(f"Invalid JSON payload: {e}")
        return {"status": "ignored"}

    # ---- WhatsApp wraps messages under entry[].changes[].value.messages[] ----
    entries = data.get("entry", []) or []
    if not entries:
        logger.debug("No entries found in webhook payload.")
        return {"status": "ok"}

    # ---- Iterate entries / changes ----
    for entry in entries:
        for change in entry.get("changes", []) or []:
            value = change.get("value", {}) or {}
            messages = value.get("messages", []) or []
            if not messages:
                continue

            for msg in messages:
                from_phone = msg.get("from")
                if not from_phone:
                    logger.warning("Message without 'from' field; skipping.")
                    continue

                # Try to fetch the active session for this phone (do NOT auto-create here)
                session = get_active_session(from_phone)
                # For debugging: show whether we found one
                logger.debug(f"[webhook] incoming from={from_phone} active_session_exists={bool(session)}")

                msg_type = msg.get("type")
                # If we still don't have a session, create a minimal t_and_c session only when we need to
                # (e.g. user sent greeting). For everything else, ask user to start.
                choice_id = None
                raw_text = None
                lower = None

                if msg_type == "interactive":
                    interactive = (msg.get("interactive") or {})
                    btn = interactive.get("button_reply")
                    if btn:
                        choice_id = btn.get("id")

                elif msg_type == "text":
                    raw_text = ((msg.get("text") or {}).get("body") or "")
                    lower = raw_text.strip().lower()

                # === GLOBAL: quick restart via button / keywords ===
                if choice_id == "restart_flow" or (lower in {"restart", "reset", "start over", "startover", "hi"} if lower is not None else False):
                    logger.info(f"[RESTART] by {from_phone} (had_active={bool(session)})")
                    # End any active session and start a fresh one
                    try:
                        await end_session(from_phone, reason="user_restart")
                    except Exception:
                        # ignore errors ending previous session
                        pass
                    session = await start_new_session(from_phone, initial_stage="t_and_c")
                    # snapshot and send T&C
                    background_tasks.add_task(save_session_snapshot, from_phone, session)
                    background_tasks.add_task(send_terms_and_conditions_question, from_phone)
                    continue

                # If there is no active session, only accept greetings/text that should start one,
                # otherwise ask user to start explicitly.
                if not session:
                    if msg_type == "text" and lower in {"hi", "hello", "hey", "hii"}:
                        # create new session at t_and_c and prompt T&C
                        session = await start_new_session(from_phone, initial_stage="t_and_c")
                        background_tasks.add_task(save_session_snapshot, from_phone, session)
                        background_tasks.add_task(send_terms_and_conditions_question, from_phone)
                        continue
                    else:
                        # Polite instruction — ask user to start
                        background_tasks.add_task(send_text, from_phone, "To begin, reply *start* or say *hi*.")
                        continue

                # At this point we have an active session object
                logger.info(f"Incoming message from {from_phone} | Type: {msg_type} | Stage: {session.get('stage')}")

                # If session marked done -> nudge restart
                if session.get("status") == "done":
                    background_tasks.add_task(send_text, from_phone, "This session is finished. Reply *restart* to begin again.")
                    background_tasks.add_task(send_restart_button, from_phone)
                    continue

                # =========================
                # INTERACTIVE BUTTONS (stage-specific)
                # =========================
                if msg_type == "interactive" and choice_id:
                    st = session.get("stage")

                    if choice_id == "agree_yes":
                        # Ensure we have an active session (should be) and move forward
                        session["stage"] = "q_scene"
                        session["updated_at_utc"] = now_utc_iso()
                        session["updated_at_ist"] = now_ist_iso()
                        background_tasks.add_task(save_session_snapshot, from_phone, session)
                        background_tasks.add_task(send_text, from_phone, "Great! ✅ Let’s begin.")
                        background_tasks.add_task(send_scene_question, from_phone)
                        continue

                    if choice_id == "agree_no":
                        session["stage"] = "t_and_c"
                        session["updated_at_utc"] = now_utc_iso()
                        session["updated_at_ist"] = now_ist_iso()
                        background_tasks.add_task(save_session_snapshot, from_phone, session)
                        background_tasks.add_task(send_text, from_phone, "No worries. You can come back anytime to accept and continue 👋")
                        continue

                    if st == "q_scene" and choice_id in {"scene_chai", "scene_rooftop"}:
                        session["scene"] = "chai" if choice_id == "scene_chai" else "chai_2"
                        session["stage"] = "q_name"
                        session["updated_at_utc"] = now_utc_iso()
                        session["updated_at_ist"] = now_ist_iso()
                        background_tasks.add_task(save_session_snapshot, from_phone, session)
                        background_tasks.add_task(send_name_question, from_phone)
                        continue

                    if st == "q_gender" and choice_id in {"gender_me_male", "gender_me_female"}:
                        session["gender"] = "male" if choice_id == "gender_me_male" else "female"
                        session["stage"] = "q_buddy_name"
                        session["updated_at_utc"] = now_utc_iso()
                        session["updated_at_ist"] = now_ist_iso()
                        background_tasks.add_task(save_session_snapshot, from_phone, session)
                        background_tasks.add_task(send_buddy_name_question, from_phone)
                        continue

                    if st == "q_buddy_gender" and choice_id in {"gender_buddy_male", "gender_buddy_female"}:
                        session["buddy_gender"] = "male" if choice_id == "gender_buddy_male" else "female"
                        session["stage"] = "q_user_image"
                        session["updated_at_utc"] = now_utc_iso()
                        session["updated_at_ist"] = now_ist_iso()
                        background_tasks.add_task(save_session_snapshot, from_phone, session)
                        background_tasks.add_task(send_text, from_phone, "Please send **your photo** now (as an image).")
                        continue

                    # fallback for unexpected interactive button
                    background_tasks.add_task(send_text, from_phone, "Let’s continue. Please follow the prompts.")
                    continue

                # =========================
                # TEXT MESSAGES (stage-specific)
                # =========================
                if msg_type == "text" and lower is not None:
                    st = session.get("stage")

                    if st == "t_and_c" and lower in {"hi", "hello", "hey", "heyy", "hii"}:
                        # If user texts greetings while in t_and_c we re-send T&C
                        background_tasks.add_task(send_terms_and_conditions_question, from_phone)
                        continue

                    if st == "q_name":
                        if 1 <= len(raw_text.strip()) <= 60:
                            session["name"] = raw_text.strip()
                            session["stage"] = "q_gender"
                            session["updated_at_utc"] = now_utc_iso()
                            session["updated_at_ist"] = now_ist_iso()
                            background_tasks.add_task(save_session_snapshot, from_phone, session)
                            background_tasks.add_task(send_gender_question, from_phone)
                        else:
                            background_tasks.add_task(send_text, from_phone, "Please send a valid name (1–60 characters).")
                        continue

                    if st == "q_buddy_name":
                        if 1 <= len(raw_text.strip()) <= 60:
                            session["buddy_name"] = raw_text.strip()
                            session["stage"] = "q_buddy_gender"
                            session["updated_at_utc"] = now_utc_iso()
                            session["updated_at_ist"] = now_ist_iso()
                            background_tasks.add_task(save_session_snapshot, from_phone, session)
                            background_tasks.add_task(send_buddy_gender_question, from_phone)
                        else:
                            background_tasks.add_task(send_text, from_phone, "Please send a valid buddy name (1–60 characters).")
                        continue

                    # generic re-prompts for text messages at wrong input
                    if st == "q_scene":
                        background_tasks.add_task(send_scene_question, from_phone)
                    elif st == "q_gender":
                        background_tasks.add_task(send_gender_question, from_phone)
                    elif st == "q_user_image":
                        background_tasks.add_task(send_text, from_phone, "Please send your photo as an *image*.")
                    elif st == "q_buddy_image":
                        background_tasks.add_task(send_text, from_phone, "Please send your buddy’s photo as an *image*.")
                    elif st == "done":
                        background_tasks.add_task(send_text, from_phone, "You’re all set already. 🙌")
                    else:
                        background_tasks.add_task(send_text, from_phone, "Let’s continue. Please follow the prompts.")
                    continue

                # =========================
                # IMAGE (WhatsApp media)
                # =========================
                if msg_type == "image":
                    image_obj = msg.get("image", {}) or {}
                    media_id = image_obj.get("id")
                    if not media_id:
                        logger.warning(f"Image message without media id from {from_phone}")
                        continue

                    logger.info(f"[IMAGE] from={from_phone} stage={session.get('stage')} media_id={media_id}")

                    # We require an active session with a room_id. Do NOT create room_id here.
                    room_id = session.get("room_id")
                    if not room_id:
                        # session should have been created at start; instruct user to restart flow
                        logger.warning(f"No room_id in active session for {from_phone}; asking user to restart.")
                        background_tasks.add_task(send_text, from_phone, "Please restart the flow by replying *restart* or type *hi* to begin.")
                        background_tasks.add_task(send_restart_button, from_phone)
                        continue

                    # ---------- Branch 1: Expecting USER image ----------
                    if session.get("stage") == "q_user_image":
                        filename = f"{room_id}_p1.jpg"
                        try:
                            abs_path = await download_whatsapp_media_to_file(media_id, filename)
                        except Exception as e:
                            logger.exception(f"Failed to download user image: {e}")
                            background_tasks.add_task(send_text, from_phone, "Couldn’t read that image. Please resend.")
                            continue

                        session["user_image_path"] = abs_path
                        try:
                            key1 = s3_key_for_user_upload(room_id, "p1")
                            upload_file_to_s3(session["user_image_path"], key1, cache_control="private, max-age=31536000")
                            session["user_image_url"] = presign_get_url(key1, expires=3600)
                            session["updated_at_utc"] = now_utc_iso()
                            session["updated_at_ist"] = now_ist_iso()
                            background_tasks.add_task(save_session_snapshot, from_phone, session)
                            logger.info(f"[S3] Uploaded user image -> s3://{S3_GF_BUCKET}/{key1}")
                        except Exception as e:
                            logger.exception(f"S3 upload/presign failed (user image): {e}")
                            background_tasks.add_task(send_text, from_phone, "Upload failed. Please try again.")
                            continue

                        # Move the flow to expect buddy image
                        session["stage"] = "q_buddy_image"
                        session["updated_at_utc"] = now_utc_iso()
                        session["updated_at_ist"] = now_ist_iso()
                        background_tasks.add_task(save_session_snapshot, from_phone, session)
                        background_tasks.add_task(send_text, from_phone, "Got it. Now please send your **buddy’s photo** (as an image).")
                        continue

                    # ---------- Branch 2: Expecting BUDDY image ----------
                    if session.get("stage") == "q_buddy_image":
                        filename = f"{room_id}_p2.jpg"
                        try:
                            abs_path = await download_whatsapp_media_to_file(media_id, filename)
                        except Exception as e:
                            logger.exception(f"Failed to download buddy image: {e}")
                            background_tasks.add_task(send_text, from_phone, "Couldn’t read that image. Please resend.")
                            continue

                        session["buddy_image_path"] = abs_path
                        try:
                            key2 = s3_key_for_user_upload(room_id, "p2")
                            upload_file_to_s3(session["buddy_image_path"], key2, cache_control="private, max-age=31536000")
                            session["buddy_image_url"] = presign_get_url(key2, expires=3600)
                            session["updated_at_utc"] = now_utc_iso()
                            session["updated_at_ist"] = now_ist_iso()
                            background_tasks.add_task(save_session_snapshot, from_phone, session)
                            logger.info(f"[S3] Uploaded buddy image -> s3://{S3_GF_BUCKET}/{key2}")
                        except Exception as e:
                            logger.exception(f"S3 upload/presign failed (buddy image): {e}")
                            background_tasks.add_task(send_text, from_phone, "Upload failed. Please try again.")
                            continue

                        # Validate required fields before generating
                        required_keys = ("scene", "gender", "buddy_gender", "user_image_url", "buddy_image_url")
                        missing = [k for k in required_keys if not session.get(k)]
                        if missing:
                            logger.error(f"Missing fields before goldflake generation: {missing} for {from_phone}")
                            background_tasks.add_task(send_text, from_phone, "We’re missing some details. Let’s continue where we left off.")
                            # guide user to provide missing fields
                            if not session.get("scene"):
                                session["stage"] = "q_scene"
                                background_tasks.add_task(send_scene_question, from_phone)
                            elif not session.get("gender"):
                                session["stage"] = "q_gender"
                                background_tasks.add_task(send_gender_question, from_phone)
                            elif not session.get("buddy_gender"):
                                session["stage"] = "q_buddy_gender"
                                background_tasks.add_task(send_buddy_gender_question, from_phone)
                            elif not session.get("user_image_url"):
                                session["stage"] = "q_user_image"
                                background_tasks.add_task(send_text, from_phone, "Please send your photo as an image.")
                            else:
                                session["stage"] = "q_buddy_image"
                                background_tasks.add_task(send_text, from_phone, "Please send your buddy’s photo.")
                            background_tasks.add_task(save_session_snapshot, from_phone, session)
                            continue

                        # Build final inputs
                        p1_url = session["user_image_url"]
                        p2_url = session["buddy_image_url"]
                        scene = session["scene"]
                        user_gender = session["gender"]
                        buddy_gender = session["buddy_gender"]

                        # Background worker — queue generation
                        async def _generate_and_send(from_phone: str,
                                                    room_id: str,
                                                    scene: str,
                                                    user_gender: str,
                                                    buddy_gender: str,
                                                    p1_url: str,
                                                    p2_url: str) -> None:
                            """
                            Worker to run generator, poll S3, send final image and persist final session state.
                            Important: schedule DB writes with asyncio.create_task(...) instead of awaiting them
                                    to avoid 'Future attached to a different loop' errors.
                            """
                            try:
                                await send_text(from_phone, "Awesome! Generating your image. This can take a bit…")

                                upload_key = s3_key(room_id, scene)

                                g1 = (user_gender or "").lower().strip()
                                g2 = (buddy_gender or "").lower().strip()
                                if g1 == "female" and g2 == "male":
                                    g1, g2 = g2, g1
                                    p1_url, p2_url = p2_url, p1_url
                                    logger.debug("Swapped p1/p2 because generator expects male_first ordering.")
                                combined_gender_folder = f"{g1}_{g2}"

                                if combined_gender_folder not in {"male_male", "male_female", "female_female"}:
                                    await send_text(from_phone, "Invalid gender combination.")
                                    return

                                # Run heavy sync work off the event loop
                                result = await asyncio.to_thread(
                                    _run, room_id, combined_gender_folder, scene, p1_url, p2_url, upload_key
                                )

                                if not (isinstance(result, dict) and result.get("success")):
                                    err = (result or {}).get("error", "Unknown error")
                                    logger.error(f"[goldflake] generation failed: {err}")
                                    await send_text(from_phone, "Generation failed. Please try again later.")
                                    return

                                # Poll S3 for the result
                                for _ in range(120):
                                    try:
                                        _s3.head_object(Bucket=S3_GF_BUCKET, Key=upload_key)
                                        break
                                    except Exception:
                                        await asyncio.sleep(1)
                                else:
                                    logger.error(f"[goldflake] timed out waiting for S3 key: {upload_key}")
                                    await send_text(from_phone, "Upload is taking longer than expected. Please try again shortly.")
                                    return

                                # Presign and send final image
                                s3_url = presign_get_url(upload_key, expires=3600)
                                await send_image_by_link(
                                    to_phone=from_phone,
                                    url=s3_url,
                                    caption=f"{scene.title()} | Tap to view",
                                )

                                # --- Persist final image URL and mark session done ---
                                try:
                                    session = get_active_session(from_phone)
                                    if session and session.get("room_id") == room_id:
                                        session["final_image_url"] = s3_url
                                        session["status"] = "done"
                                        session["stage"] = "done"
                                        session["updated_at_utc"] = now_utc_iso()
                                        session["updated_at_ist"] = now_ist_iso()

                                        # schedule snapshot write on the running loop (non-blocking)
                                        try:
                                            asyncio.create_task(save_session_snapshot(from_phone, session))
                                        except Exception as e:
                                            logger.exception(f"[snapshot] failed to schedule save_session_snapshot (active): {e}")
                                    else:
                                        snap = {
                                            "room_id": room_id,
                                            "wa_phone": from_phone,
                                            "final_image_url": s3_url,
                                            "status": "done",
                                            "stage": "done",
                                            "updated_at_utc": now_utc_iso(),
                                            "updated_at_ist": now_ist_iso(),
                                        }
                                        try:
                                            asyncio.create_task(save_session_snapshot(from_phone, snap))
                                        except Exception as e:
                                            logger.exception(f"[snapshot] failed to schedule save_session_snapshot (minimal): {e}")
                                except Exception as e:
                                    logger.exception(f"[goldflake] Exception while preparing final snapshot: {e}")

                                # End the session (archive in-memory). await is OK here.
                                try:
                                    archived = await end_session(from_phone, reason="completed_success")
                                    # persist archived session too (scheduled, non-blocking)
                                    if archived:
                                        archived["final_image_url"] = archived.get("final_image_url", s3_url)
                                        archived["status"] = "done"
                                        archived["stage"] = "done"
                                        try:
                                            asyncio.create_task(save_session_snapshot(from_phone, archived))
                                        except Exception as e:
                                            logger.exception(f"[snapshot] failed to schedule save_session_snapshot (archived): {e}")
                                except Exception as e:
                                    logger.exception(f"[goldflake] Failed to end session after generation: {e}")

                            except Exception as e:
                                logger.exception(f"[goldflake] Failed to generate/send image: {e}")
                                try:
                                    await send_text(from_phone, "Generation failed. Please try again later.")
                                except Exception:
                                    pass

                        # schedule the generation (non-blocking)
                        background_tasks.add_task(
                            fire_and_forget,
                            _generate_and_send(from_phone, room_id, scene, user_gender, buddy_gender, p1_url, p2_url)
                        )

                        session["stage"] = "processing"
                        session["updated_at_utc"] = now_utc_iso()
                        session["updated_at_ist"] = now_ist_iso()
                        background_tasks.add_task(save_session_snapshot, from_phone, session)
                        continue

                    # ---------- Any other stage receiving an image ----------
                    background_tasks.add_task(send_text, from_phone, "We’re expecting a different input. Let’s continue.")
                    if session.get("stage") == "q_user_image":
                        background_tasks.add_task(send_text, from_phone, "Please send **your photo** as an image.")
                    elif session.get("stage") == "q_buddy_image":
                        background_tasks.add_task(send_text, from_phone, "Please send your **buddy’s photo** as an image.")
                    continue

                # =========================
                # OTHER MESSAGE TYPES → ignore politely
                # =========================
                logger.info(f"Ignoring non-text/non-interactive/non-image message at stage {session.get('stage')} from {from_phone}")

    # Finish quickly; background tasks do the heavy lifting
    logger.info("Webhook processing complete.")
    return {"status": "ok"}

# --- Buttons: Scene selection (chai / rooftop) ---
async def send_scene_question(to_phone: str):
    url = f"https://graph.facebook.com/v20.0/{PHONE_NUMBER_ID}/messages"
    payload = {
        "messaging_product": "whatsapp",
        "to": to_phone,
        "type": "interactive",
        "interactive": {
            "type": "button",
            "body": {"text": "Which scene captures your perfect smoke-buddy chill?"},
            "action": {
                "buttons": [
                    {"type": "reply", "reply": {"id": "scene_chai", "title": "Chai"}},
                    {"type": "reply", "reply": {"id": "scene_rooftop", "title": "Rooftop"}},
                ]
            },
        },
    }
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN}", "Content-Type": "application/json"}
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(url, headers=headers, json=payload)
        if resp.status_code // 100 != 2:
            logger.error(f"Failed to send scene question to {to_phone}: {resp.text}")
        else:
            logger.info(f"Scene question sent to {to_phone}")

# --- Ask for name (plain text) ---
async def send_name_question(to_phone: str):
    await send_text(to_phone, "Cool. What’s your name? (Please reply with text)")

# --- Buttons: Your gender (male / female) ---
async def send_gender_question(to_phone: str):
    url = f"https://graph.facebook.com/v20.0/{PHONE_NUMBER_ID}/messages"
    payload = {
        "messaging_product": "whatsapp",
        "to": to_phone,
        "type": "interactive",
        "interactive": {
            "type": "button",
            "body": {"text": "Your gender?"},
            "action": {
                "buttons": [
                    {"type": "reply", "reply": {"id": "gender_me_male", "title": "Male"}},
                    {"type": "reply", "reply": {"id": "gender_me_female", "title": "Female"}},
                ]
            },
        },
    }
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN}", "Content-Type": "application/json"}
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(url, headers=headers, json=payload)
        if resp.status_code // 100 != 2:
            logger.error(f"Failed to send gender question to {to_phone}: {resp.text}")
        else:
            logger.info(f"Gender (self) question sent to {to_phone}")

# --- Ask for buddy name (plain text) ---
async def send_buddy_name_question(to_phone: str):
    await send_text(to_phone, "Your smoke buddy’s name? (Reply with text)")

# --- Buttons: Buddy gender (male / female) ---
async def send_buddy_gender_question(to_phone: str):
    url = f"https://graph.facebook.com/v20.0/{PHONE_NUMBER_ID}/messages"
    payload = {
        "messaging_product": "whatsapp",
        "to": to_phone,
        "type": "interactive",
        "interactive": {
            "type": "button",
            "body": {"text": "Your buddy’s gender?"},
            "action": {
                "buttons": [
                    {"type": "reply", "reply": {"id": "gender_buddy_male", "title": "Male"}},
                    {"type": "reply", "reply": {"id": "gender_buddy_female", "title": "Female"}},
                ]
            },
        },
    }
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN}", "Content-Type": "application/json"}
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(url, headers=headers, json=payload)
        if resp.status_code // 100 != 2:
            logger.error(f"Failed to send buddy gender question to {to_phone}: {resp.text}")
        else:
            logger.info(f"Gender (buddy) question sent to {to_phone}")

async def re_prompt_for_stage(session: Dict[str, Any], to_phone: str, background_tasks: BackgroundTasks):
    stage = session.get("stage")
    logger.info(f"Re-prompting {to_phone} for stage={stage}")
    if stage == "t_and_c":
        background_tasks.add_task(send_terms_and_conditions_question, to_phone)
    elif stage == "q_scene":
        background_tasks.add_task(send_scene_question, to_phone)
    elif stage == "q_name":
        background_tasks.add_task(send_name_question, to_phone)
    elif stage == "q_gender":
        background_tasks.add_task(send_gender_question, to_phone)
    elif stage == "q_buddy_name":
        background_tasks.add_task(send_buddy_name_question, to_phone)
    elif stage == "q_buddy_gender":
        background_tasks.add_task(send_buddy_gender_question, to_phone)
    else:
        background_tasks.add_task(send_text, to_phone, "Let’s continue. Please follow the prompts.")
