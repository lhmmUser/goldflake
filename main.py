from fastapi import FastAPI, Request, Query, BackgroundTasks
import httpx
import os
import logging
import uuid
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
from typing import Coroutine, Any
from app1 import run_comfy_workflow_and_send_image_goldflake, s3_key
from db import users_collection, users_collection_yippee, users_collection_goldflake
app = FastAPI()
load_dotenv()

ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
PHONE_NUMBER_ID = os.getenv("PHONE_NUMBER_ID")
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN")
SESSIONS: Dict[str, Dict[str, Any]] = {} 
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
S3_GF_BUCKET = os.getenv("S3_GF_BUCKET") or os.environ["S3_GF_BUCKET"]
GRAPH_BASE = "https://graph.facebook.com/v21.0" 

# --- LOGGING CONFIG ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

_s3 = boto3.client("s3", region_name=AWS_REGION)

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

def get_session(phone: str) -> Dict[str, Any]:
    if phone not in SESSIONS:
        SESSIONS[phone] = {
            "stage": "t_and_c",  # next step: T&C (will be set to scene after agree)
            "scene": None,
            "name": None,
            "gender": None,
            "buddy_name": None,
            "buddy_gender": None,
        }
    return SESSIONS[phone]

@app.middleware("http")
async def access_log(request: Request, call_next):
    # Minimal access log for every request (GET + POST)
    body = b""
    try:
        body = await request.body()
    except Exception:
        pass
    logger.info(f"[ACCESS] {request.method} {request.url.path} | headers={dict(request.headers)} | body={body[:500]}")
    resp = await call_next(request)
    logger.info(f"[ACCESS] {request.method} {request.url.path} -> {resp.status_code}")
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


# --- SEND BUTTON MESSAGE ---
async def send_terms_and_conditions_question(to_phone: str):

    url = f"https://graph.facebook.com/v20.0/{PHONE_NUMBER_ID}/messages"
    body_text = (
        "Welcome to the world of Snow Flake 🔮– where every bite is a portal to your wildest & spookiest imaginations\n\n"
        "👩‍🎤 Find your Halloween Fantasy avatar and stand a chance to win Amazon vouchers worth Rs. 10,000 every week!🏆\n"
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


def call_goldflake(room_id: str, user_gender: str, scene: str,
                   user_file_url: str, buddy_file_url: str) -> Optional[bytes]:
    """
    Calls your run_comfy_workflow_and_send_image_goldflake with the correct mapping.
    Returns image bytes (if your function returns bytes). If it returns None, we log and return None.
    """
    # Use scene as both archetype and final_profile; adjust if your downstream expects other labels
    try:
        result = run_comfy_workflow_and_send_image_goldflake(
            sender=room_id,
            gender=(user_gender or "").lower(),
            final_profile=scene,                  # used in s3_key(...) inside your function
            person1_input_image=user_file_url,    # user (person1)
            person2_input_image=buddy_file_url,   # buddy (person2)
            archetype=scene                       # 'chai' or 'rooftop'
        )
        return result  # expected to be image bytes per your note
    except Exception as e:
        logger.exception(f"Goldflake run failed: {e}")
        return None


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

        combined_gender_folder = f"{g1}_{g2}"                                 # folder key like male_female
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


# --- RECEIVE WHATSAPP WEBHOOK ---
@app.post("/webhook")
async def receive_webhook(request: Request, background_tasks: BackgroundTasks):
    # ---- Parse payload safely ----
    try:
        data = await request.json()  # parse incoming JSON body
    except Exception as e:
        logger.error(f"Invalid JSON payload: {e}")  # log parse error
        return {"status": "ignored"}                 # return benign response

    # ---- WhatsApp wraps messages under entry[].changes[].value.messages[] ----
    entries = data.get("entry", [])                  # get entry array
    if not entries:                                  # if no entries, nothing to do
        logger.debug("No entries found in webhook payload.")
        return {"status": "ok"}                      # acknowledge

    # ---- Iterate over entries and changes as per WA webhook schema ----
    for entry in entries:
        for change in entry.get("changes", []):
            value = change.get("value", {}) or {}    # extract change.value
            messages = value.get("messages", []) or []  # list of messages; empty for status updates
            if not messages:                         # skip if no actual inbound message
                continue

            # ---- Process each inbound message ----
            for msg in messages:
                from_phone = msg.get("from")         # sender phone id
                if not from_phone:                   # ensure sender exists
                    logger.warning("Message without 'from' field; skipping.")
                    continue

                msg_type = msg.get("type")           # message type: interactive/text/image/etc
                session = get_session(from_phone)    # get/create a per-sender session dict
                logger.info(
                    f"Incoming message from {from_phone} | Type: {msg_type} | Stage: {session['stage']}"
                )

                # =========================
                # BUTTON / INTERACTIVE REPLIES
                # =========================
                if msg_type == "interactive":
                    interactive = msg.get("interactive", {}) or {}          # interactive object
                    button_reply = interactive.get("button_reply")           # only handling button replies here
                    if not button_reply:                                     # list replies can be handled similarly
                        logger.info(f"No button_reply (maybe list reply) from {from_phone}; ignoring.")
                        continue

                    choice_id = button_reply.get("id")                       # chosen button id
                    logger.info(f"[BUTTON] from={from_phone} id={choice_id} stage={session['stage']}")

                    # T&C agree/disagree handling
                    if choice_id == "agree_yes":
                        session["stage"] = "q_scene"                          # advance to scene question
                        background_tasks.add_task(send_text, from_phone, "Great! ✅ Let’s begin.")
                        background_tasks.add_task(send_scene_question, from_phone)  # ask scene
                        continue

                    if choice_id == "agree_no":
                        session["stage"] = "t_and_c"                          # back to T&C stage
                        background_tasks.add_task(
                            send_text,
                            from_phone,
                            "No worries. You can come back anytime to accept and continue 👋",
                        )
                        continue

                    # Scene selection (chai / rooftop)
                    if session["stage"] == "q_scene" and choice_id in {"scene_chai", "scene_rooftop"}:
                        session["scene"] = "chai" if choice_id == "scene_chai" else "rooftop"  # store scene
                        logger.info(f"{from_phone} chose scene={session['scene']}")
                        session["stage"] = "q_name"                           # move to name ask
                        background_tasks.add_task(send_name_question, from_phone)
                        continue

                    # User gender selection
                    if session["stage"] == "q_gender" and choice_id in {"gender_me_male", "gender_me_female"}:
                        session["gender"] = "male" if choice_id == "gender_me_male" else "female"  # store gender
                        logger.info(f"{from_phone} gender={session['gender']}")
                        session["stage"] = "q_buddy_name"                    # ask buddy's name next
                        background_tasks.add_task(send_buddy_name_question, from_phone)
                        continue

                    # Buddy gender selection
                    if session["stage"] == "q_buddy_gender" and choice_id in {"gender_buddy_male", "gender_buddy_female"}:
                        session["buddy_gender"] = "male" if choice_id == "gender_buddy_male" else "female"  # store buddy gender
                        logger.info(f"{from_phone} buddy_gender={session['buddy_gender']}")
                        session["stage"] = "q_user_image"                    # now ask for user's image
                        background_tasks.add_task(
                            send_text, from_phone, "Please send **your photo** now (as an image)."
                        )
                        continue

                    # Unexpected button for current stage → re-prompt appropriately
                    logger.warning(f"Unexpected button id for stage {session['stage']}: {choice_id}")
                    st = session["stage"]
                    if st == "t_and_c":
                        background_tasks.add_task(send_terms_and_conditions_question, from_phone)
                    elif st == "q_scene":
                        background_tasks.add_task(send_scene_question, from_phone)
                    elif st == "q_name":
                        background_tasks.add_task(send_name_question, from_phone)
                    elif st == "q_gender":
                        background_tasks.add_task(send_gender_question, from_phone)
                    elif st == "q_buddy_name":
                        background_tasks.add_task(send_buddy_name_question, from_phone)
                    elif st == "q_buddy_gender":
                        background_tasks.add_task(send_buddy_gender_question, from_phone)
                    else:
                        background_tasks.add_task(send_text, from_phone, "Let’s continue. Please follow the prompts.")
                    continue  # move to next message

                # =========================
                # PLAIN TEXT MESSAGES
                # =========================
                if msg_type == "text":
                    raw_text = (msg.get("text", {}) or {}).get("body", "")    # read text body
                    user_text = raw_text.strip()                              # trim whitespace
                    lower = user_text.lower()                                 # lower-cased
                    logger.info(f"[TEXT] from={from_phone} text='{user_text}' stage={session['stage']}")

                    # Gate: conversation starts with greeting at T&C stage
                    if session["stage"] == "t_and_c":
                        if lower in {"hi", "hey", "hello", "heyy", "hii"}:
                            logger.info(f"Starter detected from {from_phone}. Sending T&C.")
                            background_tasks.add_task(send_terms_and_conditions_question, from_phone)  # send T&C
                        else:
                            logger.info(f"Ignored message before starter from {from_phone}: '{user_text}'")
                        continue

                    # Capture user's name
                    if session["stage"] == "q_name":
                        if 1 <= len(user_text) <= 60:                         # simple validation
                            session["name"] = user_text                       # store name
                            logger.info(f"{from_phone} name='{session['name']}'")
                            session["stage"] = "q_gender"                     # ask for user's gender
                            background_tasks.add_task(send_gender_question, from_phone)
                        else:
                            background_tasks.add_task(
                                send_text, from_phone, "Please send a valid name (1–60 characters)."
                            )
                        continue

                    # Capture buddy's name
                    if session["stage"] == "q_buddy_name":
                        if 1 <= len(user_text) <= 60:                         # simple validation
                            session["buddy_name"] = user_text                 # store buddy name
                            logger.info(f"{from_phone} buddy_name='{session['buddy_name']}'")
                            session["stage"] = "q_buddy_gender"              # ask buddy gender next
                            background_tasks.add_task(send_buddy_gender_question, from_phone)
                        else:
                            background_tasks.add_task(
                                send_text, from_phone, "Please send a valid buddy name (1–60 characters)."
                            )
                        continue

                    # For other unexpected text, gently re-prompt based on stage
                    st = session["stage"]
                    logger.info(f"Unexpected text for stage {st} from {from_phone}; re-prompting.")
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
                    continue  # next message

                # =========================
                # IMAGES (WhatsApp media)
                # =========================
                if msg_type == "image":
                    image_obj = msg.get("image", {}) or {}                   # image object
                    media_id = image_obj.get("id")                           # WhatsApp media_id
                    if not media_id:                                         # ensure media id exists
                        logger.warning(f"Image message without media id from {from_phone}")
                        continue

                    logger.info(f"[IMAGE] from={from_phone} stage={session['stage']} media_id={media_id}")

                    # Ensure a room_id exists in session (used as grouping key in S3)
                    if not session.get("room_id"):
                        session["room_id"] = uuid.uuid4().hex                # 32-char hex id
                        logger.info(f"Generated room_id for {from_phone}: {session['room_id']}")
                    room_id = session["room_id"]                             # cache room id

                    # ---------- Branch 1: Expecting USER image ----------
                    if session["stage"] == "q_user_image":
                        filename = f"{room_id}_p1.jpg"                       # local temp filename
                        try:
                            abs_path = await download_whatsapp_media_to_file(media_id, filename)  # download from WA
                        except Exception as e:
                            logger.exception(f"Failed to download user image: {e}")               # log error
                            background_tasks.add_task(
                                send_text, from_phone, "Couldn’t read that image. Please resend."
                            )
                            continue

                        session["user_image_path"] = abs_path                # store local path for debug/tracking

                        # Upload to S3 and store presigned URL for p1
                        try:
                            key1 = s3_key_for_user_upload(room_id, "p1")     # s3 key: goldflake/user_uploads/{room_id}/p1.jpg
                            upload_file_to_s3(session["user_image_path"], key1, cache_control="private, max-age=31536000")  # put object
                            session["user_image_url"] = presign_get_url(key1, expires=3600)  # presigned HTTPS URL
                            logger.info(f"[S3] Uploaded user image -> s3://{S3_GF_BUCKET}/{key1}")
                        except Exception as e:
                            logger.exception(f"S3 upload/presign failed (user image): {e}")  # log S3 failure
                            background_tasks.add_task(send_text, from_phone, "Upload failed. Please try again.")  # inform user
                            continue

                        # Move the flow to buddy image
                        session["stage"] = "q_buddy_image"                   # expect buddy image next
                        background_tasks.add_task(
                            send_text, from_phone, "Got it. Now please send your **buddy’s photo** (as an image)."
                        )
                        continue  # next message

                    # ---------- Branch 2: Expecting BUDDY image ----------
                    if session["stage"] == "q_buddy_image":
                        filename = f"{room_id}_p2.jpg"                       # local temp filename
                        try:
                            abs_path = await download_whatsapp_media_to_file(media_id, filename)  # download from WA
                        except Exception as e:
                            logger.exception(f"Failed to download buddy image: {e}")               # log error
                            background_tasks.add_task(
                                send_text, from_phone, "Couldn’t read that image. Please resend."
                            )
                            continue

                        session["buddy_image_path"] = abs_path               # store local path for debug/tracking

                        # Upload buddy image to S3 and store presigned URL for p2
                        try:
                            key2 = s3_key_for_user_upload(room_id, "p2")     # s3 key: goldflake/user_uploads/{room_id}/p2.jpg
                            upload_file_to_s3(session["buddy_image_path"], key2, cache_control="private, max-age=31536000")  # put object
                            session["buddy_image_url"] = presign_get_url(key2, expires=3600)  # presigned HTTPS URL
                            logger.info(f"[S3] Uploaded buddy image -> s3://{S3_GF_BUCKET}/{key2}")
                        except Exception as e:
                            logger.exception(f"S3 upload/presign failed (buddy image): {e}")   # log S3 failure
                            background_tasks.add_task(send_text, from_phone, "Upload failed. Please try again.")  # inform user
                            continue

                        # Ensure required metadata exists before we dispatch generation
                        required_keys = ("scene", "gender", "buddy_gender", "user_image_url", "buddy_image_url")
                        missing = [k for k in required_keys if not session.get(k)]  # compute missing fields
                        if missing:
                            logger.error(f"Missing fields before goldflake: {missing} for {from_phone}")  # log missing
                            background_tasks.add_task(
                                send_text,
                                from_phone,
                                "We’re missing some details. Let’s continue where we left off.",
                            )
                            # Jump user back to the correct question based on what's missing
                            if not session.get("scene"):
                                session["stage"] = "q_scene"; background_tasks.add_task(send_scene_question, from_phone)
                            elif not session.get("gender"):
                                session["stage"] = "q_gender"; background_tasks.add_task(send_gender_question, from_phone)
                            elif not session.get("buddy_gender"):
                                session["stage"] = "q_buddy_gender"; background_tasks.add_task(send_buddy_gender_question, from_phone)
                            elif not session.get("user_image_url"):
                                session["stage"] = "q_user_image"; background_tasks.add_task(
                                    send_text, from_phone, "Please send your photo as an image."
                                )
                            else:
                                session["stage"] = "q_buddy_image"; background_tasks.add_task(
                                    send_text, from_phone, "Please send your buddy’s photo."
                                )
                            continue  # wait for user to provide missing info

                        # Build final inputs for generation — use HTTPS S3 URLs (NOT file://)
                        p1_url = session["user_image_url"]                  # S3 presigned URL for user image
                        p2_url = session["buddy_image_url"]                 # S3 presigned URL for buddy image
                        scene = session["scene"]                            # archetype / scene
                        user_gender = session["gender"]                     # person1_gender
                        buddy_gender = session["buddy_gender"]              # person2_gender

                        # Background task to call webhook_goldflake (which triggers ComfyUI)
                        async def _generate_and_send(from_phone: str,
                                                    room_id: str,
                                                    scene: str,
                                                    user_gender: str,
                                                    buddy_gender: str,
                                                    p1_url: str,
                                                    p2_url: str) -> None:
                            """
                            - Tells user we're generating.
                            - Computes the final S3 key up-front.
                            - Runs the generator in a worker thread.
                            - Polls S3 HEAD for the key to appear (upload finished).
                            - Presigns the key and sends the image link to WhatsApp.
                            """
                            try:
                                await send_text(from_phone, "Awesome! Generating your image. This can take a bit…")

                                # 🔸 1) deterministic key (caller controls it)
                                upload_key = s3_key(room_id, scene)  # e.g., chat360/generated/<room_id>_<scene>_<ts>.jpg

                                # 🔸 2) compute gender folder expected by your generator
                                combined_gender_folder = f"{user_gender.lower()}_{buddy_gender.lower()}"
                                if combined_gender_folder not in {"male_male", "male_female", "female_female"}:
                                    await send_text(from_phone, "Invalid gender combination.")
                                    return

                                # 🔸 3) run the heavy sync generator off the event loop
                                result = await asyncio.to_thread(
            _run, room_id, combined_gender_folder, scene, p1_url, p2_url, upload_key
        )

                                # We DO NOT trust/need s3_url from result. We only check status and then S3.
                                if not (isinstance(result, dict) and result.get("success")):
                                    err = (result or {}).get("error", "Unknown error")
                                    logger.error(f"[goldflake] generation failed: {err}")
                                    await send_text(from_phone, "Generation failed. Please try again later.")
                                    return

                                # 🔸 4) poll S3 for existence of the key (complete upload)
                                #     Try up to ~120s (adjust as you like)
                                for _ in range(120):
                                    try:
                                        _s3.head_object(Bucket=S3_GF_BUCKET, Key=upload_key)
                                        break  # found
                                    except Exception:
                                        await asyncio.sleep(1)
                                else:
                                    # never broke => not found within timeout
                                    logger.error(f"[goldflake] timed out waiting for S3 key: {upload_key}")
                                    await send_text(from_phone, "Upload is taking longer than expected. Please try again shortly.")
                                    return

                                # 🔸 5) now it exists -> presign and send
                                s3_url = presign_get_url(upload_key, expires=3600)
                                await send_image_by_link(
                                    to_phone=from_phone,
                                    url=s3_url,
                                    caption=f"{scene.title()} | Tap to view",
                                )
                                # mark session stage elsewhere as 'done' if you keep that state machine

                            except Exception as e:
                                logger.exception(f"[goldflake] Failed to generate/send image: {e}")
                                try:
                                    await send_text(from_phone, "Generation failed. Please try again later.")
                                except Exception:
                                    pass
                        # Queue generation in background; keep main webhook snappy
                        background_tasks.add_task(
                    fire_and_forget,
                    _generate_and_send(from_phone, room_id, scene, user_gender, buddy_gender, p1_url, p2_url)
                )      # schedule async generation
                        session["stage"] = "processing"                      # mark as processing
                        continue  # move on to next message

                    # ---------- Any other stage receiving an image ----------
                    background_tasks.add_task(
                        send_text, from_phone, "We’re expecting a different input. Let’s continue."
                    )
                    if session["stage"] == "q_user_image":
                        background_tasks.add_task(send_text, from_phone, "Please send **your photo** as an image.")
                    elif session["stage"] == "q_buddy_image":
                        background_tasks.add_task(send_text, from_phone, "Please send your **buddy’s photo** as an image.")
                    continue  # next message

                # =========================
                # OTHER MESSAGE TYPES → ignore politely
                # =========================
                logger.info(
                    f"Ignoring non-text, non-interactive, non-image message at stage {session['stage']} from {from_phone}"
                )

    # ---- Finish request quickly; background tasks will keep running ----
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
