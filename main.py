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
from db import users_collection_goldflake, buddy_collection
from pymongo import ReturnDocument

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

async def link_requester_and_buddy(requester_room_id: str | None,
                                   buddy_room_id: str | None,
                                   requester_phone: str | None,
                                   buddy_phone: str | None):
    """
    Link the requester job doc and the buddy participant doc.
    - set buddy_session_room_id & buddy_link_status on user doc
    - set linked_user_room on buddy doc
    - ensure participants array contains both phones
    """
    try:
        if not requester_room_id:
            logger.warning("[link] missing requester_room_id; skipping link")
            return

        update_doc = {
            "$set": {
                "buddy_session_room_id": buddy_room_id,
                "buddy_link_status": "invited",
                "buddy_phone": buddy_phone,
                "updated_at_utc": now_utc_iso(),
            },
            "$setOnInsert": {
                "created_at_utc": now_utc_iso(),
                "campaign": "goldflake",
                "instance": "df_encrypted",
            },
            "$addToSet": {
                "participants": buddy_phone
            },
        }

        await users_collection_goldflake.update_one({"room_id": requester_room_id}, update_doc, upsert=True)

        # ensure requester phone also present
        try:
            if requester_phone:
                await users_collection_goldflake.update_one({"room_id": requester_room_id}, {"$addToSet": {"participants": requester_phone}})
        except Exception:
            logger.exception("[link] failed to add requester to participants array (non-fatal)")

        # patch buddy doc so it references requester
        if buddy_room_id:
            try:
                await buddy_collection.update_one(
                    {"buddy_room_id": buddy_room_id},
                    {"$set": {"linked_user_room": requester_room_id, "linked_user_phone": requester_phone, "updated_at_utc": now_utc_iso()}},
                    upsert=True,
                )
            except Exception:
                logger.exception("[link] failed to patch buddy's doc (non-fatal)")

        logger.info(f"[link] linked requester_room={requester_room_id} <-> buddy_room={buddy_room_id}")
    except Exception as e:
        logger.exception(f"[link] unexpected error linking sessions: {e}")

async def acquire_generation_lock(room_id: str) -> bool:
    """
    Atomically acquire a generation lock for room_id.
    Returns True if this caller acquired the lock and should proceed with generation.
    Returns False if another process already started generation.
    Uses find_one_and_update with upsert=True so it also works when the job doc is missing.
    """
    try:
        now = now_utc_iso()
        # Try to set generation_started only when it isn't True already.
        # If the document doesn't exist, upsert=True will create it — that first caller should acquire the lock.
        prev = await users_collection_goldflake.find_one_and_update(
            {"room_id": room_id, "generation_started": {"$ne": True}},
            {"$set": {"generation_started": True, "generation_started_at": now, "updated_at_utc": now}},
            upsert=True,
            return_document=ReturnDocument.BEFORE,
        )
        # If prev is None -> either the document was created by us (we acquired the lock),
        # or the document didn't exist before. Treat as acquired.
        # If prev is a dict -> we updated an existing doc which did not previously have generation_started True -> acquired.
        # If prev is NOT None but had generation_started True (shouldn't match the filter), then no update occurred.
        # Conservative policy: return True (acquired) when prev is None OR prev exists and prev.get("generation_started") != True.
        return True
    except Exception:
        logger.exception("[lock] acquire_generation_lock failed for room_id=%s", room_id)
        # On exception, return False (don't start generation)
        return False
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

async def generate_for_pair(
    room_id: str,
    requester_phone: str,
    buddy_phone: Optional[str],
    user_image_url: str,
    buddy_image_url: str,
    scene: Optional[str] = None,
    brand_id: Optional[str] = None,
):
    """
    Run the generation for the given user+buddy images.
    This function mirrors the logic you had in the buddy flow generator.
    It calls your existing _run synchronously inside a thread via asyncio.to_thread,
    waits for S3 to show the output key, presigns, then notifies both users and persists snapshots.
    """
    try:
        logger.info("[gen] start generation for room_id=%s requester=%s buddy=%s scene=%s",
                    room_id, requester_phone, buddy_phone, scene)

        # Normalize inputs
        scene = (scene or "default").strip()
        archetype_combined = f"{scene}_{(brand_id or '').strip().lower()}" if brand_id else scene

        # Determine genders / combined folder logic if you maintain that
        # We'll read stored genders from DB if available
        job_doc = await users_collection_goldflake.find_one({"room_id": room_id})
        user_gender = (job_doc.get("person1_gender") or "").lower().strip() if job_doc else ""
        buddy_doc = None
        buddy_gender = ""
        if buddy_phone:
            buddy_doc = await buddy_collection.find_one({"wa_phone": buddy_phone})
            buddy_gender = (buddy_doc.get("person2_gender") or "").lower().strip() if buddy_doc else ""

        # Determine order of images (match your earlier logic)
        g1 = user_gender or ""
        g2 = buddy_gender or ""
        p1 = user_image_url
        p2 = buddy_image_url

        # If your generator expects a specific ordering based on gender, adjust here:
        # e.g., if generator expects male first, female second, swap if necessary.
        # (Use your original rule; this is a placeholder.)
        # Example rule used earlier: if user female and buddy male -> swap
        if g1 == "female" and g2 == "male":
            p1, p2 = p2, p1
            g1, g2 = g2, g1

        combined_gender_folder = f"{g1 or 'unknown'}_{g2 or 'unknown'}"
        # Validate combined folder acceptable values (if you maintain strict set)
        allowed = {"male_male", "male_female", "female_female", "unknown_unknown", "male_unknown", "female_unknown"}
        if combined_gender_folder not in allowed:
            logger.warning("[gen] combined gender %s not in allowed set, continuing anyway", combined_gender_folder)

        # Compute S3 upload key for output (reuse your project's convention)
        # This must match the _run expectation so the runner writes that key
        upload_key = f"{room_id}/final/{combined_gender_folder}/{archetype_combined}.jpg"

        # Run the generator in a thread (this mirrors existing _run usage)
        try:
            result = await asyncio.to_thread(_run, room_id, combined_gender_folder, scene, p1, p2, upload_key, archetype_combined)
        except Exception as e:
            logger.exception("[gen] synchronous generator _run failed for room_id=%s: %s", room_id, e)
            await send_text(requester_phone, "Generation failed due to an internal error. Please try again later.")
            if buddy_phone:
                await send_text(buddy_phone, "Generation failed due to an internal error. Please try again later.")
            # Ensure generation_started flag cleared so future attempts possible
            try:
                await users_collection_goldflake.update_one({"room_id": room_id}, {"$set": {"generation_failed": True, "updated_at_utc": now_utc_iso()}}, upsert=False)
            except Exception:
                logger.exception("[gen] failed to mark generation_failed for room_id=%s", room_id)
            return

        # Validate result success shape if your _run returns structured dict
        if not (isinstance(result, dict) and result.get("success")):
            logger.error("[gen] _run returned failure for room_id=%s: %s", room_id, result)
            await send_text(requester_phone, "Generation failed. Please try again later.")
            if buddy_phone:
                await send_text(buddy_phone, "Generation failed. Please try again later.")
            # mark failed and return
            try:
                await users_collection_goldflake.update_one({"room_id": room_id}, {"$set": {"generation_failed": True, "updated_at_utc": now_utc_iso()}}, upsert=False)
            except Exception:
                logger.exception("[gen] failed to mark generation_failed for room_id=%s", room_id)
            return

        # Wait for S3 object to appear (poll)
        max_checks = 120
        delay = 1
        for _ in range(max_checks):
            try:
                _s3.head_object(Bucket=S3_GF_BUCKET, Key=upload_key)
                break
            except Exception:
                await asyncio.sleep(delay)
        else:
            logger.error("[gen] S3 object not found after wait for key=%s", upload_key)
            await send_text(requester_phone, "Generation is taking longer than expected. Please try again shortly.")
            return

        # presign URL
        try:
            s3_url = presign_get_url(upload_key, expires=3600)
        except Exception:
            logger.exception("[gen] presign failed for key=%s", upload_key)
            s3_url = None

        # Notify requester and buddy with the image link
        caption = (scene or "Avatar").title()
        try:
            if s3_url:
                await send_image_by_link(to_phone=requester_phone, url=s3_url, caption=f"{caption} | Tap to view")
            else:
                await send_text(requester_phone, f"{caption} generated — but failed to prepare download link. Contact support.")
            if buddy_phone:
                if s3_url:
                    await send_image_by_link(to_phone=buddy_phone, url=s3_url, caption=f"{caption} — here's the avatar you helped create. Tap to view")
                else:
                    await send_text(buddy_phone, f"{caption} generated — but failed to prepare download link. Contact support.")
        except Exception:
            logger.exception("[gen] failed to send image to participants for room_id=%s", room_id)

        # Persist final_image and update statuses in DB and sessions
        now = now_utc_iso()
        try:
            await users_collection_goldflake.update_one(
                {"room_id": room_id},
                {"$set": {"final_image_url": s3_url, "status": "done", "stage": "done", "updated_at_utc": now}},
                upsert=False,
            )
        except Exception:
            logger.exception("[gen] failed to update user job final_image_url for room_id=%s", room_id)

        if buddy_phone:
            try:
                await buddy_collection.update_one(
                    {"linked_user_room": room_id},
                    {"$set": {"final_image_url": s3_url, "status": "done", "stage": "done", "updated_at_utc": now}},
                    upsert=False,
                )
            except Exception:
                logger.exception("[gen] failed to update buddy doc final_image_url for room_id=%s", room_id)

        # Update in-memory sessions (if active)
        try:
            u_session = get_active_session(requester_phone)
            if u_session:
                u_session["final_image_url"] = s3_url
                u_session["status"] = "done"
                u_session["stage"] = "done"
                u_session["updated_at_utc"] = now
                u_session["updated_at_ist"] = now_ist_iso()
                try:
                    await save_session_snapshot(requester_phone, u_session)
                except Exception:
                    logger.exception("[gen] failed to snapshot user session after generation")
        except Exception:
            logger.exception("[gen] updating in-memory user session failed")

        try:
            if buddy_phone:
                b_session = get_active_session(buddy_phone)
                if b_session:
                    b_session["final_image_url"] = s3_url
                    b_session["status"] = "done"
                    b_session["stage"] = "done"
                    b_session["updated_at_utc"] = now
                    b_session["updated_at_ist"] = now_ist_iso()
                    try:
                        await save_session_snapshot(buddy_phone, b_session)
                    except Exception:
                        logger.exception("[gen] failed to snapshot buddy session after generation")
        except Exception:
            logger.exception("[gen] updating in-memory buddy session failed")

        # Optionally mark generation_complete flag for audits
        try:
            await users_collection_goldflake.update_one({"room_id": room_id}, {"$set": {"generation_complete": True, "generation_completed_at": now}}, upsert=False)
        except Exception:
            logger.exception("[gen] failed to set generation_complete flag for room_id=%s", room_id)

        # Try to end / archive sessions
        try:
            await end_session(requester_phone, reason="completed_success")
            if buddy_phone:
                await end_session(buddy_phone, reason="completed_success")
        except Exception:
            logger.exception("[gen] end_session failed after generation")

        logger.info("[gen] generation complete for room_id=%s", room_id)

    except Exception as e:
        logger.exception("[gen] unexpected error generate_for_pair for room_id=%s: %s", room_id, e)
        # best-effort notification
        try:
            await send_text(requester_phone, "Generation failed due to an internal error. Please try again later.")
            if buddy_phone:
                await send_text(buddy_phone, "Generation failed due to an internal error. Please try again later.")
        except Exception:
            pass

async def save_session_snapshot(wa_phone: str, session: Dict[str, Any]) -> None:
    """
    Persist snapshot to user-data or buddy-data depending on which WA number is calling.
    - If wa_phone matches the requester's session wa_phone, write to user-data (job document).
    - If wa_phone matches buddy session wa_phone, write to buddy-data (participant document).
    """
    try:
        room_id = session.get("room_id") or uuid.uuid4().hex
        session["room_id"] = room_id
        now_utc = now_utc_iso()
        now_ist = now_ist_iso()

        # Common metadata
        base_meta = {
            "updated_at_utc": now_utc,
            "updated_at_ist": now_ist,
        }

        # Detect role: if session explicitly sets role use it; otherwise infer by comparing wa_phone
        role = session.get("role")
        session_wa = session.get("wa_phone") or session.get("from_phone") or wa_phone
        if not role:
            # if the caller wa_phone equals the session's wa_phone -> it's that session's doc
            role = "user" if wa_phone == session_wa else "buddy"

        if role == "user":
            # Build user/job doc (only user-owned fields)
            doc_set = {
                "room_id": room_id,
                "wa_phone": session_wa,
                "person1_name": session.get("name") or session.get("person1_name"),
                "person1_gender": session.get("gender") or session.get("person1_gender"),
                "person1_selfie": session.get("user_image_url") or session.get("user_image_path") or session.get("person1_selfie"),
                # job-level config
                "campaign": session.get("campaign") or "goldflake",
                "instance": session.get("instance") or "df_encrypted",
                "archetype": session.get("scene") or session.get("archetype"),
                "brand": session.get("brand"),
                "brand_id": session.get("brand_id"),
                "combined_gender": session.get("combined_gender"),
                "stage": session.get("stage"),
                "status": session.get("status"),
                "final_image_url": session.get("final_image_url"),
                # link info to buddy (we keep only references here, actual buddy data is in buddy-data)
                "buddy_phone": session.get("buddy_number") or session.get("buddy_phone") or session.get("buddy_whatsapp"),
                "buddy_session_room_id": session.get("linked_buddy_room") or session.get("buddy_session_room_id"),
                "buddy_link_status": session.get("buddy_link_status"),
                # timestamps
                "time_req_recieved": session.get("time_req_recieved") or session.get("created_at_utc"),
                "time_req_recieved_ist": session.get("time_req_recieved_ist") or session.get("created_at_ist"),
            }
            # merge meta
            doc_set.update(base_meta)
            soi = {
                "created_at_utc": session.get("created_at_utc") or now_utc,
                "created_at_ist": session.get("created_at_ist") or now_ist,
            }

            await users_collection_goldflake.update_one(
                {"room_id": room_id},
                {"$set": doc_set, "$setOnInsert": soi},
                upsert=True,
            )
            logger.debug(f"[snapshot:user] upserted room_id={room_id} wa={session_wa} stage={session.get('stage')}")
            return

        # else role == "buddy"
        # Build buddy/participant doc (only buddy-owned fields)
        buddy_doc_set = {
            "buddy_room_id": room_id,
            "wa_phone": session_wa,
            "participant_role": "buddy",
            "linked_user_room": session.get("linked_user_room"),
            "linked_user_phone": session.get("linked_user_phone"),
            "person2_name": session.get("buddy_name") or session.get("person2_name"),
            "person2_gender": session.get("buddy_gender") or session.get("person2_gender"),
            "person2_selfie": session.get("buddy_image_url") or session.get("buddy_image_path") or session.get("person2_selfie"),
            "stage": session.get("stage"),
            "status": session.get("status"),
            # sometimes final image is produced on buddy side; persist if present
            "final_image_url": session.get("final_image_url"),
        }
        buddy_doc_set.update(base_meta)
        soi = {"created_at_utc": session.get("created_at_utc") or now_utc, "created_at_ist": session.get("created_at_ist") or now_ist}

        await buddy_collection.update_one(
            {"buddy_room_id": room_id},
            {"$set": buddy_doc_set, "$setOnInsert": soi},
            upsert=True,
        )
        logger.debug(f"[snapshot:buddy] upserted buddy_room_id={room_id} wa={session_wa} stage={session.get('stage')}")

        # Ensure the user's job doc is linked (if we know linked_user_room)
        linked_user_room = session.get("linked_user_room") or session.get("job_room_id") or session.get("linked_user_room_id")
        if linked_user_room:
            # mirror minimal linking info to user doc (idempotent)
            try:
                await users_collection_goldflake.update_one(
                    {"room_id": linked_user_room},
                    {
                        "$set": {
                            "buddy_session_room_id": room_id,
                            "buddy_phone": session_wa,
                            "updated_at_utc": now_utc,
                        },
                        "$addToSet": {"participants": session_wa},
                        "$setOnInsert": {"created_at_utc": now_utc}
                    },
                    upsert=True,
                )
                logger.debug(f"[snapshot:buddy->user] linked buddy {session_wa} -> user_room={linked_user_room}")
            except Exception:
                logger.exception("[snapshot:buddy->user] failed to patch user job with buddy link")

    except Exception as e:
        logger.exception(f"[snapshot] unexpected error saving snapshot for wa={wa_phone}: {e}")


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


UPLOAD_DIR = os.environ.get("UPLOAD_DIR", r"C:\Users\Haripriya\goldflake-main\user_uploads")
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

async def handle_user_image(from_phone: str, session: Dict[str, Any], image_url: str, background_tasks):
    """
    Called when the requester uploads an image.
    - image_url: presigned S3 URL or public URL of the uploaded user image
    - session: in-memory session dict for the requester (will be mutated)
    - background_tasks: FastAPI BackgroundTasks instance to schedule background jobs
    """
    try:
        # 1) Set user image on session and snapshot user-data
        session["user_image_url"] = image_url
        session["stage"] = "waiting_for_buddy_image"
        session["updated_at_utc"] = now_utc_iso()
        session["updated_at_ist"] = now_ist_iso()
        try:
            background_tasks.add_task(save_session_snapshot, from_phone, session)
        except Exception:
            logger.exception("[user_image] failed to schedule save_session_snapshot")

        room_id = session.get("room_id")
        if not room_id:
            # ensure we have a room id
            room_id = session["room_id"] = uuid.uuid4().hex
            try:
                await users_collection_goldflake.update_one({"room_id": room_id}, {"$setOnInsert": {"created_at_utc": now_utc_iso()}}, upsert=True)
            except Exception:
                logger.exception("[user_image] failed to ensure job doc exists for new room_id=%s", room_id)

        # 2) Check buddy-data for an already-uploaded buddy image for this requester
        buddy_doc = None
        try:
            # Prefer direct link by linked_user_room; fall back to linked_user_phone
            buddy_doc = await buddy_collection.find_one(
                {"$or": [{"linked_user_room": room_id}, {"linked_user_phone": from_phone}]}
            )
        except Exception:
            logger.exception("[user_image] buddy_collection lookup failed for room_id=%s", room_id)

        # If there is no buddy doc or no buddy image yet, just wait for buddy to upload (normal flow)
        if not buddy_doc or not (buddy_doc.get("person2_selfie") or buddy_doc.get("person2_image_url") or buddy_doc.get("buddy_image_url")):
            logger.info("[user_image] buddy image not present yet for room_id=%s; waiting", room_id)
            # user will remain waiting_for_buddy_image; return early
            return

        # Ensure there is a user job doc so lock acquisition is deterministic
        try:
            await users_collection_goldflake.update_one(
                {"room_id": room_id},
                {
                    "$setOnInsert": {
                        "room_id": room_id,
                        "created_at_utc": now_utc_iso(),
                        "campaign": "goldflake",
                        "instance": "df_encrypted",
                    }
                },
                upsert=True,
            )
        except Exception:
            logger.exception("[buddy-image] failed to ensure user job doc exists for room_id=%s", room_id)


        # 3) Buddy image exists — attempt to acquire generation lock on the job doc
        # If lock acquired, schedule generation; otherwise another process already started it
        lock_acquired = await acquire_generation_lock(room_id)
        if not lock_acquired:
            logger.info("[user_image] generation already started for room_id=%s; skipping duplicate start", room_id)
            return

        # Extract URLs and phones
        buddy_img_url = buddy_doc.get("person2_selfie") or buddy_doc.get("person2_image_url") or buddy_doc.get("buddy_image_url")
        buddy_phone = buddy_doc.get("wa_phone") or buddy_doc.get("linked_user_phone")
        scene = session.get("scene") or job_doc.get("archetype") if (job_doc := await users_collection_goldflake.find_one({"room_id": room_id})) else session.get("scene")
        brand_id = session.get("brand_id") or (job_doc.get("brand_id") if job_doc else None)

        # Notify users that generation is starting (best-effort)
        try:
            background_tasks.add_task(send_text, from_phone, "Buddy photo already received — generating your avatar now. This may take a few minutes.")
            if buddy_phone:
                background_tasks.add_task(send_text, buddy_phone, "We have the requester's photo. Generating the avatar now.")
        except Exception:
            logger.exception("[user_image] failed to notify participants about generation start")

        # 4) Schedule generation worker via background_tasks (non-blocking)
        background_tasks.add_task(fire_and_forget, generate_for_pair(room_id, from_phone, buddy_phone, image_url, buddy_img_url, scene, brand_id))
        logger.info("[user_image] scheduled generate_for_pair for room_id=%s", room_id)

    except Exception as e:
        logger.exception("[user_image] unexpected error handling user image for wa=%s: %s", from_phone, e)
        # clear generation_started flag so future attempts possible
        try:
            if session.get("room_id"):
                await users_collection_goldflake.update_one({"room_id": session.get("room_id")}, {"$set": {"generation_started": False}}, upsert=False)
        except Exception:
            logger.exception("[user_image] failed to clear generation_started after error")

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
                upload_key: str,
                archetype_for_comfy: str) -> dict:
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
                    archetype=archetype_for_comfy,   # e.g., "chai" or "rooftop"
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
                upload_key: str,
                archetype_for_comfy: str) -> dict:
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
                    archetype=archetype_for_comfy,
                    upload_key=upload_key,             # 🔸 deterministic key chosen by caller
                )
            except Exception as e:
                logger.exception(f"[goldflake] run_comfy_workflow error: {e}")
                return {"success": False, "error": f"run_comfy_workflow error: {e}"}

        if tasks:
            tasks.add_task(_run)                                              # schedule background
            logger.info(f"[goldflake] queued run for room_id={room_id} archetype={archetype_norm}")
        else:
            # logger.info(f"[goldflake] calling _run with archetype={archetype_combined} upload_key={upload_key}")

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

                    # button_reply is for "buttons"; list_reply is for "list" (options)
                    btn = interactive.get("button_reply")
                    lst = interactive.get("list_reply")

                    if btn:
                        choice_id = btn.get("id")
                        logger.debug(f"Button reply received from {from_phone}: id={choice_id}")
                    elif lst:
                        # list_reply generally contains 'id' and 'title'
                        choice_id = lst.get("id")
                        list_title = lst.get("title")  # optional: useful for logging/debug
                        logger.debug(f"List reply received from {from_phone}: id={choice_id} title={list_title}")


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
                # BUDDY-ONLY SESSION: handle buddy's flow first (if this session is a buddy session)
                # =========================
                # We create buddy sessions with initial_stage == "q_buddy_image_only" and with metadata:
                # session["linked_user_phone"], session["linked_user_room"]
                # =========================
                # BUDDY-ONLY SESSION: handle buddy's flow first (if this session is a buddy session)
                # =========================
                # Buddy stages: q_buddy_wait_agree, q_buddy_image_only
                # The buddy session should have been created with initial_stage="q_buddy_wait_agree"
                st = session.get("stage")

                # ---------- Branch: buddy image upload (final buddy stage) ----------
                if st == "q_buddy_image_only":
                    # Buddy's job is simple: send an image. If they text, nudge them to send an image.
                    if msg_type == "image":
                        image_obj = msg.get("image", {}) or {}
                        media_id = image_obj.get("id")
                        if not media_id:
                            logger.warning(f"Buddy image message without media id from {from_phone}")
                            background_tasks.add_task(send_text, from_phone, "Please send your photo as an *image*.")
                            continue

                        logger.info(f"[BUDDY-IMAGE] from={from_phone} stage={st} media_id={media_id}")

                        # Save buddy image locally
                        linked_user_room = session.get("linked_user_room")
                        room_id_for_upload = linked_user_room or session.get("room_id") or f"buddy_{from_phone}"
                        filename = f"{room_id_for_upload}_p2.jpg"
                        try:
                            abs_path = await download_whatsapp_media_to_file(media_id, filename)
                        except Exception as e:
                            logger.exception(f"Failed to download buddy image: {e}")
                            background_tasks.add_task(send_text, from_phone, "Couldn’t read that image. Please resend.")
                            continue

                        # Upload buddy image to S3 using same s3 key pattern as user p2
                        try:
                            key2 = s3_key_for_user_upload(room_id_for_upload, "p2")
                            upload_file_to_s3(abs_path, key2, cache_control="private, max-age=31536000")
                            buddy_image_url = presign_get_url(key2, expires=3600)
                            session["buddy_image_path"] = abs_path
                            session["buddy_image_url"] = buddy_image_url
                            session["updated_at_utc"] = now_utc_iso()
                            session["updated_at_ist"] = now_ist_iso()
                            background_tasks.add_task(save_session_snapshot, from_phone, session)
                            logger.info(f"[S3] Uploaded buddy image -> s3://{S3_GF_BUCKET}/{key2}")
                        except Exception as e:
                            logger.exception(f"S3 upload/presign failed (buddy image): {e}")
                            background_tasks.add_task(send_text, from_phone, "Upload failed. Please try again.")
                            continue

                        # Now check linked user — trigger generation only when both images are present
                        linked_user_phone = session.get("linked_user_phone")
                        linked_room = session.get("linked_user_room") or room_id_for_upload

                        if not linked_user_phone:
                            logger.error(f"No linked_user_phone for buddy session {from_phone}")
                            background_tasks.add_task(send_text, from_phone, "Thanks — we received your photo. We will notify the requester.")
                            continue

                        user_session = get_active_session(linked_user_phone)
                        user_image_url = None
                        if user_session:
                            user_image_url = user_session.get("user_image_url")
                        else:
                            logger.error(f"Linked user session not active for {linked_user_phone}; can't complete generation yet.")
                            background_tasks.add_task(send_text, from_phone, "Thanks — we received your photo. We'll generate the image once the requester uploads theirs.")
                            continue

                        if not user_image_url:
                            background_tasks.add_task(send_text, from_phone, "We have your photo. The requester hasn't uploaded their photo yet. We'll start generation when both photos are available.")
                            continue

                        # At this point both user_image_url and buddy_image_url exist -> queue generation
                        room_id = linked_room or user_session.get("room_id")
                        if not room_id:
                            logger.error(f"No room_id available for generation. user_session={user_session}")
                            background_tasks.add_task(send_text, from_phone, "Unexpected error: missing room reference. Please ask the requester to restart.")
                            continue

                        # Build final inputs using the user's session (the requester)
                        p1_url = user_image_url
                        p2_url = session["buddy_image_url"]
                        scene = user_session.get("scene")
                        user_gender = user_session.get("gender")
                        buddy_gender = user_session.get("buddy_gender")
                        brand_id = user_session.get("brand_id")

                        required_keys = ("scene", "brand_id", "gender", "buddy_gender", "buddy_number", "user_image_url", "buddy_image_url")
                        missing = [k for k in required_keys if not (user_session.get(k) or (k == "buddy_image_url" and session.get("buddy_image_url")))]
                        if missing:
                            logger.error(f"Missing fields before goldflake generation (buddy flow): {missing} for requester {linked_user_phone}")
                            background_tasks.add_task(send_text, from_phone, "We’re missing some details for generation. The requester needs to complete a few steps.")
                            continue

                        archetype_combined = f"{scene}_{(brand_id or '').strip().lower()}"
                        # Background worker — same generation worker you already have, run it for the requester
                        async def _generate_and_send_for_user():
                            """
                            Buddy-flow generation worker. Uses closure variables from outer scope:
                            linked_user_phone, room_id, scene, p1_url, p2_url, user_gender, buddy_gender, archetype_combined, etc.
                            `from_phone` in the outer scope is the buddy's phone (uploader).
                            This worker sends final image to both requester and buddy.
                            """
                            try:
                                # capture buddy phone from outer scope
                                buddy_phone = from_phone  # from_phone is the buddy who uploaded the image
                                await send_text(linked_user_phone, "Awesome! Generating your image. This can take a bit…")

                                upload_key = s3_key(room_id, scene)

                                g1 = (user_gender or "").lower().strip()
                                g2 = (buddy_gender or "").lower().strip()

                                # swap if needed to match generator expectation
                                if g1 == "female" and g2 == "male":
                                    g1, g2 = g2, g1
                                    p1 = p2_url
                                    p2 = p1_url
                                else:
                                    p1 = p1_url
                                    p2 = p2_url

                                combined_gender_folder = f"{g1}_{g2}"
                                if combined_gender_folder not in {"male_male", "male_female", "female_female"}:
                                    await send_text(linked_user_phone, "Invalid gender combination.")
                                    return

                                logger.info(f"[goldflake] starting generation for room_id={room_id} archetype={archetype_combined}")
                                result = await asyncio.to_thread(
                                    _run, room_id, combined_gender_folder, scene, p1, p2, upload_key, archetype_combined
                                )

                                if not (isinstance(result, dict) and result.get("success")):
                                    err = (result or {}).get("error", "Unknown error")
                                    logger.error(f"[goldflake] generation failed: {err}")
                                    await send_text(linked_user_phone, "Generation failed. Please try again later.")
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
                                    await send_text(linked_user_phone, "Upload is taking longer than expected. Please try again shortly.")
                                    return

                                s3_url = presign_get_url(upload_key, expires=3600)

                                # send to requester
                                await send_image_by_link(
                                    to_phone=linked_user_phone,
                                    url=s3_url,
                                    caption=f"{scene.title()} | Tap to view",
                                )

                                # --- NEW: also send to buddy (the uploader) ---
                                try:
                                    if buddy_phone:
                                        logger.info(f"[goldflake] also sending final image to buddy {buddy_phone} (triggered generation for {linked_user_phone})")
                                        try:
                                            await send_image_by_link(
                                                to_phone=buddy_phone,
                                                url=s3_url,
                                                caption=f"{scene.title()} — here’s the avatar you contributed to. Tap to view",
                                            )
                                        except Exception as e:
                                            logger.exception(f"[goldflake] failed to send final image to buddy {buddy_phone}: {e}")
                                except Exception as e:
                                    logger.exception(f"[goldflake] unexpected error while attempting buddy send: {e}")

                                # persist final state into requester's session
                                try:
                                    u_session = get_active_session(linked_user_phone)
                                    if u_session and u_session.get("room_id") == room_id:
                                        u_session["final_image_url"] = s3_url
                                        u_session["status"] = "done"
                                        u_session["stage"] = "done"
                                        u_session["updated_at_utc"] = now_utc_iso()
                                        u_session["updated_at_ist"] = now_ist_iso()
                                        try:
                                            asyncio.create_task(save_session_snapshot(linked_user_phone, u_session))
                                        except Exception as e:
                                            logger.exception(f"[snapshot] failed to schedule save_session_snapshot (user active): {e}")
                                    else:
                                        snap = {
                                            "room_id": room_id,
                                            "wa_phone": linked_user_phone,
                                            "final_image_url": s3_url,
                                            "status": "done",
                                            "stage": "done",
                                            "updated_at_utc": now_utc_iso(),
                                            "updated_at_ist": now_ist_iso(),
                                        }
                                        try:
                                            asyncio.create_task(save_session_snapshot(linked_user_phone, snap))
                                        except Exception as e:
                                            logger.exception(f"[snapshot] failed to schedule save_session_snapshot (user minimal): {e}")
                                except Exception as e:
                                    logger.exception(f"[goldflake] Exception while preparing final snapshot (buddy flow): {e}")

                                try:
                                    archived = await end_session(linked_user_phone, reason="completed_success")
                                    if archived:
                                        archived["final_image_url"] = archived.get("final_image_url", s3_url)
                                        archived["status"] = "done"
                                        archived["stage"] = "done"
                                        try:
                                            asyncio.create_task(save_session_snapshot(linked_user_phone, archived))
                                        except Exception as e:
                                            logger.exception(f"[snapshot] failed to schedule save_session_snapshot (archived): {e}")
                                except Exception as e:
                                    logger.exception(f"[goldflake] Failed to end session after generation (buddy flow): {e}")

                            except Exception as e:
                                logger.exception(f"[goldflake] Failed to generate/send image (buddy flow): {e}")
                                try:
                                    await send_text(linked_user_phone, "Generation failed. Please try again later.")
                                except Exception:
                                    pass


                        room_id = linked_room or user_session.get("room_id")
                        if not room_id:
                            logger.error(f"No room_id available for generation. user_session={user_session}")
                            background_tasks.add_task(send_text, from_phone, "Unexpected error: missing room reference. Please ask the requester to restart.")
                            continue

                        # Try to acquire lock atomically. If we fail, another routine already started generation.
                        lock_acquired = await acquire_generation_lock(room_id)
                        if not lock_acquired:
                            logger.info("[buddy-image] generation already started for room_id=%s; skipping duplicate start", room_id)
                            # Optionally inform the buddy politely (non-critical)
                            try:
                                background_tasks.add_task(send_text, from_phone, "Thanks — generation is already in progress. We'll notify you when it's ready.")
                            except Exception:
                                logger.exception("[buddy-image] failed to notify buddy about in-progress generation")
                            continue

                        # Lock acquired -> proceed to schedule worker
                        try:
                            background_tasks.add_task(fire_and_forget, _generate_and_send_for_user())
                            logger.info("[buddy-image] scheduled generation (lock acquired) for room_id=%s", room_id)
                        except Exception:
                            logger.exception("[buddy-image] failed to schedule generation worker for room_id=%s", room_id)
                            # clear lock so future attempts possible (best-effort)
                            try:
                                await users_collection_goldflake.update_one({"room_id": room_id}, {"$set": {"generation_started": False}}, upsert=False)
                            except Exception:
                                logger.exception("[buddy-image] failed to clear generation_started after scheduling error for room_id=%s", room_id)
                        # done processing buddy image
                        continue

                    else:
                        # any non-image: nudge buddy to send an image
                        background_tasks.add_task(send_text, from_phone, "Hi — please send your photo as an *image* so we can complete the avatar.")
                        continue
                
                # ---------- Branch: buddy must 'I agree' before image ----------
                if st == "q_buddy_wait_agree":
                    # if buddy sends exact agreed text -> advance to image-only stage
                    if msg_type == "text" and lower == "i agree":
                        session["stage"] = "q_buddy_image_only"
                        session["updated_at_utc"] = now_utc_iso()
                        session["updated_at_ist"] = now_ist_iso()
                        background_tasks.add_task(save_session_snapshot, from_phone, session)
                        background_tasks.add_task(send_text, from_phone, "Thanks — please send your photo now (as an image).")
                        logger.info(f"[buddy_notify] buddy {from_phone} agreed; now awaiting image (linked to {session.get('linked_user_phone')}).")
                        continue
                    else:
                        background_tasks.add_task(send_text, from_phone, "To continue, please reply exactly: *I agree*")
                        continue

                # =========================
                # INTERACTIVE BUTTONS (stage-specific for user flow)
                # =========================
                if msg_type == "interactive" and choice_id:
                    st = session.get("stage")

                    if choice_id == "agree_yes":
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
                        session["stage"] = "q_brand"
                        session["updated_at_utc"] = now_utc_iso()
                        session["updated_at_ist"] = now_ist_iso()
                        background_tasks.add_task(save_session_snapshot, from_phone, session)
                        background_tasks.add_task(send_brand_question, from_phone)
                        continue

                    if st == "q_brand" and choice_id in {
                        "brand_goldflake", "brand_classic", "brand_wills", "brand_flake"
                    }:
                        brand_map = {
                            "brand_goldflake": ("goldflake", "Gold Flake"),
                            "brand_classic":   ("classic",  "Classic"),
                            "brand_wills":     ("wills",    "Wills"),
                            "brand_flake":     ("flake",    "Flake"),
                        }
                        brand_pair = brand_map.get(choice_id)
                        if not brand_pair:
                            logger.error(f"Unknown brand choice_id: {choice_id}")
                            background_tasks.add_task(send_text, from_phone, "Sorry, I didn't understand that selection. Please choose again.")
                            background_tasks.add_task(send_brand_question, from_phone)
                            continue
                        brand_id, brand_label = brand_pair
                        session["brand_id"] = brand_id
                        session["brand"] = brand_label
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
                        session["stage"] = "q_buddy_number"
                        session["updated_at_utc"] = now_utc_iso()
                        session["updated_at_ist"] = now_ist_iso()
                        background_tasks.add_task(save_session_snapshot, from_phone, session)
                        background_tasks.add_task(send_buddy_number_question, from_phone)
                        continue

                    background_tasks.add_task(send_text, from_phone, "Let’s continue. Please follow the prompts.")
                    continue

                # =========================
                # TEXT MESSAGES (user flow)
                # =========================
                if msg_type == "text" and lower is not None:
                    st = session.get("stage")

                    if st == "t_and_c" and lower in {"hi", "hello", "hey", "heyy", "hii"}:
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

                    if st == "q_buddy_number":
                        digits = re.sub(r"\D", "", raw_text)
                        if len(digits) == 10:
                            # store raw 10-digit input
                            session["buddy_number"] = digits

                            # Compose full whatsapp number (try to reuse sender prefix)
                            try:
                                from_digits = re.sub(r"\D", "", from_phone or "")
                                if len(from_digits) > 10:
                                    prefix = from_digits[:-10]
                                    buddy_whatsapp = prefix + digits
                                else:
                                    buddy_whatsapp = digits
                            except Exception:
                                buddy_whatsapp = digits

                            # Keep in-memory state consistent for requester
                            session["buddy_whatsapp"] = buddy_whatsapp
                            session["buddy_phone"] = buddy_whatsapp
                            session["buddy_link_status"] = "invited"
                            session["stage"] = "q_user_image"   # advance requester stage
                            session["updated_at_utc"] = now_utc_iso()
                            session["updated_at_ist"] = now_ist_iso()

                            # Snapshot the requester's user-data doc (non-blocking)
                            try:
                                background_tasks.add_task(save_session_snapshot, from_phone, session)
                            except Exception:
                                logger.exception("[buddy_notify] failed to schedule save_session_snapshot for requester")

                            # create buddy session (non-blocking) and notify buddy
                            async def _notify_and_start_buddy(buddy_phone: str, inviter_name: str | None, linked_user_phone: str, linked_room_id: str):
                                try:
                                    # send template invite (best-effort)
                                    url = f"https://graph.facebook.com/v24.0/{PHONE_NUMBER_ID}/messages"
                                    payload = {
                                        "messaging_product": "whatsapp",
                                        "to": buddy_phone,
                                        "type": "template",
                                        "template": {"name": "hello_world", "language": {"code": "en_US"}}
                                    }
                                    headers = {"Authorization": f"Bearer {ACCESS_TOKEN}", "Content-Type": "application/json"}
                                    async with httpx.AsyncClient(timeout=20) as client:
                                        resp = await client.post(url, headers=headers, json=payload)
                                        if resp.status_code // 100 == 2:
                                            logger.info(f"[buddy_notify] template sent to {buddy_phone}")
                                        else:
                                            logger.error(f"[buddy_notify] template send failed to {buddy_phone}: {resp.text}")
                                except Exception:
                                    logger.exception(f"[buddy_notify] template http send exception for {buddy_phone}")

                                # create buddy session and snapshot buddy-data
                                try:
                                    buddy_session = await start_new_session(buddy_phone, initial_stage="q_buddy_wait_agree")
                                    # mark buddy session role and link pointers
                                    buddy_session["role"] = "buddy"
                                    buddy_session["linked_user_phone"] = linked_user_phone
                                    buddy_session["linked_user_room"] = linked_room_id
                                    buddy_session["buddy_name"] = None
                                    buddy_session["buddy_gender"] = None
                                    buddy_session["stage"] = "q_buddy_image_only"

                                    # persist buddy-data snapshot (non-blocking)
                                    try:
                                        await save_session_snapshot(buddy_phone, buddy_session)
                                    except Exception:
                                        logger.exception(f"[buddy_notify] failed to snapshot buddy session for {buddy_phone}")

                                    # schedule linking (non-blocking)
                                    try:
                                        background_tasks.add_task(link_requester_and_buddy, linked_room_id, buddy_session.get("room_id"), linked_user_phone, buddy_phone)
                                    except Exception:
                                        logger.exception("[buddy_notify] failed to schedule link_requester_and_buddy")
                                except Exception:
                                    logger.exception(f"[buddy_notify] failed to create buddy session for {buddy_phone}")

                            # notify buddy in background
                            background_tasks.add_task(_notify_and_start_buddy, session.get("buddy_whatsapp"), session.get("name"), from_phone, session.get("room_id"))

                            # ask user for next input
                            background_tasks.add_task(send_text, from_phone, "Great. Please send **your photo** now (as an image).")

                            logger.info(f"[buddy_notify] computed buddy_whatsapp='{buddy_whatsapp}' invited_by='{from_phone}'")
                        else:
                            background_tasks.add_task(send_text, from_phone, "Please enter a valid 10-digit mobile number (digits only).")
                        continue
                    

                    # generic re-prompts for text messages at wrong input
                    if st == "q_scene":
                        background_tasks.add_task(send_scene_question, from_phone)
                    elif st == "q_brand":
                        background_tasks.add_task(send_brand_question, from_phone)
                    elif st == "q_gender":
                        background_tasks.add_task(send_gender_question, from_phone)
                    elif st == "q_user_image":
                        background_tasks.add_task(send_text, from_phone, "Please send your photo as an *image*.")
                    elif st == "q_buddy_image":
                        background_tasks.add_task(send_text, from_phone, "We no longer ask you to upload your buddy's image. Your buddy will be invited to send theirs directly.")
                    elif st == "waiting_for_buddy_image":
                        background_tasks.add_task(send_text, from_phone, "We’re waiting for your buddy’s photo. They should receive an invite to send it.")
                    elif st == "done":
                        background_tasks.add_task(send_text, from_phone, "You’re all set already. 🙌")
                    else:
                        background_tasks.add_task(send_text, from_phone, "Let’s continue. Please follow the prompts.")
                    continue


                # =========================
                # IMAGE (WhatsApp media) — user uploads
                # =========================
                if msg_type == "image":
                    image_obj = msg.get("image", {}) or {}
                    media_id = image_obj.get("id")
                    if not media_id:
                        logger.warning(f"Image message without media id from {from_phone}")
                        continue

                    logger.info(f"[IMAGE] from={from_phone} stage={session.get('stage')} media_id={media_id}")

                    room_id = session.get("room_id")
                    if not room_id:
                        logger.warning(f"No room_id in active session for {from_phone}; asking user to restart.")
                        background_tasks.add_task(send_text, from_phone, "Please restart the flow by replying *restart* or type *hi* to begin.")
                        background_tasks.add_task(send_restart_button, from_phone)
                        continue

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

                        session["stage"] = "waiting_for_buddy_image"
                        session["updated_at_utc"] = now_utc_iso()
                        session["updated_at_ist"] = now_ist_iso()
                        background_tasks.add_task(save_session_snapshot, from_phone, session)

                        background_tasks.add_task(send_text, from_phone,
                                                 "Got your photo. We invited your buddy to upload their photo — we'll start generating once they send it.")
                        continue

                    # legacy branch: requester uploading buddy image (backwards compatibility)
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

                        required_keys = ("scene", "brand_id", "gender", "buddy_gender", "buddy_number", "user_image_url", "buddy_image_url")
                        missing = [k for k in required_keys if not session.get(k)]
                        if missing:
                            logger.error(f"Missing fields before goldflake generation: {missing} for {from_phone}")
                            background_tasks.add_task(send_text, from_phone, "We’re missing some details. Let’s continue where we left off.")
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

                        p1_url = session["user_image_url"]
                        p2_url = session["buddy_image_url"]
                        scene = session["scene"]
                        user_gender = session["gender"]
                        buddy_gender = session["buddy_gender"]
                        brand_id = session.get("brand_id")
                        if not brand_id:
                            logger.error(f"Missing brand_id in session for {from_phone} before generation. Session: {session}")
                            await send_text(from_phone, "We didn't record your brand selection. Please choose your brand again.")
                            background_tasks.add_task(send_brand_question, from_phone)
                            session["stage"] = "q_brand"
                            session["updated_at_utc"] = now_utc_iso()
                            session["updated_at_ist"] = now_ist_iso()
                            background_tasks.add_task(save_session_snapshot, from_phone, session)
                            continue

                        archetype_combined = f"{scene}_{(brand_id or '').strip().lower()}"

                        # Background worker — queue generation
                        async def _generate_and_send(from_phone: str,
                                                    room_id: str,
                                                    scene: str,
                                                    brand_id: str,
                                                    user_gender: str,
                                                    buddy_gender: str,
                                                    p1_url: str,
                                                    p2_url: str,
                                                    archetype_combined: str) -> None:
                            """
                            Generate the image for the requester (from_phone). After sending to requester,
                            attempt to also send the final image to the buddy (if we can determine their WA number).
                            """
                            try:
                                await send_text(from_phone, "Awesome! Generating your image. This can take a bit…")

                                upload_key = s3_key(room_id, scene)

                                g1 = (user_gender or "").lower().strip()
                                g2 = (buddy_gender or "").lower().strip()

                                # generator expects male_first ordering in some cases
                                if g1 == "female" and g2 == "male":
                                    g1, g2 = g2, g1
                                    p1_url, p2_url = p2_url, p1_url

                                combined_gender_folder = f"{g1}_{g2}"
                                if combined_gender_folder not in {"male_male", "male_female", "female_female"}:
                                    await send_text(from_phone, "Invalid gender combination.")
                                    return

                                logger.info(f"[goldflake] archetype_combined={archetype_combined} upload_key={upload_key}")

                                result = await asyncio.to_thread(
                                    _run, room_id, combined_gender_folder, scene, p1_url, p2_url, upload_key, archetype_combined
                                )

                                if not (isinstance(result, dict) and result.get("success")):
                                    err = (result or {}).get("error", "Unknown error")
                                    logger.error(f"[goldflake] generation failed: {err}")
                                    await send_text(from_phone, "Generation failed. Please try again later.")
                                    return

                                # wait for upload to appear in S3
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

                                s3_url = presign_get_url(upload_key, expires=3600)

                                # send to requester (existing behaviour)
                                await send_image_by_link(
                                    to_phone=from_phone,
                                    url=s3_url,
                                    caption=f"{scene.title()} | Tap to view",
                                )

                                # --- NEW: attempt to also send to buddy ---
                                try:
                                    # robustly determine buddy whatsapp for this requester
                                    buddy_whatsapp = None
                                    requester_session = get_active_session(from_phone)
                                    if requester_session:
                                        # prefer explicitly stored full WA number
                                        buddy_whatsapp = requester_session.get("buddy_whatsapp")
                                        # fallback to raw 10-digit buddy_number + prefix from requester
                                        if not buddy_whatsapp and requester_session.get("buddy_number"):
                                            digits = re.sub(r"\D", "", requester_session.get("buddy_number") or "")
                                            if digits:
                                                try:
                                                    from_digits = re.sub(r"\D", "", from_phone or "")
                                                    if len(from_digits) > 10:
                                                        prefix = from_digits[:-10]
                                                        buddy_whatsapp = prefix + digits
                                                    else:
                                                        buddy_whatsapp = digits
                                                except Exception:
                                                    buddy_whatsapp = digits

                                    if buddy_whatsapp:
                                        # send final image to buddy, with a friend-specific caption
                                        logger.info(f"[goldflake] sending final image to buddy {buddy_whatsapp} for requester {from_phone}")
                                        try:
                                            await send_image_by_link(
                                                to_phone=buddy_whatsapp,
                                                url=s3_url,
                                                caption=f"{scene.title()} — your friend shared this avatar with you. Tap to view",
                                            )
                                        except Exception as e:
                                            logger.exception(f"[goldflake] failed to send final image to buddy {buddy_whatsapp}: {e}")
                                    else:
                                        logger.info(f"[goldflake] no buddy_whatsapp found for requester {from_phone}; skipping buddy send")
                                except Exception as e:
                                    logger.exception(f"[goldflake] unexpected error while attempting buddy send for {from_phone}: {e}")

                                # persist final image & mark session done for requester
                                try:
                                    session = get_active_session(from_phone)
                                    if session and session.get("room_id") == room_id:
                                        session["final_image_url"] = s3_url
                                        session["status"] = "done"
                                        session["stage"] = "done"
                                        session["updated_at_utc"] = now_utc_iso()
                                        session["updated_at_ist"] = now_ist_iso()
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

                                # End the session (archive in-memory)
                                try:
                                    archived = await end_session(from_phone, reason="completed_success")
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
                            _generate_and_send(from_phone, room_id, scene,brand_id, user_gender, buddy_gender, p1_url, p2_url, archetype_combined)
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
            "type": "list",
            "body": {
                "text": "Which scene captures your perfect smoke-buddy chill?"
            },
            "action": {
                "button": "Choose Scene",
                "sections": [
                    {
                        "title": "Scenes",
                        "rows": [
                            {
                                "id": "scene_chai",
                                "title": "Chai",
                                "description": "Relaxing with chai and conversations"
                            },
                            {
                                "id": "scene_rooftop",
                                "title": "Rooftop",
                                "description": "Breezy rooftop chill with music"
                            },
                        ],
                    }
                ],
            },
        },
    }

    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }

    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(url, headers=headers, json=payload)
        if resp.status_code // 100 != 2:
            logger.error(f"Failed to send scene question to {to_phone}: {resp.text}")
        else:
            logger.info(f"Scene question sent to {to_phone}")

async def send_brand_question(to_phone: str):
    url = f"https://graph.facebook.com/v20.0/{PHONE_NUMBER_ID}/messages"
    payload = {
        "messaging_product": "whatsapp",
        "to": to_phone,
        "type": "interactive",
        "interactive": {
            "type": "list",
            "body": {
                "text": "Select your first smoke brand?"
            },
            "action": {
                "button": "Choose Brand",
                "sections": [
                    {
                        "title": "Brands",
                        "rows": [
                            {"id": "brand_goldflake", "title": "Gold Flake"},
                            {"id": "brand_classic", "title": "Classic"},
                            {"id": "brand_wills", "title": "Wills"},
                            {"id": "brand_flake", "title": "Flake"},
                        ],
                    }
                ],
            },
        },
    }

    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }

    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(url, headers=headers, json=payload)
        if resp.status_code // 100 != 2:
            logger.error(f"Failed to send brand question to {to_phone}: {resp.text}")
        else:
            logger.info(f"Brand question sent to {to_phone}")


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

async def send_buddy_number_question(to_phone: str):
    await send_text(
        to_phone,
        "Please share your buddy’s 10-digit mobile number."
    )

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
    elif stage == "q_buddy_number":
        background_tasks.add_task(send_buddy_number_question, to_phone)
    else:
        background_tasks.add_task(send_text, to_phone, "Let’s continue. Please follow the prompts.")
