import base64
import uuid
import json
import os
import urllib.request
import urllib.parse
from io import BytesIO
from PIL import Image
import websocket
import time
import pillow_heif
pillow_heif.register_heif_opener()
from s3_client import get_s3_client
import requests 
import inspect
import asyncio
from config import settings

s3 = get_s3_client()
S3_BUCKET = settings.S3_BUCKET_DF
S3_BUCKET_2 = settings.S3_BUCKET_YIPPEE

def s3_key(sender, profile):
    ts = int(time.time())
    return f"chat360/generated/{sender}_{profile.replace(' ', '_').lower()}_{ts}.jpg"

INTERNAL_BASE_URL = settings.INTERNAL_BASE_URL
# Settings
_server_raw = settings.COMFY_SERVER.strip()
server_address = (
    _server_raw.replace("http://", "")
               .replace("https://", "")
               .replace("ws://", "")
               .replace("wss://", "")
               .strip("/")
)
comfy_input_root = settings.COMFY_INPUT_ROOT

profile_to_folder = {
    "The Golden Goal Seeker": "df_ggs",
    "The Wanted and Wild": "df_ww",
    "The Strike Master": "df_smr",
    "The Highway Howler": "df_hhw",
    "The Iron Gladiator": "df_igdtr",
    "The Shadow Striker": "df_sstr",
    "The Hopeless Romantic": "df_hr",
    "The Style Iconoclast": "df_si",
    "The Stage Stormer": "df_rs",
    "The Starbound Voyager": "df_sv",
}

profile_to_folder_sf = {
    "Artist": "sf_art",
    "Badminton Player": "sf_bad",
    "Chef": "sf_chef",
    "Cricketer": "sf_cric",
    "Dancer": "sf_dan",
    "Doctor": "sf_doc",
    "Football Player": "sf_foot",
    "Musician": "sf_mus",
    "Pilot": "sf_pio",
    "Socialmedia Influencer": "sf_soc",
    "Professor": "sf_pro",
}
def _as_int(value) -> int | None:
    
    try:
        return int(str(value).strip())
    except Exception:
        return None

def get_denoise_from_age(age) -> float:
   
    ai = _as_int(age)
    return 0.5 if ai in (1, 2) else 0.9

    
def get_weight_from_age(age) -> float:
    
    ai = _as_int(age)
    return 0.5 if ai in (1, 2) else 0.8

def call_maybe_async(fn, *args, **kwargs):
    """
    If fn returns an awaitable, schedule it on the current loop (or run a new one).
    Otherwise just call it.
    """
    try:
        result = fn(*args, **kwargs)
        if inspect.isawaitable(result):
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(result)
            except RuntimeError:
                # No running loop (e.g., called from a plain thread) -> create one
                asyncio.run(result)
    except Exception as e:
        print(f"⚠️ WhatsApp send failed: {e}")

def transcode_to_jpeg(image_bytes: bytes) -> bytes:
    """
    Re-encode any input image bytes to JPEG with controlled quality.
    Optional resize via env JPEG_MAX_SIDE (disabled by default).
    """
    # Config from env (safe defaults)
    quality = int(os.getenv("JPEG_QUALITY", "85"))          # 60–90 is typical
    max_side = int(os.getenv("JPEG_MAX_SIDE", "0") or 0)    # 0 = no resize

    with Image.open(BytesIO(image_bytes)) as im:
        # Optional downscale to cap the longest side
        if max_side and max(im.size) > max_side:
            ratio = max_side / max(im.size)
            new_size = (int(im.width * ratio), int(im.height * ratio))
            im = im.resize(new_size, Image.LANCZOS)

        # JPEG requires RGB (no alpha)
        if im.mode not in ("RGB", "L"):
            im = im.convert("RGB")
        elif im.mode == "L":  # grayscale -> keep as L or convert to RGB; both are valid JPEG
            im = im.convert("RGB")

        out = BytesIO()
        im.save(
            out,
            format="JPEG",
            quality=quality,
            optimize=True,
            progressive=True,
            subsampling=2,   # 4:2:0 for smaller files
        )
        return out.getvalue()

def run_comfy_workflow_and_send_image(sender: str, name: str, gender: str, final_profile: str, image_url: str):
    client_id = str(uuid.uuid4())
    print(f"👤 Starting image generation for: {name}, {gender}, {final_profile}")
    gender = gender.lower()
    folder_key = profile_to_folder.get(final_profile)
    if not folder_key:
        print(f"❌ Unknown profile '{final_profile}' — no matching folder_key.")
        
        return

    workflow_path = os.path.join(comfy_input_root, "df", gender, folder_key, f"{folder_key}.json")
    print("🧩 Using workflow path:", workflow_path)

    try:
        with open(workflow_path, "r", encoding="utf-8") as f:
            jsonwf = json.load(f)
        print("✅ Workflow loaded successfully.")
    except Exception as e:
        print(f"❌ Failed to load workflow: {e}")
        
        return


    if "68" in jsonwf and "inputs" in jsonwf["68"]:
        caps_name = name.upper()
        jsonwf["68"]["inputs"]["value"] = caps_name
        print(f"📝 Injected name '{name}' into node 68.")
    else:
        print("⚠️ Node 68 not found or missing inputs.")

    # Save selfie
    ext = image_url.lower().split('.')[-1]
    user_image_rel_path = os.path.join("user_images", f"{sender}_input.png")
    user_image_abs_path = os.path.join(comfy_input_root, user_image_rel_path)
    os.makedirs(os.path.dirname(user_image_abs_path), exist_ok=True)
    print("📥 Preparing to download selfie from:", image_url)

    try:
        selfie_bytes = urllib.request.urlopen(image_url).read()
        
        image = Image.open(BytesIO(selfie_bytes))
        image.save(user_image_abs_path)
        print(f"✅ Selfie saved to {user_image_abs_path}")
    except Exception as e:
        print("❌ Failed to download/save selfie:", e)
        
        return

    if "12" in jsonwf and "inputs" in jsonwf["12"]:
        jsonwf["12"]["inputs"]["image"] = user_image_rel_path.replace("\\", "/")
        print(f"🧠 Injected selfie path into node 12: {user_image_rel_path}")
    else:
        print("⚠️ Node 12 not found in workflow.")

    # Workflow execution
    def queue_prompt(prompt):
        p = {"prompt": prompt, "client_id": client_id}
        data = json.dumps(p).encode("utf-8")
        req = urllib.request.Request(f"http://{server_address}/prompt", data=data)
        print("🚀 Queuing prompt to ComfyUI...")
        return json.loads(urllib.request.urlopen(req).read())

    def get_image(filename, subfolder, folder_type):
        data = {"filename": filename, "subfolder": subfolder, "type": folder_type}
        url_values = urllib.parse.urlencode(data)
        print("📸 Fetching image from ComfyUI output...")
        with urllib.request.urlopen(f"http://{server_address}/view?{url_values}") as response:
            return response.read()

    def get_history(prompt_id):
        print("🕓 Fetching workflow history...")
        with urllib.request.urlopen(f"http://{server_address}/history/{prompt_id}") as response:
            return json.loads(response.read())

    def get_images(ws, prompt):
        prompt_id = queue_prompt(prompt)["prompt_id"]
        print(f"🆔 Prompt ID: {prompt_id}")
        while True:
            out = ws.recv()
            if isinstance(out, str):
                message = json.loads(out)
                if message["type"] == "executing":
                    data = message["data"]
                    if data["node"] is None and data["prompt_id"] == prompt_id:
                        print("✅ Workflow execution completed.")
                        break
        history = get_history(prompt_id)[prompt_id]
        for node_id, output in history["outputs"].items():
            if "images" in output:
                img = output["images"][0]
                return get_image(img["filename"], img["subfolder"], img["type"])
        return None

    try:
        print("🔌 Connecting to ComfyUI WebSocket...")
        ws = websocket.WebSocket()
        ws.connect(f"ws://{server_address}/ws?clientId={client_id}")
        image_result = get_images(ws, jsonwf)
        ws.close()

        if image_result:
            # NEW: force JPEG re-encode
            jpeg_bytes = transcode_to_jpeg(image_result)

            key = s3_key(sender, final_profile)  # keep .jpg to match your GET filters
            try:
                s3.put_object(
                    Bucket=S3_BUCKET,
                    Key=key,
                    Body=jpeg_bytes,
                    ContentType="image/jpeg",
                    ACL="public-read"
                )

                print(f"✅ Uploaded to S3: s3://{S3_BUCKET}/{key}")

                try:
                    requests.post(
                        f"{INTERNAL_BASE_URL}/internal/mark-uploaded",
                        json={"room_id": sender, "campaign": "darkfantasy"},
                        timeout=5,
                    )
                except Exception as e:
                    print("⚠️ Could not notify internal /internal/mark-uploaded:", e)

                # Optionally send WhatsApp preview
                encoded_image = base64.b64encode(image_result).decode("utf-8")
                

                try: 
                    
                    chat360_url_df = settings.CHAT360_URL_DF
                    payload = {
                        "room_id": sender,
                        "keyword": "hyperverge status"
                    }
                    headers = {"Content-Type": "application/json"}
                    response = requests.post(chat360_url_df, json=payload, headers=headers)
                    if response.status_code == 200:
                        print("📣 Chat360 notified successfully.")
                    else:
                        print(f"⚠️ Failed to notify Chat360: {response.status_code} - {response.text}")
                except Exception as e:
                    print("❌ Error notifying Chat360:", e)

            except Exception as e:
                print("❌ Failed to upload image to S3:", e)
                
        else:
            print("⚠️ Image generation failed or returned nothing.")
            

    except Exception as e:
        print("❌ Error during ComfyUI execution:", e)
        

    # Cleanup selfie
    try:
        os.remove(user_image_abs_path)
        print(f"🧹 Deleted temporary selfie: {user_image_abs_path}")
    except Exception as e:
        print("⚠️ Could not delete selfie:", e)

def run_comfy_workflow_and_send_image_sf(sender: str, name: str, gender: str, final_profile: str, image_url: str, age: str):
    client_id = str(uuid.uuid4())
    print(f"👤 Starting image generation for: {name}, {gender}, {final_profile}")
    gender = gender.lower()
    folder_key = profile_to_folder_sf.get(final_profile)
    if not folder_key:
        print(f"❌ Unknown profile '{final_profile}' — no matching folder_key.")
        

    workflow_path = os.path.join(comfy_input_root, "sf", gender, folder_key, f"{folder_key}.json")
    print("🧩 Using workflow path:", workflow_path)

    try:
        with open(workflow_path, "r", encoding="utf-8") as f:
            jsonwf = json.load(f)
        print("✅ Workflow loaded successfully.")
    except Exception as e:
        print(f"❌ Failed to load workflow: {e}")
        
        return
    
    # Inject denoise (node 1)
    if "1" in jsonwf and "inputs" in jsonwf["1"]:
        denoise_value = get_denoise_from_age(age)
        jsonwf["1"]["inputs"]["denoise"] = denoise_value
        print(f"🎛️ Injected denoise={denoise_value} into node 1 (age={age}).")
    else:
        print("⚠️ Node 1 not found in workflow.")

    if "3" in jsonwf and isinstance(jsonwf["3"], dict) and "inputs" in jsonwf["3"]:
        weight_value = get_weight_from_age(age)
        jsonwf["3"]["inputs"]["weight"] = float(weight_value)
        print(f"⚖️ Node 3: weight set to {weight_value} (age={age})")
    else:
        print("⚠️ Node 3 missing or malformed; skipping weight injection.")

    if "68" in jsonwf and "inputs" in jsonwf["68"]:
        caps_name = name.upper()
        jsonwf["68"]["inputs"]["value"] = caps_name
        print(f"📝 Injected name '{name}' into node 68.")
    else:
        print("⚠️ Node 68 not found or missing inputs.")

    # Save selfie
    ext = image_url.lower().split('.')[-1]
    user_image_rel_path = os.path.join("user_images", f"{sender}_input.png")
    user_image_abs_path = os.path.join(comfy_input_root, user_image_rel_path)
    os.makedirs(os.path.dirname(user_image_abs_path), exist_ok=True)
    print("📥 Preparing to download selfie from:", image_url)

    try:
        selfie_bytes = urllib.request.urlopen(image_url).read()
       
        image = Image.open(BytesIO(selfie_bytes))
        image.save(user_image_abs_path)
        print(f"✅ Selfie saved to {user_image_abs_path}")
    except Exception as e:
        print("❌ Failed to download/save selfie:", e)
        
        return

    if "12" in jsonwf and "inputs" in jsonwf["12"]:
        jsonwf["12"]["inputs"]["image"] = user_image_rel_path.replace("\\", "/")
        print(f"🧠 Injected selfie path into node 12: {user_image_rel_path}")
    else:
        print("⚠️ Node 12 not found in workflow.")

    # Workflow execution
    def queue_prompt(prompt):
        p = {"prompt": prompt, "client_id": client_id}
        data = json.dumps(p).encode("utf-8")
        req = urllib.request.Request(f"http://{server_address}/prompt", data=data)
        print("🚀 Queuing prompt to ComfyUI...")
        return json.loads(urllib.request.urlopen(req).read())

    def get_image(filename, subfolder, folder_type):
        data = {"filename": filename, "subfolder": subfolder, "type": folder_type}
        url_values = urllib.parse.urlencode(data)
        print("📸 Fetching image from ComfyUI output...")
        with urllib.request.urlopen(f"http://{server_address}/view?{url_values}") as response:
            return response.read()

    def get_history(prompt_id):
        print("🕓 Fetching workflow history...")
        with urllib.request.urlopen(f"http://{server_address}/history/{prompt_id}") as response:
            return json.loads(response.read())

    def get_images(ws, prompt):
        prompt_id = queue_prompt(prompt)["prompt_id"]
        print(f"🆔 Prompt ID: {prompt_id}")
        while True:
            out = ws.recv()
            if isinstance(out, str):
                message = json.loads(out)
                if message["type"] == "executing":
                    data = message["data"]
                    if data["node"] is None and data["prompt_id"] == prompt_id:
                        print("✅ Workflow execution completed.")
                        break
        history = get_history(prompt_id)[prompt_id]
        for node_id, output in history["outputs"].items():
            if "images" in output:
                img = output["images"][0]
                return get_image(img["filename"], img["subfolder"], img["type"])
        return None

    try:
        print("🔌 Connecting to ComfyUI WebSocket...")
        ws = websocket.WebSocket()
        ws.connect(f"ws://{server_address}/ws?clientId={client_id}")
        image_result = get_images(ws, jsonwf)
        ws.close()

        if image_result:
            jpeg_bytes = transcode_to_jpeg(image_result)

            key = s3_key(sender, final_profile)
            try:
                s3.put_object(
                    Bucket=S3_BUCKET_2,
                    Key=key,
                    Body=jpeg_bytes,
                    ContentType="image/jpeg",
                    ACL="public-read"
                )

                print(f"✅ Uploaded to S3: s3://{S3_BUCKET_2}/{key}")

                 # ✅ NEW: notify our API to stamp time_image_uploaded & lag (sunfeast)
                try:
                    requests.post(
                        f"{INTERNAL_BASE_URL}/internal/mark-uploaded",
                        json={"room_id": sender, "campaign": "sunfeast"},
                        timeout=5,
                    )
                except Exception as e:
                    print("⚠️ Could not notify internal /internal/mark-uploaded:", e)


                # Optionally send WhatsApp preview
                encoded_image = base64.b64encode(image_result).decode("utf-8")
                

                try: 
                    
                    chat360_url_yippee = settings.CHAT360_URL_YIPPEE
                    payload = {
                        "room_id": sender,
                        "keyword": "hyperverge status"
                    }
                    headers = {"Content-Type": "application/json"}
                    response = requests.post(chat360_url_yippee, json=payload, headers=headers)
                    if response.status_code == 200:
                        print("📣 Chat360 notified successfully.")
                    else:
                        print(f"⚠️ Failed to notify Chat360: {response.status_code} - {response.text}")
                except Exception as e:
                    print("❌ Error notifying Chat360:", e)

            except Exception as e:
                print("❌ Failed to upload image to S3:", e)
                
        else:
            print("⚠️ Image generation failed or returned nothing.")
            

    except Exception as e:
        print("❌ Error during ComfyUI execution:", e)
        

    # Cleanup selfie
    try:
        os.remove(user_image_abs_path)
        print(f"🧹 Deleted temporary selfie: {user_image_abs_path}")
    except Exception as e:
        print("⚠️ Could not delete selfie:", e)
