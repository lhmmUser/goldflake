import base64
import uuid
import json
import os
import urllib.request
import urllib.parse
from io import BytesIO
from PIL import Image
from whatsapp_utils import send_image_to_whatsapp, send_whatsapp_reply
import websocket
import boto3
import time
import pyheif


s3 = boto3.client("s3")
S3_BUCKET = "diffrun-generated"

def s3_key(sender, profile):
    ts = int(time.time())
    return f"chat360/generated/{sender}_{profile.replace(' ', '_').lower()}_{ts}.jpg"


# Settings
server_address = "127.0.0.1:8188"
comfy_input_root = "/Drive/ComfyUI/input"  # change if needed

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
    "The Starbound Voyager": "df_sv"
}

def run_comfy_workflow_and_send_image(sender: str, name: str, gender: str, final_profile: str, image_url: str):
    client_id = str(uuid.uuid4())
    print(f"👤 Starting image generation for: {name}, {gender}, {final_profile}")
    gender = gender.lower()
    folder_key = profile_to_folder.get(final_profile)
    if not folder_key:
        print(f"❌ Unknown profile '{final_profile}' — no matching folder_key.")
        send_whatsapp_reply(sender, f"⚠️ Unknown personality: {final_profile}")
        return

    workflow_path = os.path.join(comfy_input_root, "df", gender, folder_key, f"{folder_key}.json")
    print("🧩 Using workflow path:", workflow_path)

    try:
        with open(workflow_path, "r", encoding="utf-8") as f:
            jsonwf = json.load(f)
        print("✅ Workflow loaded successfully.")
    except Exception as e:
        print(f"❌ Failed to load workflow: {e}")
        send_whatsapp_reply(sender, "⚠️ Workflow not found.")
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
        if ext == "heic":
    
            print("🌀 HEIC image detected. Converting to PNG...")
            heif_file = pyheif.read(BytesIO(selfie_bytes))
            image = Image.frombytes(
                heif_file.mode,
                heif_file.size,
                heif_file.data,
                "raw",
                heif_file.mode,
                heif_file.stride,
            )
        else:
            image = Image.open(BytesIO(selfie_bytes))
        image.save(user_image_abs_path)
        print(f"✅ Selfie saved to {user_image_abs_path}")
    except Exception as e:
        print("❌ Failed to download/save selfie:", e)
        send_whatsapp_reply(sender, "⚠️ Invalid selfie URL or image format.")
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
            key = s3_key(sender, final_profile)
            try:
                s3.put_object(
                Bucket=S3_BUCKET,
                Key=key,
                Body=image_result,
                ContentType="image/jpeg",
                ACL="public-read"  # or remove this if you're using presigned URLs
                )
                print(f"✅ Uploaded to S3: s3://{S3_BUCKET}/{key}")

                # Optionally send WhatsApp preview
                encoded_image = base64.b64encode(image_result).decode("utf-8")
                send_image_to_whatsapp(sender, encoded_image)

                try: 
                    import requests
                    chat360_url = "https://app.chat360.io/api/chatbox/dynamic-webhook/5a8100b8-d6e3-4d89-89e2-f0c8c53169a8"
                    payload = {
                        "room_id": sender,
                        "keyword": "hyperverge status"
                    }
                    headers = {"Content-Type": "application/json"}
                    response = requests.post(chat360_url, json=payload, headers=headers)
                    if response.status_code == 200:
                        print("📣 Chat360 notified successfully.")
                    else:
                        print(f"⚠️ Failed to notify Chat360: {response.status_code} - {response.text}")
                except Exception as e:
                    print("❌ Error notifying Chat360:", e)

            except Exception as e:
                print("❌ Failed to upload image to S3:", e)
                send_whatsapp_reply(sender, "⚠️ Image generated, but upload to cloud failed.")
        else:
            print("⚠️ Image generation failed or returned nothing.")
            send_whatsapp_reply(sender, "⚠️ Failed to generate image.")

    except Exception as e:
        print("❌ Error during ComfyUI execution:", e)
        send_whatsapp_reply(sender, "⚠️ Workflow execution error.")

    # Cleanup selfie
    try:
        os.remove(user_image_abs_path)
        print(f"🧹 Deleted temporary selfie: {user_image_abs_path}")
    except Exception as e:
        print("⚠️ Could not delete selfie:", e)
