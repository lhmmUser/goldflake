import base64
from whatsapp_utils import send_image_to_whatsapp, send_whatsapp_reply
from PIL import Image
from io import BytesIO

async def run_comfy_workflow_and_send_image(sender: str, name: str, gender: str, final_profile: str, image_bytes: bytes = None):

    import websocket
    import uuid
    import json
    import os
    import urllib.request
    import urllib.parse

    server_address = "127.0.0.1:8188"
    #server_address = "comfyui.diffrunbackend.com"
    client_id = str(uuid.uuid4())

    # ✅ Correct ComfyUI input root
    comfy_input_root = "/Drive/ComfyUI/input"

    profile_to_folder = {
        "The Golden Goal Seeker": "df_ggs",
        "The Wanted and Wild": "df_ww",
        "The Strike Master": "df_smr",
        "The Midnight Rider": "df_hhw",
        "The Iron Gladiator": "df_igdtr",
        "The Shadow Striker": "df_sstr",
        "The Hopeless Romantic": "df_hr",
        "The Style Iconoclast": "df_si",
        "The Stage Stormer": "df_rs",
        "The Starbound Voyager": "df_sv"
    }

    folder_key = profile_to_folder.get(final_profile)
    print("folder_key",folder_key)
    if not folder_key:
        print(f"❌ Unknown profile: {final_profile}")
        await send_whatsapp_reply(sender, f"⚠️ Unknown personality: {final_profile}. Please contact support.")
        return

    workflow_path = os.path.join(comfy_input_root, "df", gender, folder_key, f"{folder_key}.json")
    print("workflow_path",workflow_path)
    try:
        with open(workflow_path, "r", encoding="utf-8") as f:
            jsonwf = json.load(f)
    except Exception as e:
        print(f"❌ Failed to load workflow: {e}")
        await send_whatsapp_reply(sender, "Workflow not found. Please contact support.")
        return

    # ✅ Inject name
    # ✅ Dynamically inject name into the first node that accepts a text 'value'
    if "68" in jsonwf and "inputs" in jsonwf["68"]:
        print(f"{name} → Injecting into node 68")
        jsonwf["68"]["inputs"]["value"] = name
    else:
        print("⚠️ Node 68 not found or malformed in workflow JSON")


    # ✅ Handle uploaded image and replace node 12
    if image_bytes:
        user_image_rel_path = os.path.join("user_images", f"{sender}_input.png")
        user_image_abs_path = os.path.join(comfy_input_root, user_image_rel_path)
        os.makedirs(os.path.dirname(user_image_abs_path), exist_ok=True)

        try:
            image = Image.open(BytesIO(image_bytes))
            image.save(user_image_abs_path)
        except Exception as e:
            print("❌ Invalid image upload:", e)
            await send_whatsapp_reply(sender, "⚠️ Please send a valid image file.")
            return

        if "12" in jsonwf and "inputs" in jsonwf["12"]:
            jsonwf["12"]["inputs"]["image"] = user_image_rel_path.replace("\\", "/")

    # Run workflow via WebSocket
    def queue_prompt(prompt):
        p = {"prompt": prompt, "client_id": client_id}
        data = json.dumps(p).encode('utf-8')
        req = urllib.request.Request(f"http://{server_address}/prompt", data=data)
        return json.loads(urllib.request.urlopen(req).read())

    def get_image(filename, subfolder, folder_type):
        data = {"filename": filename, "subfolder": subfolder, "type": folder_type}
        url_values = urllib.parse.urlencode(data)
        with urllib.request.urlopen(f"http://{server_address}/view?{url_values}") as response:
            return response.read()

    def get_history(prompt_id):
        with urllib.request.urlopen(f"http://{server_address}/history/{prompt_id}") as response:
            return json.loads(response.read())

    def get_images(ws, prompt):
        prompt_id = queue_prompt(prompt)['prompt_id']
        while True:
            out = ws.recv()
            if isinstance(out, str):
                message = json.loads(out)
                if message['type'] == 'executing':
                    data = message['data']
                    if data['node'] is None and data['prompt_id'] == prompt_id:
                        break
        history = get_history(prompt_id)[prompt_id]
        for node_id, output in history["outputs"].items():
            if "images" in output:
                image_info = output["images"][0]
                return get_image(image_info["filename"], image_info["subfolder"], image_info["type"])
        return None

    try:
        ws = websocket.WebSocket()
        ws.connect(f"ws://{server_address}/ws?clientId={client_id}")
        image_result = get_images(ws, jsonwf)
        ws.close()

        if image_result:
            encoded_image = base64.b64encode(image_result).decode("utf-8")
            await send_image_to_whatsapp(sender, encoded_image)
        else:
            await send_whatsapp_reply(sender, "⚠️ We couldn't generate your image. Please try again.")
    except Exception as e:
        print("❌ Error running workflow:", e)
        await send_whatsapp_reply(sender, "Something went wrong during image generation.")
