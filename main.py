from fastapi import FastAPI, Request, Query, BackgroundTasks
import httpx
import base64
from form import update_user_session, reset_session, user_sessions, get_next_question, handle_response
from app import run_comfy_workflow_and_send_image
from whatsapp_utils import send_whatsapp_reply
from fastapi.responses import PlainTextResponse

app = FastAPI()

ACCESS_TOKEN = "EAAdlXL2TRc0BO1s2QlLWSDcs1RfZA2ksSc5zxMhdpGZBnCu99ez2QEEbZA0GISqr9WlGaIFYvqkMjGnZCbBEYREzBie0FWnttlXccoqNS09jDFyqrfNVE0u7J6gf2ClV0JKZBVcEFq48Uxs2z9FKhyu7NMl57jRCyissPEm66t89ZACUTDqm73v7Fo0h480fVmlAZDZD"
PHONE_NUMBER_ID = "569466079590956"
VERIFY_TOKEN = "my_secret_token_123"

@app.get("/root")
def root():
    return {"status": "ok", "message": "Webhook server is running"}

@app.get("/webhook")
def verify_webhook(
    hub_mode: str = Query(..., alias="hub.mode"),
    hub_verify_token: str = Query(..., alias="hub.verify_token"),
    hub_challenge: str = Query(..., alias="hub.challenge")
):
    if hub_mode == "subscribe" and hub_verify_token == VERIFY_TOKEN:
        return PlainTextResponse(hub_challenge)
    return {"status": "unauthorized"}


@app.post("/webhook")
async def receive_whatsapp_event(request: Request, background_tasks: BackgroundTasks):
    payload = await request.json()
    print("🔹 Received Webhook:", payload)

    try:
        changes = payload["entry"][0]["changes"][0]
        value = changes.get("value", {})
        messages = value.get("messages", [])

        if not messages:
            print("⚠️ No messages in payload")
            return {"status": "no_message"}

        message = messages[0]
        sender = message.get("from")
        message_type = message.get("type")
        print("📨 Message Type:", message_type)

        if sender not in user_sessions:
            reset_session(sender)
        session = user_sessions[sender]

        # ✅ TEXT MESSAGE HANDLER
        if message_type == "text":
            text = message["text"]["body"].strip()

            if text.lower() in ["hi", "hello", "start"]:
                reset_session(sender)
                await send_gender_buttons(sender)
                return {"status": "gender_prompted"}

            elif "gender" in session and "name" not in session:
    # 👤 Save the user's name
                update_user_session(sender, "name", text)
                await send_next_question(sender, get_next_question(sender))
                return {"status": "quiz_started"}

            elif "final_profile" not in session:
    # ✅ Only handle if the text is a digit (quiz response)
                if text.isdigit():
                    handle_response(sender, text)
                    next_q = get_next_question(sender)
                    await send_next_question(sender, next_q)

                    if next_q["type"] == "done":
                        await send_whatsapp_reply(sender, "📸 Now upload a photo of yourself to see your transformation.")
                    return {"status": "question_answered"}
                else:
                    await send_whatsapp_reply(sender, "⚠️ Please tap a button or choose from the list.")
                    return {"status": "invalid_quiz_input"}


        # ✅ INTERACTIVE MESSAGE HANDLER (buttons & lists)
        elif message_type == "interactive":
            interactive_type = message["interactive"].get("type")

            if interactive_type == "button_reply":
                reply_id = message["interactive"]["button_reply"]["id"]
                update_user_session(sender, "gender", "female" if reply_id == "F" else "male")
                await send_whatsapp_reply(sender, "👍 Got it! Now please type your name.")
                return {"status": "awaiting_name"}

            elif interactive_type == "list_reply":
                reply_id = message["interactive"]["list_reply"]["id"]
                handle_response(sender, reply_id)

                # ✅ Ensure final_profile is now present if this was the last step
                session = user_sessions.get(sender, {})
                next_q = get_next_question(sender)

                    # If the quiz just completed
                if next_q["type"] == "done":
                # Final profile *may already be set* by handle_response
                    if "final_profile" not in session:
                        await send_whatsapp_reply(sender, "📸 Now upload a photo of yourself to see your transformation.")
                    else:
                        await send_whatsapp_reply(sender, f"🎭 Your fantasy profile is: *{session['final_profile']}*\n📸 Now upload a photo to complete your journey.")

                else:
                    await send_next_question(sender, next_q)

                return {"status": "quiz_progressed"}


        # ✅ IMAGE UPLOAD HANDLER
        elif message_type == "image":
            if "final_profile" not in session:
                await send_whatsapp_reply(sender, "🧠 Please complete the quiz first.")
                return {"status": "quiz_incomplete"}

            image_id = message["image"]["id"]
            image_url = await fetch_media_url(image_id)
            image_bytes = await download_media(image_url)
            update_user_session(sender, "user_image", image_bytes)

            await send_whatsapp_reply(sender, "🎨 Got your image! Generating your fantasy transformation...")
            background_tasks.add_task(
                run_comfy_workflow_and_send_image,
                sender,
                session.get("name"),
                session.get("gender"),
                session.get("final_profile"),
                image_bytes
            )

            return {"status": "processing_started"}

    except Exception as e:
        print("❌ Error handling webhook:", e)

    return {"status": "received"}
# 🔹 UTILITIES
async def send_whatsapp_reply(to: str, message: str):
    url = f"https://graph.facebook.com/v18.0/{PHONE_NUMBER_ID}/messages"
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {
        "messaging_product": "whatsapp",
        "to": to,
        "type": "text",
        "text": {"body": message}
    }
    async with httpx.AsyncClient() as client:
        await client.post(url, headers=headers, json=payload)

async def send_gender_buttons(to: str):
    await send_button_options(
        to,
        "What is your gender?",
        buttons=[
            {"id": "M", "title": "Male"},
            {"id": "F", "title": "Female"},
        ]
    )

async def fetch_media_url(media_id: str) -> str:
    url = f"https://graph.facebook.com/v18.0/{media_id}"
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN}"}
    async with httpx.AsyncClient() as client:
        response = await client.get(url, headers=headers)
        return response.json().get("url")

async def download_media(url: str) -> bytes:
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN}"}
    async with httpx.AsyncClient() as client:
        response = await client.get(url, headers=headers)
        return response.content

async def send_button_options(to: str, body_text: str, buttons: list):
    url = f"https://graph.facebook.com/v18.0/{PHONE_NUMBER_ID}/messages"
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {
        "messaging_product": "whatsapp",
        "to": to,
        "type": "interactive",
        "interactive": {
            "type": "button",
            "body": {"text": body_text},
            "action": {
                "buttons": [
                    {"type": "reply", "reply": {"id": btn["id"], "title": btn["title"][:20]}}
                    for btn in buttons[:3]
                ]
            }
        }
    }
    print("📦 WhatsApp Button Payload:", payload)
    async with httpx.AsyncClient() as client:
        response = await client.post(url, headers=headers, json=payload)
        print("📬 Button Response:", response.status_code, response.text)


async def send_list_options(to: str, header_text: str, body_text: str, footer_text: str, rows: list):
    url = f"https://graph.facebook.com/v18.0/{PHONE_NUMBER_ID}/messages"
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {
        "messaging_product": "whatsapp",
        "to": to,
        "type": "interactive",
        "interactive": {
            "type": "list",
            "header": {"type": "text", "text": header_text},
            "body": {"text": body_text},
            "footer": {"text": footer_text},
            "action": {
                "button": "Choose",
                "sections": [{"title": "Options", "rows": rows}]
            }
        }
    }
    print("\U0001F4E6 WhatsApp List Payload:", payload)
    async with httpx.AsyncClient() as client:
        response = await client.post(url, headers=headers, json=payload)
        print("\U0001F4EC List Response:", response.status_code, response.text)


async def send_next_question(sender: str, question: dict):
    if question["type"] == "text":
        await send_whatsapp_reply(sender, question["text"])
    elif question["type"] == "buttons":
        await send_button_options(sender, question["text"], question["buttons"])
    elif question["type"] == "list":
        await send_list_options(
            to=sender,
            header_text="Question",
            body_text=question["text"],
            footer_text="Select one to continue",
            rows=[{"id": btn["id"], "title": btn["title"]} for btn in question["buttons"]]
        )
    elif question["type"] == "done":
        profile = user_sessions[sender]["final_profile"]
        await send_whatsapp_reply(sender, f"🎭 Your fantasy profile is: *{profile}*\nThanks for playing! 🌟")
        reset_session(sender)



   
