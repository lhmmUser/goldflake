# whatsapp_utils.py
import httpx
import base64

ACCESS_TOKEN = "EAAdlXL2TRc0BO1s2QlLWSDcs1RfZA2ksSc5zxMhdpGZBnCu99ez2QEEbZA0GISqr9WlGaIFYvqkMjGnZCbBEYREzBie0FWnttlXccoqNS09jDFyqrfNVE0u7J6gf2ClV0JKZBVcEFq48Uxs2z9FKhyu7NMl57jRCyissPEm66t89ZACUTDqm73v7Fo0h480fVmlAZDZD"
PHONE_NUMBER_ID = "569466079590956"

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

async def send_image_to_whatsapp(to: str, base64_image: str):
    url = f"https://graph.facebook.com/v18.0/{PHONE_NUMBER_ID}/media"
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
    }

    files = {
        "file": ("image.png", base64.b64decode(base64_image), "image/png")
    }
    data = {
        "messaging_product": "whatsapp",
        "type": "image"
    }

    async with httpx.AsyncClient() as client:
        resp = await client.post(url, headers=headers, data=data, files=files)
        media_id = resp.json().get("id")

        if media_id:
            msg_payload = {
                "messaging_product": "whatsapp",
                "to": to,
                "type": "image",
                "image": {"id": media_id}
            }
            await client.post(
                f"https://graph.facebook.com/v18.0/{PHONE_NUMBER_ID}/messages",
                headers={"Authorization": f"Bearer {ACCESS_TOKEN}", "Content-Type": "application/json"},
                json=msg_payload
            )
        else:
            await send_whatsapp_reply(to, "Image upload failed.")
