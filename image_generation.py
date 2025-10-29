import httpx
from PIL import Image
from io import BytesIO
import os

async def process_image(media_id, recipient_id, access_token, phone_number_id, action):
    try:
        headers = {"Authorization": f"Bearer {access_token}"}

        # Step 1: Get media URL
        meta_url = f"https://graph.facebook.com/v18.0/{media_id}"
        async with httpx.AsyncClient() as client:
            meta_res = await client.get(meta_url, headers=headers)
            media_url = meta_res.json().get("url")

            # Step 2: Download image
            image_res = await client.get(media_url, headers=headers)
            original_image = Image.open(BytesIO(image_res.content)).convert("RGB")

        # Step 3: Transform image
        if action == "convert_bw":
            final_image = original_image.convert("L").convert("RGB")
        elif action == "flip_horizontal":
            final_image = original_image.transpose(Image.FLIP_LEFT_RIGHT)
        else:
            final_image = original_image

        # Step 4: Save and upload
        final_path = "output.jpg"
        final_image.save(final_path)

        upload_url = f"https://graph.facebook.com/v18.0/{phone_number_id}/media"
        files = {"file": ("output.jpg", open(final_path, "rb"), "image/jpeg")}
        data = {"messaging_product": "whatsapp"}

        async with httpx.AsyncClient() as client:
            upload_res = await client.post(upload_url, headers=headers, data=data, files=files)
            new_media_id = upload_res.json().get("id")

            # Step 5: Send back
            message_url = f"https://graph.facebook.com/v18.0/{phone_number_id}/messages"
            payload = {
                "messaging_product": "whatsapp",
                "to": recipient_id,
                "type": "image",
                "image": {
                    "id": new_media_id,
                    "caption": f"Here is your {action.replace('_', ' ').title()} image"
                }
            }
            await client.post(message_url, headers=headers, json=payload)

        os.remove(final_path)

    except Exception as e:
        print("Error processing image:", e)
