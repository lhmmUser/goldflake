from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel, HttpUrl
from fastapi.staticfiles import StaticFiles
import uuid, httpx, sys, base64, os

sys.path.append("..")
from app import run_comfy_workflow_and_send_image

app = FastAPI()
jobs = {}

class GenerateRequest(BaseModel):
    gender: str
    name: str
    phone_number: str
    image: HttpUrl
    fantasy_profile: str
    client_job_id: str

@app.post("/generate")
async def generate(req: GenerateRequest, bg: BackgroundTasks):
    try:
        async with httpx.AsyncClient() as client:
            r = await client.get(str(req.image), timeout=20)
        if r.status_code != 200:
            raise HTTPException(400, "Invalid image URL")
        image_bytes = r.content
    except Exception as e:
        raise HTTPException(400, f"Image download failed: {e}")

    job_id = str(uuid.uuid4())
    jobs[job_id] = {"status": "queued", "result": None}

    async def run_and_record():
        try:
            # 🔁 Call your original function (it doesn't return anything)
            await run_comfy_workflow_and_send_image(
                sender="webform",
                name=req.name,
                gender=req.gender,
                final_profile=req.fantasy_profile,
                image_bytes=image_bytes
            )

            # ✅ Now independently read the generated image
            # Based on your usual ComfyUI output structure:
            image_path = f"/Drive/ComfyUI/output/{job_id}_out.png"

            if not os.path.exists(image_path):
                # try fallback if your actual image name is static
                image_path = "/Drive/ComfyUI/output/result.png"  # update as per your naming convention

            with open(image_path, "rb") as f:
                encoded = base64.b64encode(f.read()).decode("utf-8")

            jobs[job_id]["status"] = "done"
            jobs[job_id]["result"] = f"data:image/png;base64,{encoded}"
        except Exception as e:
            print("❌ Intercept error:", e)
            jobs[job_id]["status"] = "failed"
            jobs[job_id]["result"] = str(e)

    bg.add_task(run_and_record)
    return {"server_job_id": job_id, "status_url": f"/result/{job_id}"}

@app.get("/result/{job_id}")
def get_result(job_id: str):
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(404, "Invalid job ID")
    return job

app.mount("/", StaticFiles(directory=".", html=True), name="static")
