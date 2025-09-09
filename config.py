# config.py
import os
from dotenv import load_dotenv

load_dotenv()

def _get(name: str, default: str | None = None, *, required: bool = False) -> str:
    v = os.getenv(name, default)
    if required and (v is None or v == ""):
        raise RuntimeError(f"Missing required env var: {name}")
    return v or ""

class Settings:
    # MongoDB
    MONGO_URL_DF      = _get("MONGO_URL_DF", required=True)
    MONGO_URL_YIPPEE  = _get("MONGO_URL_YIPPEE", required=True)

    # AWS / S3
    AWS_REGION        = _get("AWS_REGION", "ap-south-1")
    S3_BUCKET_DF      = _get("S3_BUCKET_DF", "diffrun-generated")
    S3_BUCKET_YIPPEE  = _get("S3_BUCKET_YIPPEE", "yippee-generated")

    # ComfyUI
    COMFY_SERVER      = _get("COMFY_SERVER", "127.0.0.1:8188")
    COMFY_INPUT_ROOT  = _get("COMFY_INPUT_ROOT", r"E:\ComfyUI_windows_portable_nvidia\ComfyUI_windows_portable\ComfyUI\input")

    # Internal callback
    INTERNAL_BASE_URL = _get("INTERNAL_BASE_URL", "http://127.0.0.1:8000")

    # Optional notify
    ENABLE_CHAT360_NOTIFY = _get("ENABLE_CHAT360_NOTIFY", "false").lower() == "true"
    CHAT360_URL_DF           = _get("CHAT360_URL_DF", "")  # leave empty if unused
    CHAT360_URL_YIPPEE       = _get("CHAT360_URL_YIPPEE", "")  # leave empty if unused
settings = Settings()
