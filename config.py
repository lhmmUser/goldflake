import os

SERVER_ADDRESS = "127.0.0.1:8188"

INPUT_FOLDER = "/Drive/ComfyUI/input"
OUTPUT_FOLDER = "/Drive/ComfyUI/output"
JPG_OUTPUT = "/Drive/ComfyUI/jpg-output"
FINAL_IMAGES = "/Drive/ComfyUI/final-images"
WATERMARK_PATH = "/Drive/Diffrun/images/Watermark.png"

os.makedirs(FINAL_IMAGES, exist_ok=True)
os.makedirs(INPUT_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)
os.makedirs(JPG_OUTPUT, exist_ok=True)