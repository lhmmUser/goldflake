import os
import boto3
from dotenv import load_dotenv
from config import settings

load_dotenv()

def get_s3_client():
    session = boto3.Session(
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=settings.AWS_REGION,
    )
    client = session.client("s3")

    # Only safe diagnostics (no secret values)
    try:
        creds = session.get_credentials()
        source = getattr(creds, "method", "none") if creds else "none"
    except Exception:
        source = "unknown"
    print(f"[S3] client initialized | region={settings.AWS_REGION} | cred_source={source}")
    return client


