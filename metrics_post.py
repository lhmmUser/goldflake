#metrics_post.py
import os, threading, time, boto3
from datetime import datetime, timezone
from fastapi import Request

AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
ASG_NAME   = os.getenv("ASG_NAME", "df-asg")   # set in user data
NAMESPACE  = "App/HTTP"
METRIC     = "POSTPerMinute"

_cloudwatch = boto3.client("cloudwatch", region_name=AWS_REGION)
_lock = threading.Lock()
_count = 0

def inc_if_post(request: Request):
    global _count
    if request.method == "POST":
        with _lock:
            _count += 1

def _publisher():
    global _count
    while True:
        time.sleep(60)
        with _lock:
            c = _count
            _count = 0
        try:
            _cloudwatch.put_metric_data(
                Namespace=NAMESPACE,
                MetricData=[{
                    "MetricName": METRIC,
                    "Dimensions": [
                        {"Name": "AutoScalingGroupName", "Value": ASG_NAME}
                    ],
                    "Timestamp": datetime.now(timezone.utc),
                    "Unit": "Count",
                    "Value": c,
                }]
            )
        except Exception:
            # keep quiet in prod; log if you prefer
            pass

def start_publisher_thread():
    t = threading.Thread(target=_publisher, daemon=True)
    t.start()
