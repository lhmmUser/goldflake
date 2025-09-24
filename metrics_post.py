# metrics_post.py
import os
import time
import threading
import logging
import socket
from typing import Optional

import boto3
from botocore.config import Config
import requests
from fastapi import Request

# ------------------------- config -------------------------

AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")

# IMPORTANT: set this in your ASG user-data or .env to the *real* ASG name
ASG_NAME = os.getenv("ASG_NAME", "df_yippe_template").strip()

NAMESPACE   = "App/HTTP"          # must match your IAM policy condition
METRIC_MIN  = "POSTPerMinute"     # 1-min count per instance + fleet

LOG_LEVEL = os.getenv("METRICS_LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [metrics] %(message)s",
)
log = logging.getLogger("metrics")

HOST = socket.gethostname()

# ------------------- global state / clients ----------------

_cw = boto3.client(
    "cloudwatch",
    region_name=AWS_REGION,
    config=Config(retries={"max_attempts": 5, "mode": "standard"}),
)

_lock = threading.Lock()
_count = 0
_started = False  # guard so we only start one publisher thread

# ------------------------- helpers ------------------------

def _get_instance_id() -> str:
    """Best-effort fetch of EC2 instance-id from IMDS (no error if not EC2)."""
    try:
        return requests.get(
            "http://169.254.169.254/latest/meta-data/instance-id",
            timeout=1,
        ).text.strip()
    except Exception:
        return "unknown"

INSTANCE_ID = os.getenv("INSTANCE_ID", "").strip() or _get_instance_id()


def _sleep_to_next_minute() -> None:
    """Sleep so the loop wakes up on a neat 60s boundary."""
    now = time.time()
    delay = 60 - (now % 60)
    if delay < 0.05:
        delay = 0.05
    time.sleep(delay)

# ------------------------- API ----------------------------

def inc_if_post(request: Request) -> None:
    """
    Call this from an HTTP middleware on every request.
    Increments the counter for POST requests only.
    """
    if request.method.upper() == "POST":
        global _count
        with _lock:
            _count += 1


def _publisher_loop() -> None:
    if not ASG_NAME:
        log.warning(
            "ASG_NAME is empty. Set ASG_NAME in env/user-data to your real Auto Scaling Group name!"
        )

    log.info(
        "publisher start: ns=%s metric=%s asg=%s instance=%s host=%s region=%s",
        NAMESPACE, METRIC_MIN, ASG_NAME, INSTANCE_ID, HOST, AWS_REGION,
    )

    while True:
        _sleep_to_next_minute()

        with _lock:
            c = _count
            _count = 0

        # human-readable trace
        log.info("POST metrics: last_min=%d asg=%s instance=%s", c, ASG_NAME, INSTANCE_ID)

        # publish per-instance + fleet aggregate (same metric name)
        metric_data = [
            {
                "MetricName": METRIC_MIN,
                "Dimensions": [
                    {"Name": "AutoScalingGroupName", "Value": ASG_NAME},
                    {"Name": "InstanceId", "Value": INSTANCE_ID},
                ],
                "Unit": "Count",
                "Value": c,
            },
            {
                "MetricName": METRIC_MIN,
                "Dimensions": [
                    {"Name": "AutoScalingGroupName", "Value": ASG_NAME},
                ],
                "Unit": "Count",
                "Value": c,
            },
        ]

        try:
            _cw.put_metric_data(Namespace=NAMESPACE, MetricData=metric_data)
        except Exception as e:
            # never interrupt serving traffic due to metrics; just log it
            log.warning("put_metric_data failed: %s", e)


def start_publisher_thread() -> None:
    """
    Start the background publisher exactly once per process.
    Safe to call multiple times.
    """
    global _started
    if _started:
        return
    _started = True
    t = threading.Thread(
        target=_publisher_loop,
        name="cw-metrics-publisher",
        daemon=True,
    )
    t.start()
