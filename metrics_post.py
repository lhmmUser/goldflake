# metrics_post.py
import os, threading, time, boto3, logging, socket, urllib.request
from collections import deque
from datetime import datetime, timezone
from fastapi import Request

AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
ASG_NAME   = os.getenv("ASG_NAME", "df_yippe_template")  # <- set to actual ASG name in env
NAMESPACE  = "App/HTTP"
METRIC_MIN = "POSTPerMinute"
METRIC_5M  = "POST5MinSum"

# Comma-separated allowlist of POST paths to count (normalized without trailing "/")
_ALLOWED_POST_PATHS = {
    p.strip().rstrip("/") for p in os.getenv("METRICS_POST_PATHS", "/chat360/webhook,/api/yippee").split(",") if p.strip()
}

LOG_LEVEL = os.getenv("METRICS_LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [metrics] %(message)s",
)
log = logging.getLogger("metrics")

_cw = boto3.client("cloudwatch", region_name=AWS_REGION)
_lock = threading.Lock()
_count = 0
_last5 = deque(maxlen=5)

def _imdsv2_instance_id(timeout=0.3):
    """Return EC2 instance-id via IMDSv2, or hostname if not on EC2."""
    token_req = urllib.request.Request(
        "http://169.254.169.254/latest/api/token",
        method="PUT",
        headers={"X-aws-ec2-metadata-token-ttl-seconds": "21600"},
    )
    try:
        with urllib.request.urlopen(token_req, timeout=timeout) as r:
            token = r.read().decode()
        req = urllib.request.Request(
            "http://169.254.169.254/latest/meta-data/instance-id",
            headers={"X-aws-ec2-metadata-token": token},
        )
        with urllib.request.urlopen(req, timeout=timeout) as r2:
            return r2.read().decode()
    except Exception:
        return socket.gethostname()  # fallback outside EC2

INSTANCE_ID = os.getenv("INSTANCE_ID") or _imdsv2_instance_id()
log.info("metrics source INSTANCE_ID=%s ASG_NAME=%s", INSTANCE_ID, ASG_NAME)

def inc_if_post(request: Request):
    """Call this in a middleware for each request. Counts only allowed POST paths."""
    global _count
    if request.method == "POST":
        path = (request.url.path or "").rstrip("/") or "/"
        if path in _ALLOWED_POST_PATHS:
            with _lock:
                _count += 1
    # No GET tracking at all

def _publisher():
    global _count
    log.info(
        "publisher start: ns=%s min_metric=%s sum5_metric=%s asg=%s instance=%s",
        NAMESPACE, METRIC_MIN, METRIC_5M, ASG_NAME, INSTANCE_ID,
    )
    while True:
        time.sleep(60)
        ts = datetime.now(timezone.utc)
        with _lock:
            c = _count
            _count = 0
            _last5.append(c)
            s5 = sum(_last5)

        # snapshot log (concise; POSTs only)
        log.info(
            "POST metrics: last_min=%d last5_sum=%d window=%s asg=%s instance=%s",
            c, s5, list(_last5), ASG_NAME, INSTANCE_ID
        )

        # publish per-instance and ASG rollup
        try:
            _cw.put_metric_data(
                Namespace=NAMESPACE,
                MetricData=[
                    {
                        "MetricName": METRIC_MIN,
                        "Dimensions": [
                            {"Name": "AutoScalingGroupName", "Value": ASG_NAME},
                            {"Name": "InstanceId", "Value": INSTANCE_ID},
                        ],
                        "Timestamp": ts,
                        "Unit": "Count",
                        "Value": c,
                    },
                    {
                        "MetricName": METRIC_5M,
                        "Dimensions": [
                            {"Name": "AutoScalingGroupName", "Value": ASG_NAME},
                            {"Name": "InstanceId", "Value": INSTANCE_ID},
                        ],
                        "Timestamp": ts,
                        "Unit": "Count",
                        "Value": s5,
                    },
                    {
                        "MetricName": METRIC_MIN,
                        "Dimensions": [
                            {"Name": "AutoScalingGroupName", "Value": ASG_NAME},
                        ],
                        "Timestamp": ts,
                        "Unit": "Count",
                        "Value": c,
                    },
                    {
                        "MetricName": METRIC_5M,
                        "Dimensions": [
                            {"Name": "AutoScalingGroupName", "Value": ASG_NAME},
                        ],
                        "Timestamp": ts,
                        "Unit": "Count",
                        "Value": s5,
                    },
                ],
            )
        except Exception as e:
            log.warning("put_metric_data failed: %s", e)

# Guard to avoid multiple publisher threads in one process
_started = False
def start_publisher_thread():
    global _started
    if _started:
        log.info("publisher already started; skipping")
        return
    _started = True
    t = threading.Thread(target=_publisher, daemon=True)
    t.start()
