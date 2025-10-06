import os, threading, time, boto3, logging, socket
from collections import deque
from datetime import datetime, timezone
from fastapi import Request

AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
ASG_NAME   = os.getenv("ASG_NAME", "df_yippe_template")
NAMESPACE  = "App/HTTP"
METRIC_MIN = "POSTPerMinute"
METRIC_5M  = "POST5MinSum"

# --- logging ---------------------------------------------------------------
LOG_LEVEL = os.getenv("METRICS_LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [metrics] %(message)s",
)
log = logging.getLogger("metrics")
HOST = socket.gethostname()

# --- cw client & counters --------------------------------------------------
_cw = boto3.client("cloudwatch", region_name=AWS_REGION)
_lock = threading.Lock()
_count = 0
_last5 = deque(maxlen=5)  # stores the last five 1-minute counts

def inc_if_post(request: Request):
    """Call this in a middleware for each request."""
    global _count
    if request.method == "POST":
        with _lock:
            _count += 1

def _publisher():
    global _count
    log.info(
        "publisher start: ns=%s min_metric=%s sum5_metric=%s asg=%s host=%s",
        NAMESPACE, METRIC_MIN, METRIC_5M, ASG_NAME, HOST,
    )
    while True:
        time.sleep(60)
        ts = datetime.now(timezone.utc)
        with _lock:
            c = _count
            _count = 0
            _last5.append(c)
            s5 = sum(_last5)

        # human-readable snapshot in the logs
        log.info(
            "POST metrics: last_min=%d last5_sum=%d window=%s asg=%s",
            c, s5, list(_last5), ASG_NAME
        )

        # publish both metrics at the same timestamp
        try:
            _cw.put_metric_data(
                Namespace=NAMESPACE,
                MetricData=[
                    {
                        "MetricName": METRIC_MIN,
                        "Dimensions": [
                            {"Name": "AutoScalingGroupName", "Value": ASG_NAME}
                        ],
                        "Timestamp": ts,
                        "Unit": "Count",
                        "Value": c,
                    },
                    {
                        "MetricName": METRIC_5M,
                        "Dimensions": [
                            {"Name": "AutoScalingGroupName", "Value": ASG_NAME}
                        ],
                        "Timestamp": ts,
                        "Unit": "Count",
                        "Value": s5,
                    },
                ],
            )
        except Exception as e:
            # log and keep going; we don't want to break request handling
            log.warning("put_metric_data failed: %s", e)

def start_publisher_thread():
    t = threading.Thread(target=_publisher, daemon=True)
    t.start()