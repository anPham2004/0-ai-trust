"""Alert through SNS when AWS Free plan credit falls below the safety threshold."""

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3


THRESHOLD = float(os.getenv("AWS_CREDIT_ALERT_THRESHOLD_USD", "80"))
INTERVAL = int(os.getenv("AWS_CREDIT_CHECK_INTERVAL_SECONDS", "3600"))
TOPIC_ARN = os.environ["CREDIT_ALERT_TOPIC_ARN"]
STATE_FILE = Path("/state/alerted")
FREE_TIER = boto3.client("freetier", region_name="us-east-1")
SNS = boto3.client("sns", region_name=os.getenv("AWS_REGION", "ap-southeast-2"))


def check() -> None:
    state = FREE_TIER.get_account_plan_state()
    remaining = float(state["accountPlanRemainingCredits"]["amount"])
    status = state["accountPlanStatus"]
    print(json.dumps({"event": "credit_checked", "remaining_usd": remaining,
                      "threshold_usd": THRESHOLD, "status": status,
                      "timestamp": datetime.now(timezone.utc).isoformat()}), flush=True)
    unsafe = status != "ACTIVE" or remaining < THRESHOLD
    if unsafe and not STATE_FILE.exists():
        SNS.publish(
            TopicArn=TOPIC_ARN,
            Subject="0-ai-trust AWS credit alert",
            Message=(f"AWS remaining credit is ${remaining:.2f}, below the "
                     f"${THRESHOLD:.2f} safety threshold. Review or stop EC2."),
        )
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        STATE_FILE.write_text(str(remaining), encoding="utf-8")
    elif not unsafe and STATE_FILE.exists():
        STATE_FILE.unlink()


if __name__ == "__main__":
    while True:
        try:
            check()
        except Exception as exc:
            print(json.dumps({"event": "credit_check_failed",
                              "error_type": type(exc).__name__}), flush=True)
        time.sleep(INTERVAL)
