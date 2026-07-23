import hashlib
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from confluent_kafka import Consumer


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
LANDING_ROOT = Path(os.getenv("LANDING_ROOT", "/landing"))
INTERVAL = int(os.getenv("MICROBATCH_INTERVAL_SECONDS", "300"))
SOURCE_BATCH_PREFIX = os.getenv("SOURCE_BATCH_PREFIX", "g3/source/nab/batch").strip("/")
S3_BUCKET = os.environ["S3_BUCKET"]
S3_PREFIX = os.getenv("S3_PREFIX", "g3/0-ai-trust/landing").strip("/")
S3 = boto3.client("s3", region_name=os.getenv("AWS_REGION", "ap-southeast-2"))


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_log(event: str, **metrics) -> None:
    # Never log payload values, PII, credentials, URLs containing credentials, or exception text.
    print(json.dumps({"level": "INFO", "event": event, "timestamp": now_iso(), **metrics}), flush=True)


def publish_file(path: Path) -> str:
    relative = path.relative_to(LANDING_ROOT).as_posix()
    key = f"{S3_PREFIX}/{relative}"
    S3.upload_file(str(path), S3_BUCKET, key, ExtraArgs={"ServerSideEncryption": "AES256"})
    return f"s3://{S3_BUCKET}/{key}"


def artifact(path: Path, record_count: int, native_format: str | None = None) -> dict:
    result = {
        "path": publish_file(path),
        "record_count": record_count,
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
    }
    if native_format:
        result["native_format"] = native_format
    return result


def write_jsonl(source: str, batch_id: str, records: list[dict]) -> dict | None:
    if not records:
        return None
    directory = LANDING_ROOT / "raw" / source
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / f"batch-{batch_id}.jsonl"
    temporary = target.with_suffix(".tmp")
    with temporary.open("w", encoding="utf-8", newline="\n") as handle:
        for record in records:
            handle.write(json.dumps(record, separators=(",", ":"), default=str) + "\n")
    temporary.replace(target)
    return artifact(target, len(records))


def consume(pattern: str, group_id: str, limit: int = 100000) -> tuple[list, Consumer]:
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([pattern])
    messages = []
    idle_polls = 0
    while idle_polls < 5 and len(messages) < limit:
        message = consumer.poll(1.0)
        if message is None:
            idle_polls += 1
            continue
        if message.error():
            safe_log("kafka_message_error", error_code=message.error().code(), consumer_group=group_id)
            continue
        idle_polls = 0
        messages.append(message)
    return messages, consumer


def transport_record(message) -> dict:
    """Retain Kafka transport metadata and payload without interpreting business fields."""
    return {
        "topic": message.topic(),
        "partition": message.partition(),
        "offset": message.offset(),
        "timestamp": message.timestamp()[1],
        "timestamp_type": message.timestamp()[0],
        "key": message.key().decode("utf-8", errors="replace") if message.key() else None,
        "value": message.value().decode("utf-8", errors="replace") if message.value() else None,
        "captured_at": now_iso(),
    }


def export_cdc(batch_id: str) -> dict | None:
    """Land HVR-compatible native CDC envelopes with LSN and Kafka offsets intact."""
    messages, consumer = consume("^nab-cdc\\..*", "zero-ai-trust-cdc-exporter", limit=200000)
    try:
        records = [transport_record(message) for message in messages]
        result = write_jsonl("cdc", batch_id, records)
        if records:
            consumer.commit(asynchronous=False)
        return result
    finally:
        consumer.close()


def export_kafka(batch_id: str) -> dict | None:
    # Subscribe to the enterprise namespace, never a Banker Assist allowlist. Silver owns scope.
    messages, consumer = consume("^nab\\..*", "zero-ai-trust-event-exporter", limit=200000)
    try:
        # Kafka Connect control topics may contain credentials and are never source data.
        source_messages = [m for m in messages if not m.topic().startswith("_nab_connect")]
        records = [transport_record(message) for message in source_messages]
        result = write_jsonl("kafka", batch_id, records)
        if records:
            consumer.commit(asynchronous=False)
        return result
    finally:
        consumer.close()


def export_native_batch(batch_id: str) -> list[dict]:
    """Promote each new immutable CSV object from the producer drop-zone exactly once."""
    directory = LANDING_ROOT / "raw" / "file"
    directory.mkdir(parents=True, exist_ok=True)
    results = []
    paginator = S3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=f"{SOURCE_BATCH_PREFIX}/"):
        for item in page.get("Contents", []):
            key = item["Key"]
            name = Path(key).name
            if not name.lower().endswith(".csv") or name.startswith(("_", ".")):
                continue
            dataset = key[len(SOURCE_BATCH_PREFIX) + 1:].split("/", 1)[0]
            landing_key = f"{S3_PREFIX}/raw/file/{dataset}/{name}"
            try:
                S3.head_object(Bucket=S3_BUCKET, Key=landing_key)
                continue
            except ClientError as exc:
                if exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode") != 404:
                    raise
            target_dir = directory / dataset
            target_dir.mkdir(parents=True, exist_ok=True)
            target = target_dir / name
            S3.download_file(S3_BUCKET, key, str(target))
            line_count = max(0, len(target.read_bytes().splitlines()) - 1)
            results.append(artifact(target, line_count, "CSV"))
    return results


def run_batch() -> None:
    batch_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    artifacts = []
    for operation in (export_cdc, export_kafka, export_native_batch):
        try:
            result = operation(batch_id)
            artifacts.extend(result if isinstance(result, list) else [result] if result else [])
        except Exception as exc:
            safe_log("source_export_failed", source=operation.__name__, error_type=type(exc).__name__)
    manifest = {
        "batch_id": batch_id,
        "created_at": now_iso(),
        "artifacts": artifacts,
        "total_records": sum(item["record_count"] for item in artifacts),
    }
    manifest_dir = LANDING_ROOT / "manifests"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = manifest_dir / f"manifest-{batch_id}.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    publish_file(manifest_path)
    safe_log("microbatch_complete", batch_id=batch_id, artifact_count=len(artifacts), record_count=manifest["total_records"])


if __name__ == "__main__":
    LANDING_ROOT.mkdir(parents=True, exist_ok=True)
    while True:
        run_batch()
        time.sleep(INTERVAL)
