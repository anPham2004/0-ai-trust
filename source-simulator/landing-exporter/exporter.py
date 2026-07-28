import hashlib
import json
import os
import re
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
S3_PREFIX = os.getenv("S3_PREFIX", "g3/0-ai-trust/bronze/landing").strip("/")
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


def artifact(
    path: Path,
    record_count: int,
    native_format: str | None = None,
    **metadata,
) -> dict:
    result = {
        "path": publish_file(path),
        "record_count": record_count,
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
    }
    if native_format:
        result["native_format"] = native_format
    result.update({key: value for key, value in metadata.items() if value is not None})
    return result


def dataset_slug(value: str) -> str:
    return re.sub(r"[^a-z0-9_]+", "__", value.lower()).strip("_") or "unknown_dataset"


def payload(record: dict) -> dict:
    value = record.get("payload")
    return value if isinstance(value, dict) else {}


def cdc_dataset(record: dict) -> str:
    source = payload(record).get("source") or {}
    qualified = ".".join(filter(None, (source.get("schema"), source.get("table"))))
    return dataset_slug(qualified or record.get("topic") or "unknown_cdc")


def event_dataset(record: dict) -> str:
    value = payload(record)
    return dataset_slug(value.get("source_dataset") or record.get("topic") or "unknown_event")


def transport_watermarks(records: list[dict]) -> dict:
    offsets = [record["offset"] for record in records if record.get("offset") is not None]
    lsns = []
    for record in records:
        source_lsn = (payload(record).get("source") or {}).get("lsn")
        if source_lsn is not None:
            lsns.append(int(source_lsn))
    return {
        "min_kafka_offset": min(offsets) if offsets else None,
        "max_kafka_offset": max(offsets) if offsets else None,
        "min_source_lsn": min(lsns) if lsns else None,
        "max_source_lsn": max(lsns) if lsns else None,
        "topics": sorted({record["topic"] for record in records if record.get("topic")}),
    }


def write_jsonl(source: str, dataset: str, batch_id: str, records: list[dict]) -> dict | None:
    if not records:
        return None
    directory = LANDING_ROOT / source / dataset
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / f"batch-{batch_id}.jsonl"
    temporary = target.with_suffix(".tmp")
    with temporary.open("w", encoding="utf-8", newline="\n") as handle:
        for record in records:
            handle.write(
                json.dumps({**record, "batch_id": batch_id}, separators=(",", ":"), default=str)
                + "\n"
            )
    temporary.replace(target)
    return artifact(
        target,
        len(records),
        "JSONL",
        source_type=source.upper(),
        source_dataset=dataset,
        **transport_watermarks(records),
    )


def write_grouped_jsonl(source: str, batch_id: str, records: list[dict], classifier) -> list[dict]:
    grouped = {}
    for record in records:
        grouped.setdefault(classifier(record), []).append(record)
    return [
        result
        for dataset, dataset_records in sorted(grouped.items())
        if (result := write_jsonl(source, dataset, batch_id, dataset_records)) is not None
    ]


def create_consumer(pattern: str, group_id: str) -> Consumer:
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([pattern])
    return consumer


def consume(consumer: Consumer, limit: int = 100000) -> list:
    messages = []
    idle_polls = 0
    assignment_deadline = time.monotonic() + 60
    while idle_polls < 5 and len(messages) < limit:
        message = consumer.poll(1.0)
        if message is None:
            # poll() also drives consumer-group coordination. Do not treat the
            # initial join/rebalance period as an empty source micro-batch.
            if not consumer.assignment() and time.monotonic() < assignment_deadline:
                continue
            idle_polls += 1
            continue
        if message.error():
            safe_log("kafka_message_error", error_code=message.error().code())
            continue
        idle_polls = 0
        messages.append(message)
    return messages


def transport_record(message) -> dict:
    """Retain transport metadata and expose the JSON value as a nested object."""
    parse_error = None
    try:
        decoded = message.value().decode("utf-8", errors="strict") if message.value() else None
        parsed = json.loads(decoded) if decoded else None
    except (UnicodeDecodeError, json.JSONDecodeError):
        decoded = message.value().decode("utf-8", errors="replace") if message.value() else None
        parsed = None
        parse_error = "INVALID_JSON"
    record = {
        "topic": message.topic(),
        "partition": message.partition(),
        "offset": message.offset(),
        "timestamp": message.timestamp()[1],
        "timestamp_type": message.timestamp()[0],
        "key": message.key().decode("utf-8", errors="replace") if message.key() else None,
        "payload": parsed,
        "captured_at": now_iso(),
    }
    if parse_error:
        record.update({"parse_error": parse_error, "raw_value": decoded})
    return record


def export_cdc(batch_id: str, consumer: Consumer) -> list[dict]:
    """Land HVR-compatible native CDC envelopes with LSN and Kafka offsets intact."""
    messages = consume(consumer, limit=200000)
    records, invalid = [], []
    for message in messages:
        record = transport_record(message)
        envelope = payload(record)
        # Debezium tombstones carry no business image. The preceding delete
        # envelope is the replayable source record and is retained.
        if record.get("parse_error"):
            invalid.append(record)
            continue
        if not envelope:  # Debezium tombstone following a retained delete envelope.
            continue
        if not (envelope.get("source") or {}).get("table"):
            record["parse_error"] = "UNCLASSIFIED_CDC_ENVELOPE"
            invalid.append(record)
            continue
        record["record"] = envelope.get("after") or envelope.get("before")
        record["load_type"] = "INITIAL" if envelope.get("op") == "r" else "INCREMENTAL"
        records.append(record)
    results = write_grouped_jsonl("cdc", batch_id, records, cdc_dataset)
    if invalid_result := write_jsonl("quarantine", "cdc", batch_id, invalid):
        results.append(invalid_result)
    if messages:
        consumer.commit(asynchronous=False)
    return results


def export_events(batch_id: str, consumer: Consumer) -> list[dict]:
    # Subscribe to the enterprise namespace, never a Banker Assist allowlist. Silver owns scope.
    messages = consume(consumer, limit=200000)
    records, invalid = [], []
    for message in messages:
        record = transport_record(message)
        envelope = payload(record)
        if record.get("parse_error"):
            invalid.append(record)
            continue
        if not envelope.get("source_dataset") or not isinstance(envelope.get("payload"), dict):
            record["parse_error"] = "UNCLASSIFIED_EVENT_ENVELOPE"
            invalid.append(record)
            continue
        record["record"] = envelope["payload"]
        record["load_type"] = envelope.get("load_type") or "INCREMENTAL"
        records.append(record)
    results = write_grouped_jsonl("event", batch_id, records, event_dataset)
    if invalid_result := write_jsonl("quarantine", "event", batch_id, invalid):
        results.append(invalid_result)
    if messages:
        consumer.commit(asynchronous=False)
    return results


def export_native_batch(batch_id: str) -> list[dict]:
    """Promote each new immutable CSV object from the producer drop-zone exactly once."""
    directory = LANDING_ROOT / "file"
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
            landing_key = f"{S3_PREFIX}/file/{dataset}/{name}"
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
            results.append(
                artifact(
                    target,
                    line_count,
                    "CSV",
                    source_type="FILE",
                    source_dataset=dataset_slug(dataset),
                )
            )
    return results


def run_batch(cdc_consumer: Consumer, event_consumer: Consumer) -> None:
    batch_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    artifacts = []
    operations = (
        (export_cdc, (batch_id, cdc_consumer)),
        (export_events, (batch_id, event_consumer)),
        (export_native_batch, (batch_id,)),
    )
    for operation, arguments in operations:
        try:
            result = operation(*arguments)
            artifacts.extend(result if isinstance(result, list) else [result] if result else [])
        except Exception as exc:
            safe_log("source_export_failed", source=operation.__name__, error_type=type(exc).__name__)
    manifest = {
        "manifest_version": "1.0",
        "batch_id": batch_id,
        "created_at": now_iso(),
        "heartbeat_status": "HEALTHY",
        "artifacts": artifacts,
        "artifact_count": len(artifacts),
        "total_records": sum(item["record_count"] for item in artifacts),
        "source_record_counts": {
            source_type: sum(
                item["record_count"]
                for item in artifacts
                if item.get("source_type") == source_type
            )
            for source_type in ("CDC", "EVENT", "FILE")
        },
    }
    manifest_dir = LANDING_ROOT / "manifests"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = manifest_dir / f"manifest-{batch_id}.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    publish_file(manifest_path)
    safe_log("microbatch_complete", batch_id=batch_id, artifact_count=len(artifacts), record_count=manifest["total_records"])


if __name__ == "__main__":
    LANDING_ROOT.mkdir(parents=True, exist_ok=True)
    cdc_consumer = create_consumer("^nab-cdc\\..*", "zero-ai-trust-cdc-exporter")
    event_consumer = create_consumer("^nab\\..*", "zero-ai-trust-event-exporter")
    try:
        while True:
            run_batch(cdc_consumer, event_consumer)
            time.sleep(INTERVAL)
    finally:
        cdc_consumer.close()
        event_consumer.close()
