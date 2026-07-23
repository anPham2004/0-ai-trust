"""Refresh intervals shared by future Silver and Gold dataset declarations."""

DOWNSTREAM_TRIGGER_INTERVAL = "15 minutes"


def downstream_microbatch_spark_conf() -> dict[str, str]:
    """Return the per-dataset Spark configuration for downstream micro-batches."""
    return {"pipelines.trigger.interval": DOWNSTREAM_TRIGGER_INTERVAL}
