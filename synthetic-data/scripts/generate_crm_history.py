"""Generate CRM history tables: application_stage_history + status_change_history.

Reads global_id + applicationId + finalOutcome + submittedAt from
{CATALOG}.{SCHEMA}._tmp_applications (written by BPI script).
Writes Parquet to OUTPUT_PATH.
Also writes application_stage_history to {CATALOG}.{SCHEMA}._tmp_stage_history Delta.
"""
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
import pandas as pd


# ---------------------------------------------------------------------------
# Stage metadata (module-level constants — no UDF scope leak)
# ---------------------------------------------------------------------------

# Stage sequences keyed by finalOutcome (index = position in flow)
_STAGE_SEQUENCES = {
    "Accepted_CreditCheck":         ["Submitted", "Document_Verification", "Assessment", "Credit_Check", "Approved"],
    "Denied_CreditCheck":           ["Submitted", "Document_Verification", "Assessment", "Credit_Check", "Denied"],
    "Conditionally_Approved":       ["Submitted", "Document_Verification", "Assessment", "Conditionally_Approved", "Document_Verification", "Approved"],
    "Withdrawn":                    ["Submitted", "Document_Verification", "Assessment", "Withdrawn"],
    "Cancelled":                    ["Submitted", "Document_Verification", "Assessment", "Cancelled"],
}

_STAGE_SLA_DAYS = {
    "Submitted": 2, "Document_Verification": 7, "Assessment": 5,
    "Credit_Check": 3, "Approved": 1, "Denied": 1,
    "Withdrawn": 1, "Cancelled": 1, "Conditionally_Approved": 1,
}

_STAGE_TEAM = {
    "Submitted": "Intake",
    "Document_Verification": "Review",
    "Assessment": "Underwriting",
    "Credit_Check": "Underwriting",
    "Approved": "Compliance", "Denied": "Compliance",
    "Withdrawn": "Compliance", "Cancelled": "Compliance",
    "Conditionally_Approved": "Compliance",
}

_TEAM_STAFF_RANGE = {
    "Intake": (1, 10), "Review": (11, 25),
    "Underwriting": (26, 45), "Compliance": (46, 60),
}

_TRANSITION_REASON = {
    "Document_Verification": ["auto_progression"],
    "Assessment":            ["auto_progression"],
    "Credit_Check":          ["auto_progression", "additional_info_required"],
    "Approved":              ["approved_by_officer"],
    "Denied":                ["credit_fail", "credit_fail", "credit_fail", "document_rejected", "document_rejected"],
    "Withdrawn":             ["withdrawn_by_customer"],
    "Cancelled":             ["customer_request"],
    "Conditionally_Approved": ["additional_info_required"],
}

# Stage duration log-normal params (μ, σ) in days
_STAGE_DURATION = {
    "Submitted": (1.0, 0.3), "Document_Verification": (5.0, 1.5),
    "Assessment": (4.0, 1.2), "Credit_Check": (2.0, 0.6),
}
_DEFAULT_DURATION = (1.0, 0.3)


# ---------------------------------------------------------------------------
# Pandas UDFs
# ---------------------------------------------------------------------------

@F.pandas_udf(StringType())
def _udf_pick_sequence(outcome_series: pd.Series) -> pd.Series:
    """Map finalOutcome + hash to a stage sequence key."""
    import numpy as np
    results = []
    keys = list(_STAGE_SEQUENCES.keys())
    outcome_map = {
        "Accepted":  ["Accepted_CreditCheck", "Conditionally_Approved"],
        "Denied":    ["Denied_CreditCheck"],
        "Cancelled": ["Withdrawn", "Cancelled"],
    }
    rng = np.random.default_rng(42)
    for outcome in outcome_series:
        choices = outcome_map.get(outcome, ["Accepted_CreditCheck"])
        results.append(rng.choice(choices))
    return pd.Series(results)


@F.pandas_udf(StringType())
def _udf_stage_from_pos(seq_key_series: pd.Series, pos_series: pd.Series) -> pd.Series:
    """Return stage name at position pos (0-based, clamped to sequence length)."""
    results = []
    for seq_key, pos in zip(seq_key_series, pos_series):
        seq = _STAGE_SEQUENCES.get(seq_key, ["Submitted"])
        idx = min(int(pos), len(seq) - 1)
        results.append(seq[idx])
    return pd.Series(results)


@F.pandas_udf(StringType())
def _udf_seq_len(seq_key_series: pd.Series) -> pd.Series:
    """Return total stages in sequence."""
    return pd.Series([str(len(_STAGE_SEQUENCES.get(k, ["Submitted"]))) for k in seq_key_series])


@F.pandas_udf(StringType())
def _udf_entered_at(submitted_series: pd.Series, stage_series: pd.Series, pos_series: pd.Series) -> pd.Series:
    """enteredAt = submittedAt + cumulative duration of prior stages (log-normal)."""
    import numpy as np
    from datetime import datetime, timedelta
    results = []
    for ts_str, stage, pos in zip(submitted_series, stage_series, pos_series):
        try:
            base = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
            offset = 0.0
            # Sum durations for positions 0..(pos-1); use stage at each position approximated
            for _ in range(int(pos)):
                mu, sigma = _STAGE_DURATION.get(stage, _DEFAULT_DURATION)
                offset += max(0.1, np.random.lognormal(mean=np.log(mu), sigma=sigma))
            results.append((base + timedelta(days=offset)).strftime("%Y-%m-%dT%H:%M:%S"))
        except Exception:
            results.append(ts_str)
    return pd.Series(results)


@F.pandas_udf(StringType())
def _udf_sla_deadline(entered_series: pd.Series, stage_series: pd.Series) -> pd.Series:
    """slaDeadline = enteredAt + SLA days for stage."""
    from datetime import datetime, timedelta
    results = []
    for ts_str, stage in zip(entered_series, stage_series):
        try:
            base = datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S")
            days = _STAGE_SLA_DAYS.get(stage, 1)
            results.append((base + timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%S"))
        except Exception:
            results.append(ts_str)
    return pd.Series(results)


@F.pandas_udf(StringType())
def _udf_assigned_team(stage_series: pd.Series) -> pd.Series:
    return pd.Series([_STAGE_TEAM.get(s, "Compliance") for s in stage_series])


@F.pandas_udf(StringType())
def _udf_assigned_to(team_series: pd.Series, id_series: pd.Series) -> pd.Series:
    import numpy as np
    results = []
    for team, row_id in zip(team_series, id_series):
        lo, hi = _TEAM_STAFF_RANGE.get(team, (1, 60))
        idx = int(np.abs(hash((team, int(row_id)))) % (hi - lo + 1)) + lo
        results.append(f"STAFF-{idx:03d}")
    return pd.Series(results)


@F.pandas_udf(StringType())
def _udf_transition_reason(new_stage_series: pd.Series, id_series: pd.Series) -> pd.Series:
    import numpy as np
    results = []
    for stage, row_id in zip(new_stage_series, id_series):
        choices = _TRANSITION_REASON.get(stage, ["auto_progression"])
        idx = int(np.abs(hash((stage, int(row_id)))) % len(choices))
        results.append(choices[idx])
    return pd.Series(results)


# ---------------------------------------------------------------------------
# application_stage_history
# ---------------------------------------------------------------------------

def _generate_stage_history(spark, cfg):
    N = cfg.N_STAGE_HISTORY
    SEED = cfg.SEED
    N_APPS = cfg.N_APPLICATIONS
    PARTITIONS = cfg.get_partitions(N)

    apps = spark.read.parquet(cfg.tmp_path("applications_fk"))
    apps_idx = apps.withColumn(
        "_app_pool_idx",
        (F.row_number().over(Window.orderBy(F.monotonically_increasing_id())) - 1).cast("int")
    )

    base = (
        spark.range(0, N, numPartitions=PARTITIONS)
        .withColumn("_fk_idx", (F.abs(F.hash(F.col("id"), F.lit(SEED + 30))) % N_APPS).cast("int"))
        # stage position 0–5 within this application row
        .withColumn("_pos", (F.abs(F.hash(F.col("id"), F.lit(SEED + 31))) % 5).cast("int"))
    )

    stage_fk = (
        base
        .join(
            apps_idx.select("_app_pool_idx", "global_id", "applicationId", "finalOutcome", "submittedAt"),
            base._fk_idx == apps_idx._app_pool_idx,
            "left"
        )
        .drop("_fk_idx", "_app_pool_idx")
    )

    # Map finalOutcome → sequence key
    stage_fk = stage_fk.withColumn("_seq_key", _udf_pick_sequence(F.col("finalOutcome")))

    # Total stages in sequence
    stage_fk = stage_fk.withColumn("_seq_len", _udf_seq_len(F.col("_seq_key")).cast("int"))

    # Clamp position to actual sequence length
    stage_fk = stage_fk.withColumn("_pos", F.least(F.col("_pos"), F.col("_seq_len") - F.lit(1)))

    # Stage name at position
    stage_fk = stage_fk.withColumn("stage", _udf_stage_from_pos(F.col("_seq_key"), F.col("_pos")))

    # enteredAt
    stage_fk = stage_fk.withColumn("enteredAt", _udf_entered_at(F.col("submittedAt"), F.col("stage"), F.col("_pos")))

    # exitedAt: null if last stage in sequence, else enteredAt + stage duration
    # Approximate: if pos == seq_len - 1 → null, else add SLA days as proxy
    stage_fk = stage_fk.withColumn(
        "exitedAt",
        F.when(
            F.col("_pos") >= (F.col("_seq_len") - 1),
            F.lit(None).cast("string")
        ).otherwise(
            _udf_sla_deadline(F.col("enteredAt"), F.col("stage"))
        )
    )

    # assignedTeam, assignedTo, slaDeadline
    stage_fk = stage_fk.withColumn("assignedTeam", _udf_assigned_team(F.col("stage")))
    stage_fk = stage_fk.withColumn("assignedTo", _udf_assigned_to(F.col("assignedTeam"), F.col("id")))
    stage_fk = stage_fk.withColumn("slaDeadline", _udf_sla_deadline(F.col("enteredAt"), F.col("stage")))

    # pendingActionParty: who is responsible for the next action at this stage.
    SEED = cfg.SEED
    r_pap = F.rand(SEED + 515)
    stage_fk = stage_fk.withColumn(
        "pendingActionParty",
        F.when(F.col("stage").isin("Submitted", "Document_Verification"),
            F.when(r_pap < 0.40, "CUSTOMER")
             .when(r_pap < 0.60, "ORGANISATION")
             .when(r_pap < 0.75, "AUTHORISED_REPRESENTATIVE")
             .when(r_pap < 0.85, "DIRECTOR")
             .when(r_pap < 0.92, "GUARANTOR")
             .otherwise("THIRD_PARTY"))
        .when(F.col("stage").isin("Assessment", "Credit_Check"),
            F.when(r_pap < 0.75, "INTERNAL_TEAM")
             .when(r_pap < 0.90, "THIRD_PARTY")
             .otherwise("CUSTOMER"))
        .when(F.col("stage").isin("Approved", "Settlement"),
            F.when(r_pap < 0.50, "INTERNAL_TEAM")
             .when(r_pap < 0.70, "CUSTOMER")
             .when(r_pap < 0.85, "ORGANISATION")
             .otherwise("THIRD_PARTY"))
        .otherwise(F.lit(None).cast("string"))
    )

    stage_final = stage_fk.select(
        F.expr("uuid()").alias("historyId"),
        "global_id", "applicationId", "stage",
        "enteredAt", "exitedAt", "assignedTeam", "assignedTo", "slaDeadline",
        "pendingActionParty",
    )

    out_path = f"{cfg.OUTPUT_PATH}/application_stage_history"
    stage_final.write.mode("overwrite").parquet(out_path)
    print(f"  application_stage_history ({N:,} rows) → {out_path}")


# ---------------------------------------------------------------------------
# status_change_history
# ---------------------------------------------------------------------------

def _generate_status_changes(spark, cfg):
    N = cfg.N_STATUS_CHANGES
    SEED = cfg.SEED
    N_APPS = cfg.N_APPLICATIONS
    PARTITIONS = cfg.get_partitions(N)

    apps = spark.read.parquet(cfg.tmp_path("applications_fk"))
    apps_idx = apps.withColumn(
        "_app_pool_idx",
        (F.row_number().over(Window.orderBy(F.monotonically_increasing_id())) - 1).cast("int")
    )

    base = (
        spark.range(0, N, numPartitions=PARTITIONS)
        .withColumn("_fk_idx", (F.abs(F.hash(F.col("id"), F.lit(SEED + 40))) % N_APPS).cast("int"))
        .withColumn("_pos", (F.abs(F.hash(F.col("id"), F.lit(SEED + 41))) % 4 + 1).cast("int"))
    )

    change_fk = (
        base
        .join(
            apps_idx.select("_app_pool_idx", "global_id", "applicationId", "finalOutcome", "submittedAt"),
            base._fk_idx == apps_idx._app_pool_idx,
            "left"
        )
        .drop("_fk_idx", "_app_pool_idx")
    )

    # Derive sequence key and old/new stages from position
    change_fk = change_fk.withColumn("_seq_key", _udf_pick_sequence(F.col("finalOutcome")))
    change_fk = change_fk.withColumn("_seq_len", _udf_seq_len(F.col("_seq_key")).cast("int"))
    change_fk = change_fk.withColumn("_pos", F.least(F.col("_pos"), F.col("_seq_len") - F.lit(1)))

    # oldStatus = stage at pos-1, newStatus = stage at pos
    change_fk = change_fk.withColumn(
        "oldStatus",
        _udf_stage_from_pos(F.col("_seq_key"), (F.col("_pos") - F.lit(1)).cast("int"))
    )
    change_fk = change_fk.withColumn("newStatus", _udf_stage_from_pos(F.col("_seq_key"), F.col("_pos")))

    # changedAt = submittedAt + cumulative offset for pos-1 stages
    change_fk = change_fk.withColumn(
        "changedAt",
        _udf_entered_at(F.col("submittedAt"), F.col("oldStatus"), (F.col("_pos") - F.lit(1)).cast("int"))
    )

    # changedBy: staff correlated with oldStatus team
    change_fk = change_fk.withColumn("_team", _udf_assigned_team(F.col("oldStatus")))
    change_fk = change_fk.withColumn("changedBy", _udf_assigned_to(F.col("_team"), F.col("id")))

    # reason: correlated with newStatus
    change_fk = change_fk.withColumn("reason", _udf_transition_reason(F.col("newStatus"), F.col("id")))

    change_final = change_fk.select(
        F.expr("uuid()").alias("changeId"),
        "global_id", "applicationId",
        "oldStatus", "newStatus", "changedAt", "changedBy", "reason",
    )

    out_path = f"{cfg.OUTPUT_PATH}/status_change_history"
    change_final.write.mode("overwrite").parquet(out_path)
    print(f"  status_change_history ({N:,} rows) → {out_path}")


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------

def run(spark, cfg):
    print("\n=== CRM: application_stage_history ===")
    _generate_stage_history(spark, cfg)
    print("\n=== CRM: status_change_history ===")
    _generate_status_changes(spark, cfg)
    print("\n=== CRM history generation complete ===")


if __name__ == "__main__":
    import config as cfg

    spark = cfg.get_spark()
    run(spark, cfg)
    spark.stop()
