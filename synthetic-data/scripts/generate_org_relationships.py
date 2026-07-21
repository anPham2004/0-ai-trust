"""Generate organisation_party_relationships and organisation_relationships tables.

organisation_party_relationships: models directors, reps, owners per org.
organisation_relationships: models org-to-org links (parent/subsidiary, trust, etc.).

Depends on: customers_fk (step 1), organisations (step 2).
"""
from pyspark.sql import functions as F
from pyspark.sql.window import Window

import config as cfg


# ---------------------------------------------------------------------------
# organisation_party_relationships
# ---------------------------------------------------------------------------

def _generate_party_relationships(spark, cfg):
    N = cfg.N_ORG_PARTY_RELS
    SEED = cfg.SEED

    # Load customer pool for global_id assignment.
    cust = spark.read.parquet(cfg.tmp_path("customers_fk")).withColumn(
        "cust_idx",
        (F.row_number().over(Window.orderBy(F.monotonically_increasing_id())) - 1).cast("int"),
    )

    base = (
        spark.range(0, N, numPartitions=cfg.get_partitions(N))
        .withColumn(
            "cust_idx",
            (F.abs(F.hash(F.col("id"), F.lit(SEED + 520))) % cfg.N_CUSTOMERS).cast("int"),
        )
    )
    base = base.join(cust.select("cust_idx", "global_id"), on="cust_idx", how="left").drop("cust_idx")

    # Deterministic organisationId FK.
    base = base.withColumn(
        "organisationId",
        F.concat(
            F.lit("ORG-"),
            F.lpad(
                (F.abs(F.hash(F.col("id"), F.lit(SEED + 521))) % cfg.N_ORGANISATIONS).cast("string"),
                5, "0",
            ),
        ),
    )

    # partyRole distribution.
    r = F.rand(SEED + 522)
    party_role = (
        F.when(r < 0.25, "DIRECTOR")
         .when(r < 0.50, "AUTHORISED_REPRESENTATIVE")
         .when(r < 0.70, "BENEFICIAL_OWNER")
         .when(r < 0.85, "SIGNATORY")
         .when(r < 0.95, "GUARANTOR")
         .otherwise("SECRETARY")
    )

    is_active = (F.rand(SEED + 523) < 0.85).cast("boolean")

    # authorityLevel distribution.
    r_auth = F.rand(SEED + 524)
    authority = (
        F.when(r_auth < 0.40, "FULL")
         .when(r_auth < 0.75, "LIMITED")
         .otherwise("VIEW_ONLY")
    )

    # Date range: startDate within config window, endDate only when inactive.
    start_epoch = F.lit(cfg.START_DATE.timestamp()).cast("long")
    secs = F.lit((cfg.END_DATE - cfg.START_DATE).total_seconds())
    start_date = F.date_format(
        (start_epoch + (F.rand(SEED + 525) * secs).cast("long")).cast("timestamp"),
        "yyyy-MM-dd",
    )
    end_date = F.when(
        ~F.col("isActive"),
        F.date_format(
            (F.unix_timestamp(F.col("startDate"), "yyyy-MM-dd")
             + (F.rand(SEED + 526) * 180 * 86400).cast("long")).cast("timestamp"),
            "yyyy-MM-dd",
        ),
    ).otherwise(F.lit(None).cast("string"))

    df = (
        base
        .withColumn("relationshipId", F.expr("uuid()"))
        .withColumn("partyRole", party_role)
        .withColumn("isActive", is_active)
        .withColumn("authorityLevel", authority)
        .withColumn("startDate", start_date)
        .withColumn("endDate", end_date)
        .select(
            "global_id", "relationshipId", "organisationId",
            "partyRole", "isActive", "startDate", "endDate", "authorityLevel",
        )
    )

    df.write.mode("overwrite").parquet(f"{cfg.OUTPUT_PATH}/organisation_party_relationships")
    print(f"  organisation_party_relationships: {N:,} rows")


# ---------------------------------------------------------------------------
# organisation_relationships (org-to-org, no global_id)
# ---------------------------------------------------------------------------

def _generate_org_relationships(spark, cfg):
    N = cfg.N_ORG_RELATIONSHIPS
    SEED = cfg.SEED

    base = spark.range(0, N, numPartitions=cfg.get_partitions(N))

    # sourceOrgId and targetOrgId — guaranteed different via +1 offset.
    source = F.concat(
        F.lit("ORG-"),
        F.lpad(
            (F.abs(F.hash(F.col("id"), F.lit(SEED + 530))) % cfg.N_ORGANISATIONS).cast("string"),
            5, "0",
        ),
    )
    target = F.concat(
        F.lit("ORG-"),
        F.lpad(
            (
                (F.abs(F.hash(F.col("id"), F.lit(SEED + 531))) % (cfg.N_ORGANISATIONS - 1)) + 1
            ).cast("string"),
            5, "0",
        ),
    )

    r = F.rand(SEED + 532)
    rel_type = (
        F.when(r < 0.40, "PARENT_SUBSIDIARY")
         .when(r < 0.60, "TRUST_TRUSTEE")
         .when(r < 0.80, "PARTNERSHIP")
         .when(r < 0.90, "FRANCHISE")
         .otherwise("JOINT_VENTURE")
    )

    start_epoch = F.lit(cfg.START_DATE.timestamp()).cast("long")
    secs = F.lit((cfg.END_DATE - cfg.START_DATE).total_seconds())

    df = (
        base
        .withColumn("relationshipId", F.expr("uuid()"))
        .withColumn("sourceOrgId", source)
        .withColumn("targetOrgId", target)
        .withColumn("relationshipType", rel_type)
        .withColumn("isActive", (F.rand(SEED + 533) < 0.90).cast("boolean"))
        .withColumn("startDate", F.date_format(
            (start_epoch + (F.rand(SEED + 534) * secs).cast("long")).cast("timestamp"),
            "yyyy-MM-dd",
        ))
        .select(
            "relationshipId", "sourceOrgId", "targetOrgId",
            "relationshipType", "isActive", "startDate",
        )
    )

    df.write.mode("overwrite").parquet(f"{cfg.OUTPUT_PATH}/organisation_relationships")
    print(f"  organisation_relationships: {N:,} rows")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run(spark, cfg):
    print("=== Organisation Relationships generation ===")
    _generate_party_relationships(spark, cfg)
    _generate_org_relationships(spark, cfg)
    print("=== Organisation Relationships done ===")


if __name__ == "__main__":
    spark = cfg.get_spark()
    run(spark, cfg)
    spark.stop()
