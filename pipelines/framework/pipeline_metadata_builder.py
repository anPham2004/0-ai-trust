"""Attach reproducible contract and Zero Trust evidence to governed records."""

from pyspark.sql import DataFrame, functions as F

from framework.data_contract_loader import contract_fingerprint


def with_contract_metadata(dataframe: DataFrame, contract: dict) -> DataFrame:
    # DCS v3.1.0: known limitations live in customProperties under g3:knownSourceLimitations
    limitations = next(
        (p["value"] for p in contract.get("customProperties", [])
         if p.get("property") == "g3:knownSourceLimitations"),
        []
    )
    limitation_array = (
        F.array(*[F.lit(value) for value in limitations])
        if limitations
        else F.array().cast("array<string>")
    )
    return (
        dataframe
        .withColumn("contract_name", F.lit(contract.get("name")))
        .withColumn("contract_version", F.lit(str(contract.get("version", "UNVERSIONED"))))
        .withColumn("contract_hash", F.lit(contract_fingerprint(contract)))
        .withColumn("known_limitations", limitation_array)
        .withColumn("quality_evaluated_at", F.current_timestamp())
    )
