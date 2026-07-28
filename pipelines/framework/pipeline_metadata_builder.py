"""Attach reproducible contract and Zero Trust evidence to curated records."""
from pyspark.sql import DataFrame, functions as F
from framework.data_contract_loader import contract_fingerprint


def with_contract_metadata(dataframe: DataFrame, contract: dict) -> DataFrame:
    limitations = contract.get("known_source_limitations", [])
    limitation_array = (
        F.array(*[F.lit(value) for value in limitations])
        if limitations
        else F.array().cast("array<string>")
    )
    return (
        dataframe
        .withColumn("contract_name", F.lit(contract.get("product")))
        .withColumn("contract_version", F.lit(str(contract.get("version", "UNVERSIONED"))))
        .withColumn("contract_hash", F.lit(contract_fingerprint(contract)))
        .withColumn("known_limitations", limitation_array)
        .withColumn("quality_evaluated_at", F.current_timestamp())
    )
