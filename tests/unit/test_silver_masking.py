"""Unit tests for the real Bronze-to-Silver masking/tokenization primitives.

Unlike test_policy_patterns.py (which tests regex literals copy-pasted into
the test file), these import the actual functions from
``framework.silver_model`` and execute them through Spark, so a change to the
real masking logic is what makes these tests fail.

This project is Databricks-native by design: ``framework.silver_model`` uses
``pyspark.dbutils.DBUtils`` to read the HMAC salt from a Databricks secret
scope, and asserts an active Spark session at import time. Neither of those
is available outside a real Databricks environment (a cluster/job, or a
local session connected via Databricks Connect), and that is intentional —
this module has no reason to run anywhere else. Rather than change production
code to make it importable on plain open-source PySpark, this test module
detects that it is not in such an environment and skips itself with a clear
reason, the same way tests/integration/silver's ``live_dq`` tests skip
without Databricks credentials.

Running these for real requires either: (a) Databricks Connect configured
against a workspace, or (b) executing pytest as a job/notebook on a
Databricks cluster. Either way, the target workspace's ``g3-zero-trust``
secret scope (see ``pii_hash_salt`` in ``silver_model.TOKEN_SECRET_SCOPE`` /
``TOKEN_SECRET_KEY``) must exist for the ``TestHmacNameToken`` cases to pass.
"""

import importlib

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.appName("test_silver_masking")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .getOrCreate()
    )
    yield session


@pytest.fixture(scope="module")
def silver_model(spark):
    try:
        return importlib.import_module("framework.silver_model")
    except (ModuleNotFoundError, RuntimeError) as exc:
        pytest.skip(
            "framework.silver_model requires a real Databricks environment "
            f"(pyspark.dbutils + an active Databricks session): {exc}"
        )


def _apply(spark, fn, value):
    dataframe = spark.createDataFrame([(value,)], "input STRING")
    return dataframe.select(fn(dataframe["input"]).alias("output")).collect()[0]["output"]


class TestTrimmed:
    def test_strips_surrounding_whitespace(self, spark, silver_model):
        assert _apply(spark, silver_model.trimmed, "  hello  ") == "hello"

    def test_empty_string_becomes_null(self, spark, silver_model):
        assert _apply(spark, silver_model.trimmed, "") is None

    def test_null_stays_null(self, spark, silver_model):
        assert _apply(spark, silver_model.trimmed, None) is None


class TestMaskedEmail:
    def test_masks_a_valid_email(self, spark, silver_model):
        assert _apply(spark, silver_model.masked_email, "Jane.Doe@Example.com") == "j***@example.com"

    def test_null_email_stays_null(self, spark, silver_model):
        assert _apply(spark, silver_model.masked_email, None) is None

    def test_malformed_email_without_at_sign_masks_to_null(self, spark, silver_model):
        assert _apply(spark, silver_model.masked_email, "not-an-email") is None

    def test_email_with_multiple_at_signs_masks_to_null(self, spark, silver_model):
        assert _apply(spark, silver_model.masked_email, "a@b@c.com") is None


class TestMaskedPhone:
    def test_masks_australian_mobile(self, spark, silver_model):
        assert _apply(spark, silver_model.masked_phone, "0412345678") == "04XX XXX XXX"

    def test_strips_separators_before_masking(self, spark, silver_model):
        assert _apply(spark, silver_model.masked_phone, "04-1234-5678") == "04XX XXX XXX"

    def test_too_short_to_mask_is_null(self, spark, silver_model):
        assert _apply(spark, silver_model.masked_phone, "1") is None

    def test_null_phone_stays_null(self, spark, silver_model):
        assert _apply(spark, silver_model.masked_phone, None) is None


class TestMaskedIdentifier:
    def test_masks_all_but_last_three_characters(self, spark, silver_model):
        assert _apply(spark, silver_model.masked_identifier, "123456789") == "********789"

    def test_internal_whitespace_is_ignored(self, spark, silver_model):
        assert _apply(spark, silver_model.masked_identifier, "123 456 789") == "********789"

    def test_too_short_to_mask_is_null(self, spark, silver_model):
        assert _apply(spark, silver_model.masked_identifier, "ab") is None

    def test_null_identifier_stays_null(self, spark, silver_model):
        assert _apply(spark, silver_model.masked_identifier, None) is None


class TestMaskedCardNumber:
    def test_keeps_last_four_digits(self, spark, silver_model):
        assert _apply(spark, silver_model.masked_card_number, "4111111111111234") == "**** **** **** 1234"

    def test_strips_separators_before_masking(self, spark, silver_model):
        assert _apply(spark, silver_model.masked_card_number, "4111-1111-1111-1234") == "**** **** **** 1234"

    def test_too_short_to_mask_is_null(self, spark, silver_model):
        assert _apply(spark, silver_model.masked_card_number, "123") is None

    def test_null_card_number_stays_null(self, spark, silver_model):
        assert _apply(spark, silver_model.masked_card_number, None) is None


class TestMaskedAmount:
    @pytest.mark.parametrize(
        "value,expected_band",
        [
            ("-50", "NEGATIVE"),
            ("0", "$0-$999"),
            ("999", "$0-$999"),
            ("1000", "$1K-$4,999"),
            ("4999", "$1K-$4,999"),
            ("5000", "$5K-$9,999"),
            ("9999", "$5K-$9,999"),
            ("10000", "$10K-$49,999"),
            ("49999", "$10K-$49,999"),
            ("50000", "$50K-$99,999"),
            ("99999", "$50K-$99,999"),
            ("100000", "$100K-$499,999"),
            ("499999", "$100K-$499,999"),
            ("500000", "$500K-$999,999"),
            ("999999", "$500K-$999,999"),
            ("1000000", "$1M+"),
        ],
    )
    def test_amount_bucket_boundaries(self, spark, silver_model, value, expected_band):
        assert _apply(spark, silver_model.masked_amount, value) == expected_band

    def test_non_numeric_amount_masks_to_null(self, spark, silver_model):
        assert _apply(spark, silver_model.masked_amount, "abc") is None

    def test_null_amount_stays_null(self, spark, silver_model):
        assert _apply(spark, silver_model.masked_amount, None) is None


class TestHmacNameToken:
    """These read the real ``g3-zero-trust``/``pii_hash_salt`` secret via
    ``dbutils``, so the exact token value is workspace-specific and
    deliberately not asserted here — only the invariants a caller relies on.
    """

    def test_is_deterministic_for_the_same_input(self, spark, silver_model):
        first = _apply(spark, silver_model.hmac_name_token, "Jane Doe")
        second = _apply(spark, silver_model.hmac_name_token, "Jane Doe")
        assert first == second

    def test_is_case_and_whitespace_insensitive(self, spark, silver_model):
        canonical = _apply(spark, silver_model.hmac_name_token, "jane doe")
        noisy = _apply(spark, silver_model.hmac_name_token, "  Jane   DOE  ")
        assert canonical == noisy

    def test_different_names_produce_different_tokens(self, spark, silver_model):
        jane = _apply(spark, silver_model.hmac_name_token, "Jane Doe")
        john = _apply(spark, silver_model.hmac_name_token, "John Doe")
        assert jane != john

    def test_null_name_stays_null(self, spark, silver_model):
        assert _apply(spark, silver_model.hmac_name_token, None) is None
