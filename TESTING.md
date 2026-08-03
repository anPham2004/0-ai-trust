# Testing Strategy: Problems, Goals, and What We Built

This document explains the testing work added on top of the existing
`tests/` suite. It follows one simple arc: what was actually wrong with the
tests before, what we needed to fix that, and exactly how we did it. It's
written so anyone picking up the repo later can see the reasoning, not just
the diff.

For context, a good data pipeline generally needs four kinds of tests:
**end-to-end tests** (run the pipeline on known input, compare to an expected
output), **data quality (DQ) tests** (run automatically every time the
pipeline processes data), **monitoring/alerting** (catches abnormal patterns
after the fact, like a sudden drop in row counts), and **unit/contract
tests** (small, fast tests of individual functions and of data shapes coming
from outside systems). This pass covers **DQ tests** and **unit tests**;
end-to-end testing and monitoring/alerting are still open (see the "Still
Missing" section).

## 1. The Problems

An audit of the existing `tests/` folder found eight concrete problems:

1. **The one "unit test" wasn't really testing anything.**
   `tests/unit/test_policy_patterns.py` checked regular expressions for
   detecting emails, phone numbers, and tax file numbers — but those regular
   expressions were typed directly *into the test file itself*, not imported
   from anywhere in the actual pipeline. If the real masking code had a bug,
   or was deleted entirely, this test would still pass, because it was never
   testing the real thing.

2. **The real masking functions had zero test coverage.**
   `masked_email`, `masked_phone`, `masked_identifier`, `masked_card_number`,
   and `masked_amount` in `pipelines/framework/silver_model.py` decide what a
   banker or an AI assistant is allowed to see instead of a customer's real
   email, phone number, or card number. None of them had any test at all.

3. **The data-quality rule compiler was only checked indirectly.**
   `executable_rules()` in `pipelines/framework/data_quality_validator.py` is
   the function that turns a Silver contract's declared quality rules into
   the actual checks Databricks enforces. The existing tests only checked
   that the contract *YAML* looked structurally correct — none of them called
   this function directly with edge cases like a rule at the wrong severity,
   a rule with the wrong `type`, or a rule missing its expression.

4. **One existing test could never fail, even if broken.**
   `contract_fingerprint()` is supposed to detect when a contract's content
   changes. Its only test hashed the *same* JSON object twice and compared
   the two hashes to each other — which will always match, no matter what
   the function does internally.

5. **Security-relevant guards had no test.**
   The contract loader (`framework/data_contract_loader.py`) has a guard that
   should stop it from reading a file outside the `contracts/` folder (a
   path-traversal check), and a rule that rejects `draft` contracts when
   running in a production environment. Neither had ever been tested.

6. **Small but important helper functions had no test.**
   `_hours()`, `_custom_property()`, and `_primary_keys()` in
   `framework/dependency_validator.py` parse contract metadata used for
   cross-entity checks (e.g. "every stage-history record must eventually
   point to a real application"). None of them had a direct test.

7. **The rule compiler had never been run against the real contracts.**
   Even after fixing problem 3, that only proves the compiler works on
   made-up examples — nobody had checked that it actually produces the
   correct result for all 19 real, approved Silver contracts in this repo.

8. **Data-quality enforcement could not be proven to run at all.**
   The project had two well-written SQL files
   (`tests/integration/silver/test_data_quality_edge_cases.sql` and
   `test_scd2_contract.sql`) meant to check things like "no invalid
   application status reached Silver." But `pytest` only ever collects
   `.py` files — it silently ignored these `.sql` files completely, in every
   environment, including CI. The README told a person to run them by hand.
   In practice, nobody could say for certain that data quality was actually
   being enforced; it was an instruction, not a guarantee.

## 2. What We Needed to Achieve

For each problem above, the goal was straightforward:

- Tests must exercise the **real, imported production code** — not a copy of
  it typed into the test file. If someone breaks the real function, the test
  must break too.
- The DQ rule compiler needed **two layers** of proof: synthetic edge-case
  tests (to check its logic in isolation) *and* a test that runs it against
  every one of the 19 real contracts (to check it works on the actual data
  we ship).
- The fingerprint, path-traversal, and status-gating logic needed tests that
  could **actually fail** if the logic broke — not tests that pass by
  construction.
- The small dependency-parsing helpers needed direct coverage so a change to
  contract metadata format doesn't silently break cross-entity checks.
- Data quality enforcement needed to go from "a README instruction someone
  might run" to **something that automatically executes and reports
  pass/fail**, ideally tied to the CI system that already exists in this
  repo.
- All of the above needed to happen **without compromising production code**
  just to make it easier to test. This project is built specifically for
  Databricks and AWS — at one point during this work, we considered changing
  how `pipelines/framework/silver_model.py` reads a secret
  (`pyspark.dbutils.DBUtils`) purely so a test could avoid needing a real
  Databricks environment. That change was reverted: `dbutils` is the
  correct, intended way to read a secret here, and reshaping production code
  just to dodge that dependency would have solved a problem this project
  doesn't have. Instead, the test that needs a real Databricks environment
  simply skips itself with a clear reason when one isn't available, the same
  way the DQ tests below skip without live credentials.

## 3. How We Implemented It

### Unit tests on real code

| File | What it tests | Problem it solves |
|---|---|---|
| `tests/conftest.py` | Nothing directly — it puts `pipelines/` on Python's import path, the same way the deployed pipeline resolves `from framework.x import y`. | Without this, no test could import any pipeline code at all. |
| `pipelines/framework/pii_patterns.py` (new production file) | N/A — this is production code, not a test. | Gives the email/mobile/TFN regular expressions one real, importable home instead of living only inside a test (problem 1). |
| `tests/unit/test_policy_patterns.py` (fixed) | The PII regex patterns. | Now imports from `pii_patterns.py` instead of redefining the same patterns inline — solves problem 1 directly. |
| `tests/unit/test_silver_masking.py` (new) | `masked_email`, `masked_phone`, `masked_identifier`, `masked_card_number`, `masked_amount`, `trimmed`, `hmac_name_token` — imported directly from `pipelines/framework/silver_model.py`. | Solves problem 2. Covers normal cases, `null` handling, malformed input, and exact boundary values (e.g. is `1000` in the `$0-$999` band or the `$1K-$4,999` band?). Because these functions only exist inside a real Databricks environment (see section 2 above), this file **skips itself with a clear reason** when run outside one — a "skipped" result here means "not run in this environment," not "this passed." Running it for real requires Databricks Connect against a live workspace, or execution as a job/notebook on an actual Databricks cluster, and the target workspace's `g3-zero-trust` secret scope must exist. |
| `tests/unit/test_data_quality_validator.py` (new) | `executable_rules()` and `required_field_rules()`, with made-up fixture contracts. | Solves problem 3. Proves: a valid rule at the right severity compiles; a rule at a different severity is quietly excluded (correct, by design); a rule with a bad `type`, `engine`, missing `id`, or missing `expression` raises an error instead of being silently dropped. |
| `tests/unit/test_data_contract_loader.py` (new) | `contract_fingerprint()`, `load_data_contract()`, `load_layer_contract()`. | Solves problems 4 and 5. Replaces the tautological fingerprint check with one that hashes genuinely different content and confirms the hashes differ; adds tests for the path-traversal guard and the draft/approved status gate. |
| `tests/unit/test_dependency_validator.py` (new) | `_hours()`, `_custom_property()`, `_primary_keys()`. | Solves problem 6. |

### Proving data quality actually runs

| File | What it does | Problem it solves |
|---|---|---|
| `tests/unit/test_dq_wiring_all_entities.py` (new) | Loads all 19 approved Silver contracts and runs the real `executable_rules()` compiler against each one, at both `error` and `warning` severity. | Solves problem 7. This is different from `test_data_quality_validator.py`: that file tests the compiler with made-up examples, this one tests it against **every real contract in the repository**, checking that every declared rule actually compiles — nothing gets silently dropped. Runs in milliseconds with no live Databricks connection, so it runs on every commit. |
| `tests/integration/silver/conftest.py` (new) | Shared setup: a fixture that connects to a real Databricks SQL warehouse, plus a rule that automatically skips (not fails) any test tagged `live_dq` if the connection details aren't available. | Ensures missing credentials produce a clean "skipped," not a broken build. |
| `tests/integration/silver/test_data_quality_edge_cases.py` (new) | Reads the existing `test_data_quality_edge_cases.sql` file, splits it into individual checks, and runs each one as its own pytest case against the live warehouse. | Solves problem 8 for this file. The `.sql` file is untouched and stays the single source of truth — this wrapper just makes those exact checks actually execute and actually report pass/fail. |
| `tests/integration/silver/test_scd2_contract.py` (new) | Same idea, for `test_scd2_contract.sql`. | Solves problem 8 for this file. |
| `pytest.ini` (new) | Registers the `live_dq` marker used above. | Silences an "unknown marker" warning; the first `pytest.ini` in the repo. |
| `.github/workflows/dq-check.yml` (new) | A GitHub Actions workflow that runs automatically right after code is synced to Databricks (or manually, on demand), executing the `live_dq`-tagged tests against the live workspace. | This is the piece that fully solves problem 8: a DQ regression in live Silver data now shows up as a red check in GitHub automatically, instead of depending on someone remembering to run a `.sql` file by hand. It deliberately **fails loudly** if its required secrets are missing, instead of quietly reporting "all green" while secretly skipping every test. Requires two new GitHub secrets: `DATABRICKS_SERVER_HOSTNAME` and `DATABRICKS_HTTP_PATH` (`DATABRICKS_TOKEN` likely already exists). |

## 4. Still Missing

For transparency, this pass does **not** cover:

- **End-to-end / golden-output testing** — running the pipeline against
  known fixture input and comparing to a recorded expected output. Does not
  exist yet.
- **Monitoring and alerting** — detecting things like a sudden drop in row
  counts or an unusual spike in quarantined records. Does not exist yet.
- **External-system contract tests** for the Debezium CDC envelope, the
  Kafka event envelope, and the S3 landing manifest shape — dropped from
  this pass by request. Nothing in the repository currently pins down these
  shapes with a schema; Bronze ingestion simply assumes fields like
  `payload.source.lsn` exist, and would silently produce `null` values
  rather than fail loudly if that shape ever changed upstream.

## 5. How to Run All of This

```bash
# Everything that runs without a live Databricks connection.
# The command installs the same dependencies used by Sync to Databricks CI:
python -m pip install -r tests/requirements.txt
make test

# The live DQ checks specifically, once you have Databricks credentials:
export DATABRICKS_SERVER_HOSTNAME=...
export DATABRICKS_HTTP_PATH=...
export DATABRICKS_TOKEN=...
python -m pytest tests -v -m live_dq

# The new scheduled/automatic check:
# fires automatically after "Sync to Databricks" finishes, or run it by hand
# from the GitHub Actions tab ("Live Silver DQ Check" -> Run workflow).

# test_silver_masking.py specifically needs a real Databricks environment
# (see section 2) — run it via Databricks Connect against a workspace, or
# as a job/notebook on an actual Databricks cluster. `make test` / plain
# local pytest will show it as skipped, not passed.
```
