"""Register shared contract-validation inputs and quarantine targets once."""

from framework.quarantine import register_record_quarantine_target
from framework.silver_model import register_source_validations


register_record_quarantine_target()
register_source_validations()
