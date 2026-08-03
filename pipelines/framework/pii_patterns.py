"""Canonical PII-detection regex patterns used by scrubbing/masking policy."""

import re

EMAIL = re.compile(r"(?i)\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b")
AU_MOBILE = re.compile(r"(?<!\d)(?:\+?61[ -]?4|04)[ -]?\d{2}[ -]?\d{3}[ -]?\d{3}(?!\d)")
TFN_LIKE = re.compile(r"(?<!\d)\d{3}[ -]?\d{3}[ -]?\d{3}(?!\d)")
