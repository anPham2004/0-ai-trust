import re
import unittest


EMAIL = re.compile(r"(?i)\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b")
AU_MOBILE = re.compile(r"(?<!\d)(?:\+?61[ -]?4|04)[ -]?\d{2}[ -]?\d{3}[ -]?\d{3}(?!\d)")
TFN_LIKE = re.compile(r"(?<!\d)\d{3}[ -]?\d{3}[ -]?\d{3}(?!\d)")


class PolicyPatternTests(unittest.TestCase):
    def test_email_is_detected_for_scrubbing(self):
        self.assertIsNotNone(EMAIL.search("Contact support@example.com today"))

    def test_australian_mobile_is_detected_for_scrubbing(self):
        self.assertIsNotNone(AU_MOBILE.search("Call 0412 345 678"))

    def test_tfn_like_value_is_a_hard_failure(self):
        self.assertIsNotNone(TFN_LIKE.search("Applicant wrote 123 456 782"))

    def test_amount_does_not_look_like_tfn(self):
        self.assertIsNone(TFN_LIKE.search("Loan amount is 650000.00"))


if __name__ == "__main__":
    unittest.main()

