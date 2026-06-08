from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tushare_mirror.disclosure import (
    DISCLOSURE_MATCH_STATUS_VALUES,
    PIT_STRENGTH_VALUES,
    DisclosureEvent,
    disclosure_sources,
    hkex_disclosure_automation_gate,
    load_disclosure_event_schema,
    load_financial_disclosure_sources,
    validate_disclosure_event_schema,
    validate_financial_disclosure_sources,
    validate_pit_strength,
)


class FinancialDisclosureSchemaTests(unittest.TestCase):
    def test_disclosure_event_schema_loads_without_side_effects(self):
        with tempfile.TemporaryDirectory() as tmp:
            before = set(Path(tmp).iterdir())
            schema = load_disclosure_event_schema()
            after = set(Path(tmp).iterdir())

        self.assertEqual(before, after)
        self.assertEqual(schema["schema_version"], "financial-disclosure-event-schema/v1")
        self.assertFalse(schema["durable_storage_enabled"])
        self.assertEqual(set(schema["pit_strength_values"]), PIT_STRENGTH_VALUES)
        self.assertEqual(set(schema["match_status_values"]), DISCLOSURE_MATCH_STATUS_VALUES)
        for field_name in [
            "event_id",
            "market",
            "source",
            "source_status",
            "source_doc_id",
            "source_url",
            "ticker",
            "ts_code",
            "external_id",
            "cik",
            "period",
            "end_date",
            "report_type",
            "form_type",
            "filing_date",
            "accepted_at",
            "disclosure_date",
            "announcement_title",
            "language",
            "match_status",
            "match_confidence",
            "pit_strength",
            "as_filed_value_verified",
            "limitations",
        ]:
            self.assertIn(field_name, schema["required_fields"])
            self.assertIn(field_name, schema["field_definitions"])

    def test_disclosure_event_and_pit_strength_validation(self):
        event = DisclosureEvent(
            event_id="sec_edgar_submissions:NVDA:20241231:10-K:0001045810-25-000023",
            market="us",
            source="sec_edgar_submissions",
            source_status="stable_public_json",
            source_doc_id="0001045810-25-000023",
            source_url="https://www.sec.gov/Archives/edgar/data/1045810/000104581025000023/",
            ticker="NVDA",
            ts_code="NVDA.US",
            external_id="NVDA",
            cik="0001045810",
            period="20241231",
            end_date="20241231",
            report_type="annual",
            form_type="10-K",
            filing_date="20250226",
            accepted_at="2025-02-26T21:36:00Z",
            disclosure_date="20250226",
            announcement_title="Form 10-K",
            language="en",
            match_status="exact",
            match_confidence=1.0,
            pit_strength="availability_only",
            as_filed_value_verified=False,
            limitations=["values are not reconciled to filing facts"],
        )
        payload = event.to_dict()
        self.assertEqual(payload["pit_strength"], "availability_only")
        self.assertEqual(validate_pit_strength("raw_only"), "raw_only")

        with self.assertRaises(ValueError):
            validate_pit_strength("trusted")
        with self.assertRaises(ValueError):
            DisclosureEvent(**{**payload, "match_status": "maybe"})
        with self.assertRaises(ValueError):
            DisclosureEvent(**{**payload, "pit_strength": "as_filed_verified"})

    def test_financial_disclosure_sources_are_conservative(self):
        inventory = load_financial_disclosure_sources()
        self.assertEqual(inventory["source_inventory_version"], "financial-disclosure-sources/v1")
        sources = {source.source_id: source for source in disclosure_sources()}
        self.assertIn("sec_edgar_submissions", sources)
        self.assertIn("sec_companyfacts", sources)
        self.assertIn("hkexnews_advanced_search", sources)

        sec = sources["sec_edgar_submissions"]
        self.assertEqual(sec.source_status, "stable_public_json")
        self.assertTrue(sec.supports_automated_metadata)
        self.assertFalse(sec.supports_value_verification)

        hkex = sources["hkexnews_advanced_search"]
        self.assertEqual(hkex.source_status, "tentative_manual_audit")
        self.assertFalse(hkex.supports_automated_metadata)
        self.assertIn("manual_audit_only", hkex.automation_status)

    def test_hkex_automation_gate_keeps_hk_manual_audit_only(self):
        gate = hkex_disclosure_automation_gate(stock_code="00700", period="20241231", max_requests=2).to_dict()
        self.assertEqual(gate["report_version"], "hkex-disclosure-metadata-probe/v1")
        self.assertEqual(gate["source_status"], "tentative_manual_audit")
        self.assertEqual(gate["automation_status"], "manual_audit_only")
        self.assertTrue(gate["manual_audit_required"])
        self.assertFalse(gate["can_auto_match_disclosure_date"])
        self.assertEqual(gate["match_status"], "source_unavailable")
        self.assertFalse(gate["real_requests_sent"])

    def test_hkex_title_only_match_is_candidate_not_availability(self):
        gate = hkex_disclosure_automation_gate(
            stock_code="00700",
            period="20241231",
            max_requests=1,
            announcement_title="Annual Results Announcement for the Year Ended 31 December 2024",
        ).to_dict()
        self.assertEqual(gate["match_status"], "candidate")
        self.assertFalse(gate["can_auto_match_disclosure_date"])
        self.assertTrue(gate["manual_audit_required"])

    def test_schema_and_source_validation_passes(self):
        self.assertEqual(validate_disclosure_event_schema(), [])
        self.assertEqual(validate_financial_disclosure_sources(), [])


if __name__ == "__main__":
    unittest.main()
