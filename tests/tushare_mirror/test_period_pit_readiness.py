from __future__ import annotations

import json
import unittest

from tushare_mirror.periods import (
    MAX_PERIODS,
    PeriodRangePlanner,
    compare_periods,
    normalize_period,
    normalize_period_list,
    period_sort_key,
    period_year,
)
from tushare_mirror.pit import pit_metadata_from_config, validate_pit_safety


class PeriodPlanningUtilityTests(unittest.TestCase):
    def test_parse_supported_quarter_and_end_date_forms(self):
        cases = {
            "2024Q1": "20240331",
            "2024Q2": "20240630",
            "2024Q3": "20240930",
            "2024Q4": "20241231",
            "20240331": "20240331",
            "20240630": "20240630",
            "20240930": "20240930",
            "20241231": "20241231",
        }
        for raw, expected in cases.items():
            with self.subTest(raw=raw):
                self.assertEqual(normalize_period(raw), expected)
                self.assertEqual(period_year(raw), 2024)

    def test_invalid_periods_are_rejected(self):
        for raw in ["2024Q5", "20240101", "2024", "Q12024", "", "20241331"]:
            with self.subTest(raw=raw):
                with self.assertRaises(ValueError):
                    normalize_period(raw)

    def test_quarterly_range_generation_is_ordered_and_bounded(self):
        plan = PeriodRangePlanner().plan(
            start_period="2024Q1",
            end_period="2024Q4",
            period_frequency="quarterly",
            max_periods=4,
        )
        self.assertEqual(plan.periods, ["20240331", "20240630", "20240930", "20241231"])
        self.assertEqual(plan.total_periods, 4)
        self.assertEqual(plan.planned_periods, 4)
        self.assertFalse(plan.truncated_by_max_periods)

    def test_annual_range_generation_uses_year_end_periods(self):
        plan = PeriodRangePlanner().plan(
            start_period="2022Q1",
            end_period="2024Q4",
            period_frequency="annual",
            max_periods=3,
        )
        self.assertEqual(plan.periods, ["20221231", "20231231", "20241231"])
        self.assertEqual(plan.frequency, "annual")

    def test_max_periods_limit_and_truncation(self):
        plan = PeriodRangePlanner().plan(
            start_period="2020Q1",
            end_period="2026Q4",
            period_frequency="annual",
            max_periods=3,
        )
        self.assertEqual(plan.total_periods, 7)
        self.assertEqual(plan.planned_periods, 3)
        self.assertEqual(plan.periods, ["20201231", "20211231", "20221231"])
        self.assertTrue(plan.truncated_by_max_periods)
        with self.assertRaises(ValueError):
            PeriodRangePlanner().plan(
                start_period="2024Q1",
                end_period="2024Q4",
                max_periods=MAX_PERIODS + 1,
            )

    def test_explicit_period_list_dedupes_and_sorts_stably(self):
        self.assertEqual(
            normalize_period_list("2024Q4,20240331,2024Q2,20240331"),
            ["20240331", "20240630", "20241231"],
        )
        self.assertLess(compare_periods("2024Q1", "2024Q2"), 0)
        self.assertEqual(period_sort_key("2024Q3"), (2024, 3))

    def test_period_plan_json_serialization_is_stable(self):
        payload = PeriodRangePlanner().plan(
            periods="2024Q2,2024Q1",
            max_periods=2,
        ).to_dict()
        rendered = json.dumps(payload, sort_keys=True)
        self.assertEqual(payload["periods"], ["20240331", "20240630"])
        self.assertIn('"frequency": "explicit"', rendered)


class PITSafetyMetadataTests(unittest.TestCase):
    def test_valid_pit_metadata_is_complete_and_json_stable(self):
        cfg = {
            "api_name": "income",
            "endpoint_kind": "financial_statement",
            "pit_safety": {
                "pit_required": True,
                "period_field": "period",
                "announcement_date_fields": ["ann_date", "f_ann_date"],
                "usable_after_field": "ann_date",
                "fallback_usable_after_policy": "block_without_disclosure_date",
                "allow_without_disclosure_date": False,
                "lookahead_risk": True,
                "strategy_safe_default": False,
            },
        }
        result = validate_pit_safety(cfg)
        self.assertEqual(result.status, "complete")
        self.assertTrue(result.pit_required)
        self.assertFalse(result.metadata.allow_without_disclosure_date)
        rendered = json.dumps(result.to_dict(), sort_keys=True)
        self.assertIn('"period_field": "period"', rendered)
        self.assertIn('"usable_after_field": "ann_date"', rendered)

    def test_missing_period_field_blocks(self):
        result = validate_pit_safety(
            {
                "api_name": "income",
                "endpoint_kind": "financial_statement",
                "pit_safety": {
                    "pit_required": True,
                    "announcement_date_fields": ["ann_date"],
                    "usable_after_field": "ann_date",
                },
            }
        )
        self.assertEqual(result.status, "blocked")
        self.assertIn("missing_period_field", result.errors)

    def test_missing_announcement_date_blocks(self):
        result = validate_pit_safety(
            {
                "api_name": "fina_indicator",
                "endpoint_kind": "financial_indicator",
                "pit_safety": {
                    "pit_required": True,
                    "period_field": "period",
                    "usable_after_field": "ann_date",
                },
            }
        )
        self.assertEqual(result.status, "blocked")
        self.assertIn("missing_announcement_date_fields", result.errors)

    def test_default_allow_without_disclosure_date_is_false(self):
        metadata = pit_metadata_from_config(
            {
                "api_name": "income",
                "endpoint_kind": "financial_statement",
                "pit_safety": {
                    "pit_required": True,
                    "period_field": "period",
                    "announcement_date_fields": ["ann_date"],
                    "usable_after_field": "ann_date",
                },
            }
        )
        self.assertFalse(metadata.allow_without_disclosure_date)
        self.assertFalse(metadata.strategy_safe_default)

    def test_unknown_pit_safety_blocks_financial_endpoint(self):
        result = validate_pit_safety(
            {
                "api_name": "income",
                "endpoint_kind": "financial_statement",
            }
        )
        self.assertEqual(result.status, "blocked")
        self.assertIn("unknown_pit_safety", result.errors)
        self.assertIn("missing_period_field", result.errors)
        self.assertIn("missing_announcement_date_fields", result.errors)
        self.assertIn("missing_usable_after_strategy", result.errors)


if __name__ == "__main__":
    unittest.main()
