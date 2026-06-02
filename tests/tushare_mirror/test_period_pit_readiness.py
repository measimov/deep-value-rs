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


if __name__ == "__main__":
    unittest.main()
