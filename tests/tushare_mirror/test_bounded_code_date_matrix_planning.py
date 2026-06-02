from __future__ import annotations

import json
import unittest

from tushare_mirror.code_date_matrix_planner import (
    CodeDateMatrixItem,
    CodeDateMatrixPlan,
    CodeDateMatrixSummary,
)


class CodeDateMatrixPlanModelTests(unittest.TestCase):
    def test_model_serialization_is_stable(self):
        summary = CodeDateMatrixSummary.from_candidate_counts(
            api_name="stk_managers",
            universe="a_share_listed",
            source_snapshot_id="snap_local",
            total_codes=2,
            total_dates=2,
            limit_codes=2,
            max_dates=2,
        )
        item = CodeDateMatrixItem(
            api_name="stk_managers",
            ts_code="000001.SZ",
            date="20250102",
            params={"ts_code": "000001.SZ", "trade_date": "20250102"},
            job_key="job_abc",
            existing_status="missing",
            planned_action="fetch",
        )
        payload = CodeDateMatrixPlan(summary=summary, items=[item]).to_dict()
        self.assertFalse(payload["blocked"])
        self.assertFalse(payload["execution_allowed"])
        self.assertTrue(payload["dry_run"])
        self.assertEqual(payload["candidate_jobs"], 4)
        self.assertEqual(payload["planned_jobs"], 4)
        self.assertEqual(payload["items"][0]["execution_allowed"], False)
        self.assertEqual(payload["items"][0]["would_require_real_request"], True)
        rendered = json.dumps(payload, sort_keys=True)
        self.assertIn('"api_name": "stk_managers"', rendered)
        self.assertIn('"items"', rendered)

    def test_candidate_limit_calculation_caps_code_and_candidate_counts(self):
        summary = CodeDateMatrixSummary.from_candidate_counts(
            api_name="stk_managers",
            universe="a_share_listed",
            source_snapshot_id="snap_local",
            total_codes=30,
            total_dates=10,
            limit_codes=20,
            max_dates=10,
            max_candidate_jobs=100,
        )
        self.assertEqual(summary.total_codes, 30)
        self.assertEqual(summary.planned_codes, 20)
        self.assertEqual(summary.total_dates, 10)
        self.assertEqual(summary.planned_dates, 5)
        self.assertEqual(summary.candidate_jobs, 300)
        self.assertEqual(summary.planned_jobs, 100)
        self.assertTrue(summary.truncated_by_code_limit)
        self.assertFalse(summary.truncated_by_date_limit)
        self.assertTrue(summary.truncated_by_candidate_limit)

    def test_date_limit_truncation_flag(self):
        summary = CodeDateMatrixSummary.from_candidate_counts(
            api_name="namechange",
            universe="a_share_listed",
            source_snapshot_id=None,
            total_codes=2,
            total_dates=25,
            limit_codes=5,
            max_dates=20,
            max_candidate_jobs=100,
        )
        self.assertEqual(summary.planned_codes, 2)
        self.assertEqual(summary.planned_dates, 20)
        self.assertEqual(summary.candidate_jobs, 50)
        self.assertEqual(summary.planned_jobs, 40)
        self.assertFalse(summary.truncated_by_code_limit)
        self.assertTrue(summary.truncated_by_date_limit)
        self.assertFalse(summary.truncated_by_candidate_limit)

    def test_blocking_errors_mark_plan_blocked(self):
        summary = CodeDateMatrixSummary.from_candidate_counts(
            api_name="stk_rewards",
            universe="a_share_listed",
            source_snapshot_id=None,
            total_codes=0,
            total_dates=0,
            limit_codes=0,
            max_dates=0,
            blocking_errors=["limit_codes_required"],
        )
        payload = CodeDateMatrixPlan(summary=summary, items=[]).to_dict()
        self.assertTrue(payload["blocked"])
        self.assertEqual(payload["blocking_errors"], ["limit_codes_required"])
        self.assertEqual(payload["items"], [])


if __name__ == "__main__":
    unittest.main()
