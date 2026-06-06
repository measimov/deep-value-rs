from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from tushare_mirror.mirror import (
    MirrorScopeReporter,
    coverage_matrix_apis_for_scope,
    daily_like_apis_for_scope,
    mirror_scope_endpoints,
    reference_refresh_apis_for_scope,
)


class HKUSLowRiskScopeTests(unittest.TestCase):
    def run_cli(self, *args):
        return subprocess.run(
            [sys.executable, "-m", "tushare_mirror", *args],
            cwd=Path(__file__).resolve().parents[2],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
        )

    def test_hk_scope_exists_and_is_read_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            before = set(Path(tmp).iterdir())
            payload = MirrorScopeReporter().report(scope="hk-low-risk").to_dict()
            after = set(Path(tmp).iterdir())
        self.assertEqual(before, after)
        self.assertEqual(payload["report_version"], "mirror-scope/v1")
        self.assertEqual(payload["scope"], "hk-low-risk")
        self.assertIn("hk_basic", payload["endpoints_in_scope"])
        self.assertIn("hk_daily_adj", payload["plan_only"])
        self.assertIn("hk_mins", payload["disabled"])
        self.assertIn("rt_hk_k", payload["disabled"])
        self.assertEqual(payload["real_probe_status"]["hk_daily_adj"], "passed")
        self.assertEqual(payload["pagination_strategy"]["hk_daily_adj"], "offset_limit")
        self.assertIn("hk_daily_adj", payload["missing_metadata"])
        self.assertEqual(payload["executable_now"], [])

    def test_us_scope_exists_and_is_read_only(self):
        payload = MirrorScopeReporter().report(scope="us-low-risk").to_dict()
        self.assertEqual(payload["scope"], "us-low-risk")
        self.assertIn("us_basic", payload["endpoints_in_scope"])
        self.assertIn("us_daily_adj", payload["plan_only"])
        self.assertIn("us_income", payload["plan_only"])
        self.assertNotIn("us_income", payload["executable_now"])
        self.assertEqual(payload["real_probe_status"]["us_daily"], "passed")
        self.assertEqual(payload["pagination_strategy"]["us_daily"], "offset_limit")
        self.assertIn("us_daily", payload["missing_metadata"])

    def test_global_scope_is_explicit_composition(self):
        payload = MirrorScopeReporter().report(scope="global-equity-low-risk").to_dict()
        self.assertEqual(payload["scope"], "global-equity-low-risk")
        self.assertEqual(sorted(payload["child_scopes"]), ["a-share-low-risk", "hk-low-risk", "us-low-risk"])
        self.assertIn("daily", payload["child_scopes"]["a-share-low-risk"]["endpoints_in_scope"])
        self.assertIn("hk_daily", payload["child_scopes"]["hk-low-risk"]["endpoints_in_scope"])
        self.assertIn("us_daily", payload["child_scopes"]["us-low-risk"]["endpoints_in_scope"])
        self.assertIn("daily", payload["executable_now"])
        self.assertNotIn("hk_mins", payload["executable_now"])
        self.assertNotIn("us_income", payload["executable_now"])
        self.assertIn("hk_daily", payload["plan_only"])
        self.assertIn("us_daily", payload["plan_only"])

    def test_cli_json_contract_is_stable_for_new_scopes(self):
        for scope in ["hk-low-risk", "us-low-risk", "global-equity-low-risk"]:
            with self.subTest(scope=scope):
                result = self.run_cli("mirror-scope", "--scope", scope, "--json")
                payload = json.loads(result.stdout)
                for key in [
                    "report_version",
                    "scope",
                    "endpoints_in_scope",
                    "executable_now",
                    "plan_only",
                    "disabled",
                    "blocked_reason",
                    "missing_metadata",
                    "real_probe_status",
                    "pagination_strategy",
                    "next_enablement_step",
                ]:
                    self.assertIn(key, payload)
                self.assertEqual(payload["report_version"], "mirror-scope/v1")

    def test_scope_helpers_accept_new_scopes_without_wildcards(self):
        self.assertEqual(daily_like_apis_for_scope("hk-low-risk"), ["hk_daily", "hk_daily_adj", "hk_adjfactor"])
        self.assertEqual(daily_like_apis_for_scope("us-low-risk"), ["us_daily", "us_daily_adj", "us_adjfactor"])
        self.assertEqual(reference_refresh_apis_for_scope("hk-low-risk"), ["hk_basic", "hk_tradecal"])
        self.assertEqual(reference_refresh_apis_for_scope("us-low-risk"), ["us_basic", "us_tradecal"])
        self.assertEqual(coverage_matrix_apis_for_scope("hk-low-risk"), ["hk_daily", "hk_daily_adj", "hk_adjfactor"])
        self.assertIn("daily", mirror_scope_endpoints("global-equity-low-risk"))
        self.assertIn("hk_daily", mirror_scope_endpoints("global-equity-low-risk"))
        self.assertIn("us_daily", mirror_scope_endpoints("global-equity-low-risk"))
        self.assertNotIn("hk_mins", daily_like_apis_for_scope("global-equity-low-risk"))

    def test_a_share_scope_contract_remains_unchanged(self):
        payload = MirrorScopeReporter().report(scope="a-share-low-risk").to_dict()
        self.assertIn("daily", payload["executable_now"])
        self.assertIn("stock_company", payload["executable_now"])
        self.assertIn("top10_holders", payload["disabled"])
        self.assertEqual(payload["missing_metadata"], [])

