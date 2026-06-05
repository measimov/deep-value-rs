from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from tushare_mirror.mirror import MirrorScopeReporter


class AShareLowRiskScopeTests(unittest.TestCase):
    def run_cli(self, *args):
        return subprocess.run(
            [sys.executable, "-m", "tushare_mirror", *args],
            cwd=Path(__file__).resolve().parents[2],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
        )

    def test_a_share_low_risk_scope_exists_and_is_read_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            before = set(Path(tmp).iterdir())
            report = MirrorScopeReporter().report(scope="a-share-low-risk")
            after = set(Path(tmp).iterdir())
        payload = report.to_dict()
        self.assertEqual(before, after)
        self.assertEqual(payload["report_version"], "mirror-scope/v1")
        self.assertEqual(payload["scope"], "a-share-low-risk")
        self.assertIn("stock_basic", payload["endpoints_in_scope"])
        self.assertIn("stock_company", payload["endpoints_in_scope"])
        self.assertIn("daily", payload["executable_now"])
        self.assertIn("stock_company", payload["disabled"])
        self.assertIn("top10_holders", payload["missing_metadata"])
        self.assertIn("next_enablement_step", payload)

    def test_high_risk_families_are_excluded(self):
        payload = MirrorScopeReporter().report(scope="a-share-low-risk").to_dict()
        scoped = set(payload["endpoints_in_scope"])
        excluded = {
            "stk_mins",
            "moneyflow_hsgt",
            "income",
            "balancesheet",
            "cashflow",
            "fina_indicator",
            "anns",
            "news",
            "realtime_quote",
        }
        self.assertTrue(scoped.isdisjoint(excluded))
        for pattern in ["minute", "tick", "order", "realtime", "income", "anns", "news"]:
            self.assertIn(pattern, payload["excluded_high_risk_patterns"])

    def test_cli_json_contract_is_stable(self):
        result = self.run_cli("mirror-scope", "--scope", "a-share-low-risk", "--json")
        payload = json.loads(result.stdout)
        self.assertEqual(payload["report_version"], "mirror-scope/v1")
        for key in [
            "endpoints_in_scope",
            "executable_now",
            "plan_only",
            "disabled",
            "blocked_reason",
            "missing_metadata",
            "next_enablement_step",
        ]:
            self.assertIn(key, payload)
        self.assertIn("daily", payload["executable_now"])
        self.assertIn("stock_company", payload["disabled"])

    def test_unknown_scope_is_rejected(self):
        result = subprocess.run(
            [sys.executable, "-m", "tushare_mirror", "mirror-scope", "--scope", "all", "--json"],
            cwd=Path(__file__).resolve().parents[2],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("unknown mirror scope", result.stderr)


if __name__ == "__main__":
    unittest.main()
