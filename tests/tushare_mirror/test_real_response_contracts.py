from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from tushare_mirror.catalog import CatalogStore
from tushare_mirror.client import QueryResult
from tushare_mirror.endpoints import load_into_catalog
from tushare_mirror.io_utils import read_jsonl_zst
from tushare_mirror.reader import LakeReader
from tushare_mirror.store import FileLakeStore
from tushare_mirror.validation import Validator


FIXTURE_ROOT = Path(__file__).resolve().parents[1] / "fixtures" / "tushare"


PARAMS = {
    "daily": {"trade_date": "20250102"},
    "stock_basic": {"list_status": "L"},
    "trade_cal": {"exchange": "SSE", "start_date": "20250101", "end_date": "20250131"},
    "adj_factor": {"trade_date": "20250102"},
    "daily_basic": {"trade_date": "20250102"},
}


FIXTURES = {
    "daily": "daily_response_minimal.json",
    "stock_basic": "stock_basic_response_minimal.json",
    "trade_cal": "trade_cal_response_minimal.json",
    "adj_factor": "adj_factor_response_minimal.json",
    "daily_basic": "daily_basic_response_minimal.json",
}


class FixtureClient:
    def __init__(self, fixture_name: str):
        payload = json.loads((FIXTURE_ROOT / fixture_name).read_text())
        self.payload = payload
        self.events = payload["events"]

    def query_paginated(self, api_name, params, fields, page_size=None):
        all_items = []
        response_fields = list((self.events[0].get("data") or {}).get("fields") or [])
        events = []
        for idx, event in enumerate(self.events):
            event = dict(event)
            event["_http_status"] = 200
            event["_page_index"] = idx
            event["_request_params"] = dict(params)
            events.append(event)
            all_items.extend((event.get("data") or {}).get("items") or [])
        return QueryResult(events=events, fields=response_fields, items=all_items)


class RealResponseContractTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name) / "lake"
        self.catalog = CatalogStore(self.root)
        self.catalog.init()
        load_into_catalog(self.root, self.catalog)

    def tearDown(self):
        self.tmp.cleanup()

    def test_fixtures_are_minimal_tushare_envelopes_without_token(self):
        for api_name, fixture_name in FIXTURES.items():
            raw = (FIXTURE_ROOT / fixture_name).read_text()
            self.assertNotIn("token", raw.lower(), api_name)
            payload = json.loads(raw)
            self.assertIn("Synthetic fixture", payload["fixture_note"])
            self.assertLessEqual(sum(len((event["data"]["items"])) for event in payload["events"]), 3, api_name)
            for event in payload["events"]:
                self.assertEqual(set(event.keys()), {"code", "msg", "data"})
                self.assertEqual(event["code"], 0)
                self.assertIn("fields", event["data"])
                self.assertIn("items", event["data"])

    def test_store_accepts_observed_envelope_shape_and_pagination(self):
        store = FileLakeStore(self.root, self.catalog)
        for api_name, fixture_name in FIXTURES.items():
            result = store.fetch(api_name, PARAMS[api_name], FixtureClient(fixture_name))
            self.assertIsNotNone(result.snapshot_id, api_name)
            ok, report_id = Validator(self.root, self.catalog).validate_snapshot(result.snapshot_id, api_name)
            self.assertTrue(ok, api_name)
            table = LakeReader(self.root, self.catalog).scan_api(api_name)
            self.assertGreater(table.num_rows, 0, api_name)
            raw_file = next(f for f in self.catalog.files_for_snapshot(result.snapshot_id) if f["content_type"] == "raw")
            raw_events = read_jsonl_zst(self.root / raw_file["relative_path"])
            self.assertEqual(len(raw_events), raw_file["raw_event_count"], api_name)
        daily_job = self.catalog.get_job(FileLakeStore(self.root, self.catalog).plan_fetch("daily", PARAMS["daily"])["job_key"])
        self.assertEqual(daily_job["raw_event_count"], 2)


if __name__ == "__main__":
    unittest.main()
