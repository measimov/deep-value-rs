from __future__ import annotations

import json
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from .errors import classify_tushare_response

TUSHARE_API_URL = "http://api.tushare.pro"


class TushareError(RuntimeError):
    def __init__(self, api_name: str, code: Any, message: str, response: Mapping[str, Any] | None = None):
        super().__init__(f"Tushare API error ({api_name}, {code}): {message}")
        self.api_name = api_name
        self.code = code
        self.message = message
        self.response = response or {}


@dataclass
class QueryResult:
    events: list[dict[str, Any]]
    fields: list[str]
    items: list[list[Any]]


class TushareClient:
    def __init__(self, token: str, api_url: str = TUSHARE_API_URL, timeout: int = 30):
        self.token = token
        self.api_url = api_url
        self.timeout = timeout

    def request(self, api_name: str, params: Mapping[str, Any], fields: Iterable[str] | str | None = None) -> dict[str, Any]:
        if isinstance(fields, str):
            fields_str = fields
        elif fields is None:
            fields_str = ""
        else:
            fields_str = ",".join(fields)
        payload = {
            "api_name": api_name,
            "token": self.token,
            "params": dict(params),
            "fields": fields_str,
        }
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(self.api_url, data=data, headers={"Content-Type": "application/json"}, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                body = resp.read().decode("utf-8")
                result = json.loads(body)
                result["_http_status"] = resp.status
                return result
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")
            try:
                result = json.loads(body)
            except Exception:
                result = {"code": None, "msg": body}
            result["_http_status"] = e.code
            return result

    def query_paginated(self, api_name: str, params: Mapping[str, Any], fields: Iterable[str], page_size: int | None = None, max_pages: int = 200) -> QueryResult:
        events: list[dict[str, Any]] = []
        all_items: list[list[Any]] = []
        response_fields: list[str] = []
        offset = int(params.get("offset", 0) or 0)
        page_index = 0
        while True:
            request_params = dict(params)
            if page_size:
                request_params["limit"] = page_size
                request_params["offset"] = offset
            response = self.request(api_name, request_params, fields)
            event = dict(response)
            event["_page_index"] = page_index
            event["_request_params"] = request_params
            events.append(event)
            if response.get("code") != 0:
                raise TushareError(api_name, response.get("code"), str(response.get("msg", "")), response)
            data = response.get("data") or {}
            fields_page = list(data.get("fields") or [])
            items = list(data.get("items") or [])
            if page_index == 0:
                response_fields = fields_page
            all_items.extend(items)
            has_more = bool(data.get("has_more"))
            if not page_size or not has_more or not items:
                break
            page_index += 1
            if page_index >= max_pages:
                raise TushareError(api_name, "pagination", "max pages exceeded", response)
            offset += page_size
        return QueryResult(events=events, fields=response_fields, items=all_items)


def classify_probe_response(response: Mapping[str, Any]) -> tuple[str, str | None]:
    return classify_tushare_response(response)
