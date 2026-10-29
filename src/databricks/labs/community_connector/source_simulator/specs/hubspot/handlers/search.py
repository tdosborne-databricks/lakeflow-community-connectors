"""Model HubSpot numeric-ID and modification-time Search predicates offline."""

import re
from datetime import datetime

from databricks.labs.community_connector.source_simulator.specs.hubspot.handlers.batch_read import (
    _build_response,
    _parse_body,
)


def _search_value(record, field):
    if field == "hs_object_id":
        return int(record["id"])
    raw = (record.get("properties") or {}).get(field)
    if raw is None and field in {"lastmodifieddate", "hs_lastmodifieddate"}:
        raw = record["updatedAt"]
    if isinstance(raw, str) and "T" in raw:
        return int(datetime.fromisoformat(raw.replace("Z", "+00:00")).timestamp() * 1000)
    return int(raw)


def serve_search(prep, spec, corpus):  # pylint: disable=unused-argument
    match = re.search(r"/crm/objects/2026-09/([^/]+)/search", prep.url or "")
    body = _parse_body(prep.body)
    records = list(corpus.get(match.group(1)) or [])
    groups = body.get("filterGroups") or [{"filters": []}]

    def matches(record, group):
        for item in group.get("filters", []):
            actual = _search_value(record, item["propertyName"])
            expected = int(item["value"])
            if item["operator"] == "GTE" and actual < expected:
                return False
            if item["operator"] == "GT" and actual <= expected:
                return False
            if item["operator"] == "LTE" and actual > expected:
                return False
        return True

    records = [record for record in records
               if any(matches(record, group) for group in groups)]
    for sort in reversed(body.get("sorts", [])):
        records.sort(key=lambda record: _search_value(record, sort["propertyName"]),
                     reverse=sort.get("direction") == "DESCENDING")
    start, limit = int(body.get("after", 0)), int(body.get("limit", 200))
    if start + limit > 10000:
        return _build_response(prep, status=400,
                               payload={"message": "Search result window exceeded"})
    results = records[start:start + limit]
    paging = {}
    if start + len(results) < len(records):
        paging = {"next": {"after": str(start + len(results))}}
    return _build_response(prep, status=200,
                           payload={"results": results, "paging": paging, "total": len(records)})
