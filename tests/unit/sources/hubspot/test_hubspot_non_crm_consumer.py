"""Exercise each snapshot through the batch reader using source and generated connectors."""

import json
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from pyspark.sql.datasource import InputPartition
from pyspark.sql.types import _make_type_verifier

from databricks.labs.community_connector.sources.hubspot import hubspot as source_module
from databricks.labs.community_connector.sparkpds.lakeflow_datasource import LakeflowBatchReader
from tests.unit.sources.hubspot.test_hubspot_custom_objects import _connector_class

TABLES = (
    "owners",
    "pipelines",
    "forms",
    "contact_lists",
    "contact_list_memberships",
    "form_submissions",
)


@pytest.mark.parametrize("generated", [False, True])
@pytest.mark.parametrize("table", TABLES)
def test_snapshot_consumer_nested_rows_and_empty_rescan(generated, table):
    corpus = Path(source_module.__file__).parents[2] / "source_simulator/specs/hubspot/corpus"
    fixtures = {name: json.loads((corpus / f"{name}.json").read_text()) for name in TABLES}
    empty = False

    def response_for(url, **_kwargs):
        if url.endswith("/crm/lists/2026-09/search"):
            rows = [] if empty else fixtures["contact_lists"]
            return Mock(
                status_code=200,
                json=lambda: {
                    "lists": rows,
                    "hasMore": False,
                    "offset": len(rows),
                    "total": len(rows),
                },
            )
        if url.endswith("/contacts/v1/lists"):
            ids = [] if empty else [{"listId": row["listId"]} for row in fixtures["contact_lists"]]
            return Mock(status_code=200, json=lambda: {"lists": ids, "has-more": False})
        if url.endswith("/idmapping"):
            pairs = (
                []
                if empty
                else [
                    {"listId": row["listId"], "legacyListId": row["listId"]}
                    for row in fixtures["contact_lists"]
                ]
            )
            return Mock(
                status_code=200,
                json=lambda: {"legacyListIdsToIdsMapping": pairs, "missingLegacyListIds": []},
            )
        if "/memberships" in url:
            name = "contact_list_memberships"
        elif "/submissions/forms/" in url:
            name = "form_submissions"
        elif "/pipelines/" in url:
            name = "pipelines"
        elif "/owners/" in url:
            name = "owners"
        elif "/lists/" in url:
            name = "contact_lists"
        else:
            assert url.endswith("/marketing/v3/forms"), url
            name = "forms"
        return Mock(status_code=200, json=lambda: {"results": [] if empty else fixtures[name]})

    conn = _connector_class(generated)({"access_token": "offline"})
    conn._non_crm._sleep = lambda _: None
    conn._history._sleep = lambda _: None
    options = {"tableName": table, "pipeline_object_types": "0-3"}
    schema = conn.get_table_schema(table, options)
    metadata = conn.read_table_metadata(table, options)
    if table == "contact_list_memberships":
        assert metadata["ingestion_type"] == "append"
        assert metadata["cursor_field"] == "membershipTimestamp"
    else:
        assert metadata["ingestion_type"] == "snapshot"
        assert metadata["cursor_field"] is None
    # The framework reader converts connector dictionaries to Spark tuples.
    reader = LakeflowBatchReader(options, schema, conn)
    with (
        patch("requests.get", side_effect=response_for),
        patch("requests.post", side_effect=response_for),
    ):
        rows = list(reader.read(InputPartition(None)))
        assert len(rows) == 1
        _make_type_verifier(schema)(rows[0])
        payload = json.loads(rows[0][schema.fieldNames().index("payload")])
        assert all(payload[key] == value for key, value in fixtures[table][0].items())
        for key in metadata["primary_keys"]:
            assert rows[0][schema.fieldNames().index(key)] is not None
        if table == "contact_list_memberships":
            assert len(list(reader.read(InputPartition(None)))) == 1
        else:
            empty = True
            assert list(reader.read(InputPartition(None))) == []


def test_membership_batch_resumes_from_offset():
    lists = {
        "lists": [
            {
                "listId": "57",
                "name": "suppression",
                "objectTypeId": "0-1",
                "processingType": "DYNAMIC",
                "processingStatus": "COMPLETE",
                "createdAt": "2023-11-30T07:33:00.961Z",
                "updatedAt": "2025-10-06T21:58:12.436Z",
            }
        ],
        "hasMore": False,
        "offset": 1,
        "total": 1,
    }
    pages = []

    def response_for(url, **_kwargs):
        if url.endswith("/crm/lists/2026-09/search"):
            return Mock(status_code=200, json=lambda: lists)
        if "/memberships" in url:
            return Mock(status_code=200, json=lambda: pages.pop(0))
        raise AssertionError(url)

    def connector():
        conn = _connector_class(False)({"access_token": "offline"})
        conn._non_crm._sleep = lambda _: None
        return conn

    pages.extend(
        [
            {
                "results": [{"recordId": "10", "membershipTimestamp": "2026-11-03T06:07:40.421Z"}],
                "paging": {"next": {"after": "a"}},
            },
            {"results": [{"recordId": "11", "membershipTimestamp": "2026-11-03T06:07:41.421Z"}]},
        ]
    )
    with (
        patch("requests.get", side_effect=response_for),
        patch("requests.post", side_effect=response_for),
    ):
        conn = connector()
        first, first_offset = conn.read_table("contact_list_memberships", {}, {})
        assert pages == []
        second, second_offset = conn.read_table("contact_list_memberships", first_offset, {})
        assert first_offset == {"membershipDone": True}
        assert second_offset == {"membershipDone": True}
        assert [row["recordId"] for row in first] == ["10", "11"]
        assert list(second) == []

    pages.append(
        {"results": [{"recordId": "12", "membershipTimestamp": "2026-11-03T06:07:42.421Z"}]}
    )
    with (
        patch("requests.get", side_effect=response_for),
        patch("requests.post", side_effect=response_for),
    ):
        walk, offset = connector().read_table("contact_list_memberships", {}, {})
        assert offset == {"membershipDone": True}
        assert [row["recordId"] for row in walk] == ["12"]
