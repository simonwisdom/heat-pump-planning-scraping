"""Tests for the shared build/parse helpers behind the live + batch paths.

The batch runner reuses `build_request_body` and `parse_result` from
extract_schema_v1 so the batched prompt is byte-identical to the validated
v4.18 live prompt. These tests pin that contract.
"""

from __future__ import annotations

import scripts.llm.batch_extract as bx
import scripts.llm.extract_schema_v1 as ex

SAMPLE_ROW = {
    "uid": "test-uid-1",
    "authority_name": "Testshire",
    "reference": "T/2026/0001",
    "description": "Installation of an air source heat pump",
    "planning_decision": "Refused",
    "decision_date": "2026-01-01",
}


def test_build_request_body_default_reasoning(monkeypatch):
    monkeypatch.delenv("HP_REASONING_EFFORT", raising=False)
    body, used = ex.build_request_body(SAMPLE_ROW, {})

    assert body["model"] == ex.MODEL
    # system message is the verbatim v4.18 prompt
    assert body["messages"][0] == {"role": "system", "content": ex.SYSTEM_PROMPT}
    # user message carries the per-app header fields
    user = body["messages"][1]["content"]
    assert body["messages"][1]["role"] == "user"
    assert "AUTHORITY: Testshire" in user
    assert "REFERENCE: T/2026/0001" in user
    assert "air source heat pump" in user
    # strict structured output against the canonical schema
    js = body["response_format"]["json_schema"]
    assert body["response_format"]["type"] == "json_schema"
    assert js["strict"] is True
    assert js["schema"] is ex.SCHEMA
    # reasoning model: reasoning_effort=low, never a temperature
    assert body["reasoning_effort"] == "low"
    assert "temperature" not in body
    # no files selected -> nothing used
    assert used == []


def test_build_request_body_temperature_fallback(monkeypatch):
    # HP_REASONING_EFFORT="" selects the non-reasoning temperature=0 path.
    monkeypatch.setenv("HP_REASONING_EFFORT", "")
    body, _ = ex.build_request_body(SAMPLE_ROW, {})
    assert body["temperature"] == 0
    assert "reasoning_effort" not in body


def test_parse_result_attaches_files_and_normalizes():
    used = [{"text_path": "texts/a.txt"}]
    # a refused app: normalize_conditions must blank the condition lists
    out = ex.parse_result('{"decision_outcome": "refused"}', used)
    assert out["_files_used"] == used
    assert out["condition_types"] == []
    assert out["hp_specific_conditions"] == []
    # parse_result must NOT add _usage (caller supplies it)
    assert "_usage" not in out


# --------------------------------------------------------------------------- #
# Batch runner pure helpers
# --------------------------------------------------------------------------- #
def test_jsonl_request_shape():
    body = {"model": "m", "messages": []}
    req = bx.jsonl_request("uid-9", body)
    assert req == {
        "custom_id": "uid-9",
        "method": "POST",
        "url": "/v1/chat/completions",
        "body": body,
    }


def test_pack_chunks_groups_under_cap_in_order():
    # caps at 10: [4,4] = 8 ok; +5 would be 13 -> new chunk
    groups = bx.pack_chunks([4, 4, 5, 5, 2], max_bytes=10)
    assert groups == [[0, 1], [2, 3], [4]]
    # every index appears exactly once, order preserved
    assert [i for g in groups for i in g] == [0, 1, 2, 3, 4]


def test_pack_chunks_oversize_request_gets_own_chunk():
    # a single 20-byte request exceeds the 10-byte cap -> isolated, not dropped
    groups = bx.pack_chunks([3, 20, 3], max_bytes=10)
    assert groups == [[0], [1], [2]]


def test_pending_uids_skips_collected_in_order():
    assert bx.pending_uids(["a", "b", "c", "d"], {"b", "d"}) == ["a", "c"]
