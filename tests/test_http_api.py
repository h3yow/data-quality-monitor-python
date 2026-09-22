"""HTTP API 端到端：状态码、错误码可区分、幂等提交、复现接口。"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

from dq_monitor.api import create_app
from dq_monitor.service import DataQualityService
from dq_monitor.store import Store

logger = logging.getLogger("dq.tests.http")


@pytest.fixture
def client(service: DataQualityService) -> TestClient:
    return TestClient(create_app(service))


def _bootstrap(client: TestClient) -> None:
    client.post("/schemas", json={
        "name": "orders",
        "fields": [
            {"name": "amount", "dtype": "float"},
            {"name": "status", "dtype": "str"},
        ],
    })
    client.post("/rules", json={
        "rule_id": "amt", "schema_name": "orders", "field_name": "amount",
        "rule_type": "RANGE", "severity": "ERROR",
        "params": {"min": 0, "max": 100},
    })
    client.post("/rules", json={
        "rule_id": "st", "schema_name": "orders", "field_name": "status",
        "rule_type": "ENUM", "severity": "WARN",
        "params": {"values": ["PAID", "DONE"]},
    })


def test_validation_errors_map_to_distinct_http_status(client: TestClient) -> None:
    r = client.post("/schemas", json={"name": "s", "fields": [
        {"name": "a", "dtype": "nope"}]})
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "RULE_PARAM_INVALID"

    _bootstrap(client)

    r = client.post("/rules", json={
        "rule_id": "x", "schema_name": "orders", "field_name": "ghost",
        "rule_type": "RANGE", "params": {"min": 0}})
    assert r.status_code == 422 and r.json()["error"]["code"] == "FIELD_NOT_FOUND"

    r = client.post("/rules", json={
        "rule_id": "x", "schema_name": "orders", "field_name": "amount",
        "rule_type": "RANGE", "params": {"min": 200, "max": 300}})
    assert r.status_code == 409 and r.json()["error"]["code"] == "RULE_CONFLICT"
    assert r.json()["error"]["details"]["reason"] == "disjoint_ranges"

    r = client.post("/batches", json={
        "batch_id": "b", "schema_name": "orders",
        "rows": [{"amount": 1.0}]})  # 缺 status
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "BATCH_FIELD_MISSING"

    assert client.get("/batches/nope/conclusion").status_code == 404
    assert client.post("/batches/nope/submit").status_code == 404


def test_full_flow_and_idempotent_submit(client: TestClient) -> None:
    _bootstrap(client)
    rows = [
        {"amount": 50.0, "status": "PAID"},
        {"amount": 200.0, "status": "WEIRD"},
    ]
    logger.info("HTTP 输入批次 rows=%s", rows)
    r = client.post("/batches", json={
        "batch_id": "b1", "schema_name": "orders", "rows": rows})
    assert r.status_code == 201

    r1 = client.post("/batches/b1/submit")
    r2 = client.post("/batches/b1/submit")
    j1, j2 = r1.json(), r2.json()
    logger.info("重复提交 accepted: 第一次=%s 第二次=%s", j1["accepted"], j2["accepted"])
    assert j1["accepted"] is True and j2["accepted"] is False
    assert j1["execution"]["execution_id"] == j2["execution"]["execution_id"]
    assert j1["execution"]["status"] == "SUCCEEDED"

    conc = client.get("/batches/b1/conclusion").json()
    assert conc["conclusion"] == "FAIL"
    assert conc["error_count"] == 1 and conc["warn_count"] == 1

    findings = client.get("/batches/b1/findings").json()
    assert findings["count"] == 2
    warns = client.get("/batches/b1/findings?severity=WARN").json()
    assert {f["severity"] for f in warns["findings"]} == {"WARN"}

    stats = client.get("/stats").json()
    assert stats["executions"] == 1 and stats["errors"] == 1 and stats["warnings"] == 1

    # 规则变更 + 历史回放
    r = client.post("/rules", json={
        "rule_id": "amt", "schema_name": "orders", "field_name": "amount",
        "rule_type": "RANGE", "severity": "ERROR",
        "params": {"min": 0, "max": 1000}})
    assert r.status_code == 201 and r.json()["version"] == 2

    replay = client.post("/batches/b1/replay").json()
    logger.info("HTTP 回放 basis=%s 结论=%s matches=%s",
                replay["basis"], replay["conclusion"], replay["matches_historical"])
    assert replay["matches_historical"] is True
    assert replay["error_count"] == 1  # 仍按 v1
    rule_versions = client.get("/rules/amt").json()
    assert [v["version"] for v in rule_versions["versions"]] == [1, 2]
    assert rule_versions["versions"][0]["status"] == "SUPERSEDED"


def test_http_concurrent_submit(client: TestClient) -> None:
    _bootstrap(client)
    client.post("/batches", json={
        "batch_id": "b1", "schema_name": "orders",
        "rows": [{"amount": 999.0, "status": "PAID", "__delay__": 0.2}],
    })

    def submit() -> dict:
        return client.post("/batches/b1/submit").json()

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: submit(), range(8)))
    accepted = [r["accepted"] for r in results]
    ids = {r["execution"]["execution_id"] for r in results}
    logger.info("HTTP 并发提交 accepted=%s 唯一执行数=%d", accepted, len(ids))
    assert accepted.count(True) == 1
    assert len(ids) == 1
    assert client.get("/stats").json()["executions"] == 1


def test_failed_execution_retry_endpoint(client: TestClient) -> None:
    _bootstrap(client)
    client.post("/batches", json={
        "batch_id": "b1", "schema_name": "orders",
        "rows": [{"amount": 1.0, "status": "PAID", "__crash__": "kaboom"}],
    })
    r = client.post("/batches/b1/submit").json()
    assert r["execution"]["status"] == "FAILED"
    assert r["execution"]["fail_reason"] == "kaboom"
    assert r["execution"]["findings"] == []

    # 未修复直接重试：仍失败
    again = client.post("/batches/b1/retry").json()
    assert again["execution"]["status"] == "FAILED"
    assert again["execution"]["attempt"] == 2
