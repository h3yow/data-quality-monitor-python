"""HTTP 接口层：影响试算与历史回溯端点端到端可用（含拒绝语义）。"""
import logging

from fastapi.testclient import TestClient

import main
from dqm.service import DQService

logger = logging.getLogger("test.api")


def make_client() -> TestClient:
    main.service = DQService()  # 每个用例独立内存状态
    return TestClient(main.app)


def test_trial_endpoint_read_only_and_repeatable() -> None:
    client = make_client()
    client.post("/rules", json={"rule_id": "rg", "rule_type": "range", "field": "age",
                                "params": {"min": 0, "max": 200}})
    client.post("/batches", json={"batch_id": "b1", "rows": [{"age": 150}]})
    client.post("/batches/b1/validate")

    payload = {"rule_id": "rg", "rule_type": "range", "field": "age",
               "params": {"min": 0, "max": 120}}
    r1 = client.post("/trials", json=payload)
    r2 = client.post("/trials", json=payload)
    assert r1.status_code == 200 and r1.json() == r2.json()
    body = r1.json()
    assert body["flipped_batch_ids"] == ["b1"]
    assert body["added_by_severity"] == {"failure": 1, "warning": 0}
    logger.info("试算端点可重复: flipped=%s", body["flipped_batch_ids"])

    # 规则仍为 v1、历史结论未被试算改动
    report = client.get("/batches/b1/report").json()
    assert report["rule_versions"] == {"rg": 1}
    assert report["status"] == "succeeded" and report["revision_no"] == 1


def test_trial_endpoint_rejects_invalid_candidate() -> None:
    client = make_client()
    resp = client.post("/trials", json={
        "rule_id": "bad", "rule_type": "range", "field": "age",
        "params": {"min": 100, "max": 0}})
    assert resp.status_code == 422
    assert resp.json()["detail"]["reason"] == "invalid_definition"
    logger.info("试算端点拒绝非法候选: %s", resp.json()["detail"])


def test_backfill_endpoints_full_flow() -> None:
    client = make_client()
    client.post("/rules", json={"rule_id": "rg", "rule_type": "range", "field": "age",
                                "params": {"min": 0, "max": 200}})
    client.post("/batches", json={"batch_id": "b1", "rows": [{"age": 150}, {"age": 300}]})
    client.post("/batches/b1/validate")
    client.post("/rules", json={"rule_id": "rg", "rule_type": "range", "field": "age",
                                "params": {"min": 0, "max": 120}})

    job = client.post("/backfills", json={"batch_ids": ["b1"], "max_workers": 2})
    assert job.status_code == 201
    job_body = job.json()
    item = job_body["items"]["b1"]
    assert item["status"] == "succeeded" and item["revision_no"] == 2

    cmp_res = client.get("/batches/b1/comparison").json()
    assert cmp_res["previous_revision_no"] == 1
    assert cmp_res["current_revision_no"] == 2
    assert len(cmp_res["added"]) == 2 and len(cmp_res["removed"]) == 1
    logger.info("回溯端点对照: +%d -%d", len(cmp_res["added"]), len(cmp_res["removed"]))

    revisions = client.get("/batches/b1/revisions").json()["revisions"]
    assert [r["revision_no"] for r in revisions] == [1, 2]
    assert [r["source"] for r in revisions] == ["normal", "backfill"]

    latest = client.get("/batches/b1/report").json()
    assert latest["revision_no"] == 2 and latest["status"] == "failed"

    # 作业可查询、重试端点对已成功作业幂等
    got_job = client.get(f"/backfills/{job_body['job_id']}")
    assert got_job.status_code == 200
    retry = client.post(f"/backfills/{job_body['job_id']}/retry")
    assert retry.status_code == 200
    # 单批回溯端点在快照已最新时返回 reran=false 且无新版本
    one = client.post("/batches/b1/backfill").json()
    assert one["reran"] is False
    assert len(client.get("/batches/b1/revisions").json()["revisions"]) == 2


def test_backfill_missing_batch_endpoint() -> None:
    client = make_client()
    resp = client.post("/backfills", json={"batch_ids": ["ghost"]})
    assert resp.status_code == 400
    logger.info("回溯不存在批次被拒绝: %s", resp.json()["detail"])
