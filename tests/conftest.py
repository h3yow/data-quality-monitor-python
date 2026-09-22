"""pytest 公共夹具。"""

from __future__ import annotations

import logging

import pytest

from dq_monitor.service import DataQualityService
from dq_monitor.store import Store


def _configure_test_logging() -> None:
    """让单测日志打印输入数据与判定依据。"""
    logger = logging.getLogger("dq")
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s %(name)s | %(message)s")
        )
        logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)


_configure_test_logging()


@pytest.fixture
def store() -> Store:
    return Store()


@pytest.fixture
def service(store: Store) -> DataQualityService:
    return DataQualityService(store)
