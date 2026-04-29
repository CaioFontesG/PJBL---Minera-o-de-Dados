from __future__ import annotations

import asyncio
import sys
from types import ModuleType

from worker import tasks


def _install_fake_collector(
    monkeypatch, module_name: str, func_name: str, marker: dict
) -> None:
    collectors_pkg = ModuleType("collectors")
    collectors_pkg.__path__ = []

    parent_name = module_name.rsplit(".", 1)[0]
    parent_pkg = ModuleType(parent_name)
    parent_pkg.__path__ = []

    collector_mod = ModuleType(module_name)

    async def _fake_collect() -> None:
        marker["called"] = True

    setattr(collector_mod, func_name, _fake_collect)

    monkeypatch.setitem(sys.modules, "collectors", collectors_pkg)
    monkeypatch.setitem(sys.modules, parent_name, parent_pkg)
    monkeypatch.setitem(sys.modules, module_name, collector_mod)


def _run_coro(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def test_run_ripe_executes_collector(monkeypatch) -> None:
    marker = {"called": False}
    _install_fake_collector(
        monkeypatch,
        module_name="collectors.ripe.collector",
        func_name="collect_ripe",
        marker=marker,
    )
    monkeypatch.setattr(tasks.asyncio, "run", _run_coro)

    tasks.run_ripe()

    assert marker["called"] is True


def test_run_bgptools_executes_collector(monkeypatch) -> None:
    marker = {"called": False}
    _install_fake_collector(
        monkeypatch,
        module_name="collectors.bgptools.collector",
        func_name="collect_bgptools",
        marker=marker,
    )
    monkeypatch.setattr(tasks.asyncio, "run", _run_coro)

    tasks.run_bgptools()

    assert marker["called"] is True
