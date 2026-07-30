import asyncio
from unittest.mock import patch

import pytest
from kimina_client import ReplResponse

from server.errors import NoAvailableReplError
from server.manager import Manager
from server.repl import Repl


@pytest.mark.asyncio
async def test_lazy_lock_initialization() -> None:
    """Test that Lock and Condition are initialized lazily in async context."""
    manager = Manager(max_repls=1, max_repl_uses=1)
    
    # Initially, lock and condition should be None
    assert manager._lock is None
    assert manager._cond is None
    
    # After calling an async method, they should be initialized
    repl = await manager.get_repl()
    
    assert manager._lock is not None
    assert manager._cond is not None
    assert repl is not None
    
    await manager.release_repl(repl)


@pytest.mark.asyncio
async def test_get_repl() -> None:
    manager = Manager(max_repls=1, max_repl_uses=1)

    repl = await manager.get_repl()

    assert repl is not None

    await manager.release_repl(repl)


@pytest.mark.asyncio
async def test_exhausted() -> None:
    manager = Manager(max_repls=0, max_repl_uses=1)

    with pytest.raises(NoAvailableReplError):
        await manager.get_repl(timeout=3)


@pytest.mark.asyncio
async def test_get_repl_with_reuse() -> None:
    manager = Manager(max_repls=1, max_repl_uses=3)

    repl1 = await manager.get_repl()
    assert repl1 is not None

    await manager.release_repl(repl1)

    repl2 = await manager.get_repl()
    assert repl2.uuid == repl1.uuid

    await manager.release_repl(repl2)

    repl3 = await manager.get_repl(reuse=False)

    assert repl3.uuid != repl1.uuid

    assert manager._busy == {repl3}
    assert manager._free == []


@pytest.mark.asyncio
async def test_concurrent_get_repl_respects_capacity() -> None:
    manager = Manager(max_repls=1, max_repl_uses=3)
    original_create = Repl.create

    async def delayed_create(
        header: str, max_repl_uses: int, max_repl_mem: int
    ) -> Repl:
        await asyncio.sleep(0.05)
        return await original_create(header, max_repl_uses, max_repl_mem)

    async def acquire_and_release() -> Repl:
        repl = await manager.get_repl(timeout=1)
        await asyncio.sleep(0.01)
        await manager.release_repl(repl)
        return repl

    with patch.object(Repl, "create", side_effect=delayed_create) as create:
        first, second = await asyncio.gather(
            acquire_and_release(), acquire_and_release()
        )

    assert create.await_count == 1
    assert first is second


@pytest.mark.asyncio
async def test_concurrent_repl_creation_is_not_serialized() -> None:
    manager = Manager(max_repls=2, max_repl_uses=3)
    original_create = Repl.create
    active_creations = 0
    max_active_creations = 0

    async def delayed_create(
        header: str, max_repl_uses: int, max_repl_mem: int
    ) -> Repl:
        nonlocal active_creations, max_active_creations
        active_creations += 1
        max_active_creations = max(max_active_creations, active_creations)
        await asyncio.sleep(0.05)
        active_creations -= 1
        return await original_create(header, max_repl_uses, max_repl_mem)

    with patch.object(Repl, "create", side_effect=delayed_create):
        first, second = await asyncio.gather(
            manager.get_repl(timeout=1), manager.get_repl(timeout=1)
        )

    assert max_active_creations == 2
    await manager.release_repl(first)
    await manager.release_repl(second)


@pytest.mark.asyncio
async def test_same_header_uses_bounded_pool() -> None:
    manager = Manager(max_repls=8, max_repl_starts=2, max_repl_uses=3)
    original_create = Repl.create

    async def delayed_create(
        header: str, max_repl_uses: int, max_repl_mem: int
    ) -> Repl:
        await asyncio.sleep(0.05)
        return await original_create(header, max_repl_uses, max_repl_mem)

    async def acquire_and_release() -> Repl:
        repl = await manager.get_repl(header="import Mathlib", timeout=1)
        await asyncio.sleep(0.01)
        await manager.release_repl(repl)
        return repl

    with patch.object(Repl, "create", side_effect=delayed_create) as create:
        repls = await asyncio.gather(*(acquire_and_release() for _ in range(8)))

    assert create.await_count == 2
    assert len({repl.uuid for repl in repls}) == 2


@pytest.mark.asyncio
async def test_cold_repl_starts_are_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    manager = Manager(max_repls=4, max_repl_starts=2, max_repl_uses=3)
    repls = [
        await manager.get_repl(header=f"import Header{i}", timeout=1)
        for i in range(4)
    ]
    active_starts = 0
    max_active_starts = 0

    async def delayed_start(self: Repl) -> None:
        nonlocal active_starts, max_active_starts
        active_starts += 1
        max_active_starts = max(max_active_starts, active_starts)
        await asyncio.sleep(0.05)
        active_starts -= 1

    async def send_header(
        self: Repl, *args: object, **kwargs: object
    ) -> ReplResponse:
        return ReplResponse(id="header", response={})

    monkeypatch.setattr(Repl, "start", delayed_start)
    monkeypatch.setattr(Repl, "send_timeout", send_header)

    await asyncio.gather(
        *(manager.prep(repl, "test", timeout=1, debug=False) for repl in repls)
    )

    assert max_active_starts == 2


@pytest.mark.asyncio
async def test_configured_pool_prewarms_to_requested_size(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    header = "import Mathlib"
    manager = Manager(
        max_repls=3,
        max_repl_starts=2,
        max_repl_uses=3,
        init_repls={header: 3},
    )
    active_starts = 0
    max_active_starts = 0

    async def delayed_start(self: Repl) -> None:
        nonlocal active_starts, max_active_starts
        active_starts += 1
        max_active_starts = max(max_active_starts, active_starts)
        await asyncio.sleep(0.05)
        active_starts -= 1

    async def send_header(
        self: Repl, *args: object, **kwargs: object
    ) -> ReplResponse:
        return ReplResponse(id="header", response={})

    monkeypatch.setattr(Repl, "start", delayed_start)
    monkeypatch.setattr(Repl, "send_timeout", send_header)

    await manager.initialize_repls()

    assert len(manager._free) == 3
    assert {repl.header for repl in manager._free} == {header}
    assert max_active_starts == 2
