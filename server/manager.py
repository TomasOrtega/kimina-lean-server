from __future__ import annotations

import asyncio
import json
from datetime import datetime
from time import time

from kimina_client import ReplResponse, Snippet
from loguru import logger

from .errors import NoAvailableReplError, ReplError
from .repl import Repl, close_verbose
from .settings import settings
from .utils import is_blank


class Manager:
    def __init__(
        self,
        *,
        max_repls: int = settings.max_repls,
        max_repl_starts: int = settings.max_repl_starts,
        max_repl_uses: int = settings.max_repl_uses,
        max_repl_mem: int = settings.max_repl_mem,
        init_repls: dict[str, int] = settings.init_repls,
    ) -> None:
        if max_repl_starts < 1:
            raise ValueError("max_repl_starts must be at least 1")
        self.max_repls = max_repls
        self.max_repl_starts = max_repl_starts
        self.max_repl_uses = max_repl_uses
        self.max_repl_mem = max_repl_mem
        self.init_repls = init_repls

        self._lock: asyncio.Lock | None = None
        self._cond: asyncio.Condition | None = None
        self._start_semaphore: asyncio.Semaphore | None = None
        self._free: list[Repl] = []
        self._busy: set[Repl] = set()
        self._starting = 0
        self._starting_by_header: dict[str, int] = {}

        logger.info(
            "REPL manager initialized with: MAX_REPLS={}, MAX_REPL_STARTS={}, MAX_REPL_USES={}, MAX_REPL_MEM={} MB",
            max_repls,
            max_repl_starts,
            max_repl_uses,
            max_repl_mem,
        )

    def _ensure_lock(self) -> None:
        """Ensure the lock and condition are initialized in an async context."""
        if self._lock is None:
            self._lock = asyncio.Lock()
            self._cond = asyncio.Condition(self._lock)
            self._start_semaphore = asyncio.Semaphore(self.max_repl_starts)

    async def initialize_repls(self) -> None:
        if len(self.init_repls) == 0:
            return
        if self.max_repls < sum(self.init_repls.values()):
            raise ValueError(
                f"Cannot initialize REPLs: Σ (INIT_REPLS values) = {sum(self.init_repls.values())} > {self.max_repls} = MAX_REPLS"
            )
        async def _initialize(header: str) -> None:
            repl = await self.get_repl(header=header)
            # All initialized imports should finish in 60 seconds.
            await self.prep(repl, snippet_id="init", timeout=60, debug=False)
            await self.release_repl(repl)

        await asyncio.gather(
            *(
                _initialize(header)
                for header, count in self.init_repls.items()
                for _ in range(count)
            )
        )

        logger.info(f"Initialized REPLs with: {json.dumps(self.init_repls, indent=2)}")

    async def get_repl(
        self,
        header: str = "",
        snippet_id: str = "",
        timeout: float = settings.max_wait,
        reuse: bool = True,
    ) -> Repl:
        """
        Async-safe way to get a `Repl` instance for a given header.
        Immediately raises an Exception if not possible.
        """
        self._ensure_lock()
        assert self._cond is not None  # Type narrowing after _ensure_lock
        deadline = time() + timeout
        repl_to_destroy: Repl | None = None
        while True:
            async with self._cond:
                logger.info(
                    f"# Free = {len(self._free)} | # Busy = {len(self._busy)} | # Max = {self.max_repls}"
                )
                if reuse:
                    for i, r in enumerate(self._free):
                        if (
                            r.header == header
                        ):  # repl shouldn't be exhausted (max uses to check)
                            repl = self._free.pop(i)
                            self._busy.add(repl)

                            logger.info(
                                f"\\[{repl.uuid.hex[:8]}] Reusing ({'started' if repl.is_running else 'non-started'}) REPL for {snippet_id}"
                            )
                            return repl
                total = len(self._free) + len(self._busy) + self._starting
                header_total = (
                    sum(repl.header == header for repl in self._free)
                    + sum(repl.header == header for repl in self._busy)
                    + self._starting_by_header.get(header, 0)
                )
                header_limit = max(
                    self.max_repl_starts, self.init_repls.get(header, 0)
                )
                can_create = not reuse or header_total < header_limit
                if total < self.max_repls and can_create:
                    self._starting += 1
                    self._starting_by_header[header] = (
                        self._starting_by_header.get(header, 0) + 1
                    )
                    break

                if self._free and can_create:
                    oldest = min(
                        self._free, key=lambda r: r.last_check_at
                    )  # Use the one that's been around the longest
                    self._free.remove(oldest)
                    repl_to_destroy = oldest
                    self._starting += 1
                    self._starting_by_header[header] = (
                        self._starting_by_header.get(header, 0) + 1
                    )
                    break

                remaining = deadline - time()
                if remaining <= 0:
                    raise NoAvailableReplError(f"Timed out after {timeout}s")

                try:
                    logger.info(
                        f"Waiting for a REPL to become available (timeout in {remaining:.2f}s)"
                    )
                    # Wait for a REPL to be released
                    await asyncio.wait_for(self._cond.wait(), timeout=remaining)
                except asyncio.TimeoutError:
                    raise NoAvailableReplError(
                        f"Timed out after {timeout}s while waiting for a REPL"
                    ) from None

        if repl_to_destroy is not None:
            asyncio.create_task(close_verbose(repl_to_destroy))

        try:
            repl = await self.start_new(header)
        except BaseException:
            async with self._cond:
                self._starting -= 1
                self._finish_start(header)
                self._cond.notify(1)
            raise

        async with self._cond:
            self._starting -= 1
            self._finish_start(header)
            self._busy.add(repl)
            return repl

    def _finish_start(self, header: str) -> None:
        remaining = self._starting_by_header[header] - 1
        if remaining:
            self._starting_by_header[header] = remaining
        else:
            del self._starting_by_header[header]

    async def destroy_repl(self, repl: Repl) -> None:
        self._ensure_lock()
        assert self._cond is not None  # Type narrowing after _ensure_lock
        async with self._cond:
            self._busy.discard(repl)
            if repl in self._free:
                self._free.remove(repl)
            asyncio.create_task(close_verbose(repl))
            self._cond.notify(1)

    async def release_repl(self, repl: Repl) -> None:
        self._ensure_lock()
        assert self._cond is not None  # Type narrowing after _ensure_lock
        async with self._cond:
            if repl not in self._busy:
                logger.error(
                    f"Attempted to release a REPL that is not busy: {repl.uuid.hex[:8]}"
                )
                return

            if repl.exhausted:
                uuid = repl.uuid
                logger.info(f"REPL {uuid.hex[:8]} is exhausted, closing it")
                self._busy.discard(repl)

                asyncio.create_task(close_verbose(repl))
                self._cond.notify(1)
                return
            self._busy.remove(repl)
            self._free.append(repl)
            repl.last_check_at = datetime.now()
            logger.info(f"\\[{repl.uuid.hex[:8]}] Released!")
            self._cond.notify(1)

    async def start_new(self, header: str) -> Repl:
        return await Repl.create(
            header, max_repl_uses=self.max_repl_uses, max_repl_mem=self.max_repl_mem
        )

    async def cleanup(self) -> None:
        self._ensure_lock()
        assert self._cond is not None  # Type narrowing after _ensure_lock
        async with self._cond:
            logger.info("Cleaning up REPL manager...")
            for repl in self._free:
                asyncio.create_task(close_verbose(repl))
            self._free.clear()

            for repl in self._busy:
                asyncio.create_task(close_verbose(repl))
            self._busy.clear()

            logger.info("REPL manager cleaned up!")
        pass

    async def prep(
        self, repl: Repl, snippet_id: str, timeout: float, debug: bool
    ) -> ReplResponse | None:
        if repl.is_running:
            return None
        self._ensure_lock()
        assert self._start_semaphore is not None
        async with self._start_semaphore:
            if repl.is_running:
                return None
            return await self._prep(repl, snippet_id, timeout, debug)

    async def _prep(
        self, repl: Repl, snippet_id: str, timeout: float, debug: bool
    ) -> ReplResponse | None:
        try:
            await repl.start()
        except Exception as e:
            logger.exception("Failed to start REPL: %s", e)
            raise ReplError("Failed to start REPL") from e

        if not is_blank(repl.header):
            try:
                cmd_response = await repl.send_timeout(
                    Snippet(id=f"{snippet_id}-header", code=repl.header),
                    timeout=timeout,
                    is_header=True,
                )
            except TimeoutError as e:
                logger.error("Header command timed out")
                raise e
            except Exception as e:
                logger.error("Failed to run header on REPL")
                raise ReplError("Failed to run header on REPL") from e

            if not debug:
                cmd_response.diagnostics = None

            if cmd_response.error:
                logger.error(f"Header command failed: {cmd_response.error}")
                await self.destroy_repl(repl)

            repl.header_cmd_response = cmd_response

            return cmd_response
        return repl.header_cmd_response
