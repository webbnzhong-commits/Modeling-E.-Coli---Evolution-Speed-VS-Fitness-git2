from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Iterable, TypeVar

T = TypeVar("T")
R = TypeVar("R")


class HubLoadingPool:
    """Small reusable thread pool runner for indexed hub loading jobs."""

    def __init__(self, workers: int = 6) -> None:
        self.workers = max(1, int(workers))

    def run_indexed(
        self,
        items: Iterable[T],
        worker: Callable[[int, T], R],
        stop_event=None,
    ):
        indexed = list(enumerate(items))
        if not indexed:
            return

        executor = ThreadPoolExecutor(max_workers=self.workers, thread_name_prefix="hub_loader")
        futures = {}
        aborted = False
        try:
            for idx, item in indexed:
                if stop_event is not None and stop_event.is_set():
                    aborted = True
                    break
                futures[executor.submit(worker, int(idx), item)] = int(idx)
            if aborted:
                for future in futures:
                    future.cancel()
                return

            for future in as_completed(futures):
                idx = int(futures[future])
                if stop_event is not None and stop_event.is_set():
                    aborted = True
                    for pending in futures:
                        if pending is not future:
                            pending.cancel()
                    break
                yield idx, future.result()
        finally:
            executor.shutdown(wait=(not aborted), cancel_futures=True)
