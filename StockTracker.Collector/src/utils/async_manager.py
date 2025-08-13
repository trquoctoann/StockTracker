
import asyncio
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Awaitable, Callable, Dict, Generic, Optional, TypeVar

import structlog

from ..core.config import config
from .rate_limiter import rate_limiter

logger = structlog.get_logger()

T = TypeVar("T")


@dataclass
class TaskResult(Generic[T]):
    task_id: str
    success: bool
    result: Optional[T] = None
    error: Optional[str] = None
    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def duration(self) -> Optional[float]:
        if self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None


@dataclass
class ConcurrencyLimits:
    max_concurrent_tasks: int = field(default_factory=lambda: config.concurrency.max_workers)
    max_async_connections: int = field(default_factory=lambda: config.concurrency.max_async_connections)
    max_threads: int = field(default_factory=lambda: config.concurrency.max_workers)

    def __post_init__(self):
        if self.max_concurrent_tasks <= 0:
            raise ValueError("max_concurrent_tasks must be positive")
        if self.max_async_connections <= 0:
            raise ValueError("max_async_connections must be positive")
        if self.max_threads <= 0:
            raise ValueError("max_threads must be positive")


class AsyncTaskManager:
    def __init__(self, limits: Optional[ConcurrencyLimits] = None):
        self.limits = limits or ConcurrencyLimits()
        self._semaphore = asyncio.Semaphore(self.limits.max_concurrent_tasks)
        self._active_tasks: Dict[str, asyncio.Task] = {}
        self._results: Dict[str, TaskResult] = {}
        self._lock = asyncio.Lock()
        self._task_counter = 0

        logger.info(
            "AsyncTaskManager initialized",
            max_concurrent=self.limits.max_concurrent_tasks,
            max_connections=self.limits.max_async_connections,
        )

    async def submit_task(
        self, coro: Awaitable[T], task_id: Optional[str] = None, rate_limit_key: Optional[str] = None
    ) -> str:
        """
        Submit an async task for execution.

        Args:
            coro: Coroutine to execute
            task_id: Optional task ID (auto-generated if not provided)
            rate_limit_key: Optional rate limit key

        Returns:
            Task ID for tracking
        """
        async with self._lock:
            if task_id is None:
                self._task_counter += 1
                task_id = f"task_{self._task_counter}"

            if task_id in self._active_tasks:
                raise ValueError(f"Task {task_id} already exists")
    
        wrapped_coro = self._wrap_task(coro, task_id, rate_limit_key)

        task = asyncio.create_task(wrapped_coro)

        async with self._lock:
            self._active_tasks[task_id] = task
            self._results[task_id] = TaskResult(task_id=task_id, success=False)

        return task_id

    async def _wrap_task(self, coro: Awaitable[T], task_id: str, rate_limit_key: Optional[str]) -> TaskResult[T]:
        """Wrap task with semaphore, rate limiting, and error handling."""
        result = self._results[task_id]

        try:
            async with self._semaphore:
                if rate_limit_key:
                    await rate_limiter.wait_if_needed("default", rate_limit_key)

                start_time = datetime.now()
                task_result = await coro
                end_time = datetime.now()

                result.success = True
                result.result = task_result
                result.end_time = end_time

                logger.debug("Task completed successfully", task_id=task_id, duration=result.duration)

        except Exception as e:
            result.success = False
            result.error = str(e)
            result.end_time = datetime.now()

            logger.error("Task failed", task_id=task_id, error=str(e), duration=result.duration)

        finally:
            async with self._lock:
                self._active_tasks.pop(task_id, None)

        return result

    async def wait_for_task(self, task_id: str, timeout: Optional[float] = None) -> TaskResult[T]:
        """
        Wait for a specific task to complete.

        Args:
            task_id: ID of task to wait for
            timeout: Optional timeout in seconds

        Returns:
            TaskResult with execution details
        """
        if task_id not in self._results:
            raise ValueError(f"Task {task_id} not found")

        task = self._active_tasks.get(task_id)
        if task:
            try:
                await asyncio.wait_for(task, timeout=timeout)
            except asyncio.TimeoutError:
                task.cancel()
                result = self._results[task_id]
                result.error = f"Task timed out after {timeout} seconds"
                result.end_time = datetime.now()

        return self._results[task_id]

    async def wait_for_all(self, timeout: Optional[float] = None) -> Dict[str, TaskResult]:
        """
        Wait for all active tasks to complete.

        Args:
            timeout: Optional timeout in seconds

        Returns:
            Dictionary of task results
        """
        async with self._lock:
            active_task_ids = list(self._active_tasks.keys())

        if not active_task_ids:
            return self._results.copy()

        try:
            await asyncio.wait_for(
                asyncio.gather(*[self._active_tasks[task_id] for task_id in active_task_ids], return_exceptions=True),
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            for task_id in active_task_ids:
                task = self._active_tasks.get(task_id)
                if task and not task.done():
                    task.cancel()
                    result = self._results[task_id]
                    result.error = f"Task timed out after {timeout} seconds"
                    result.end_time = datetime.now()

        return self._results.copy()

    async def cancel_task(self, task_id: str) -> bool:
        """
        Cancel a specific task.

        Args:
            task_id: ID of task to cancel

        Returns:
            True if task was cancelled, False if not found or already completed
        """
        task = self._active_tasks.get(task_id)
        if task and not task.done():
            task.cancel()
            result = self._results[task_id]
            result.error = "Task was cancelled"
            result.end_time = datetime.now()
            return True
        return False

    async def cancel_all(self):
        """Cancel all active tasks."""
        async with self._lock:
            active_tasks = list(self._active_tasks.values())

        for task in active_tasks:
            if not task.done():
                task.cancel()

        for task_id, result in self._results.items():
            if not result.end_time:
                result.error = "Task was cancelled"
                result.end_time = datetime.now()

    def get_active_task_count(self) -> int:
        """Get number of currently active tasks."""
        return len(self._active_tasks)

    def get_completed_task_count(self) -> int:
        """Get number of completed tasks."""
        return len([r for r in self._results.values() if r.end_time is not None])

    def get_task_result(self, task_id: str) -> Optional[TaskResult]:
        """Get result for a specific task."""
        return self._results.get(task_id)

    def clear_completed_tasks(self):
        """Clear results for completed tasks."""
        completed_task_ids = [
            task_id
            for task_id, result in self._results.items()
            if result.end_time is not None and task_id not in self._active_tasks
        ]

        for task_id in completed_task_ids:
            self._results.pop(task_id, None)


class ThreadPoolManager:
    """Manager for CPU-bound tasks using thread pools."""

    def __init__(self, limits: Optional[ConcurrencyLimits] = None):
        """Initialize thread pool manager."""
        self.limits = limits or ConcurrencyLimits()
        self._executor: Optional[ThreadPoolExecutor] = None
        self._futures: Dict[str, Future] = {}
        self._results: Dict[str, TaskResult] = {}
        self._lock = threading.Lock()
        self._task_counter = 0

        logger.info("ThreadPoolManager initialized", max_threads=self.limits.max_threads)

    def __enter__(self):
        """Context manager entry."""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.shutdown()

    def start(self):
        """Start the thread pool."""
        if self._executor is None:
            self._executor = ThreadPoolExecutor(max_workers=self.limits.max_threads)

    def shutdown(self, wait: bool = True):
        """Shutdown the thread pool."""
        if self._executor:
            self._executor.shutdown(wait=wait)
            self._executor = None

    def submit_task(self, func: Callable[..., T], *args, task_id: Optional[str] = None, **kwargs) -> str:
        """
        Submit a CPU-bound task for execution.

        Args:
            func: Function to execute
            *args: Function arguments
            task_id: Optional task ID
            **kwargs: Function keyword arguments

        Returns:
            Task ID for tracking
        """
        if self._executor is None:
            raise RuntimeError("ThreadPoolManager not started")

        with self._lock:
            if task_id is None:
                self._task_counter += 1
                task_id = f"thread_task_{self._task_counter}"

            if task_id in self._futures:
                raise ValueError(f"Task {task_id} already exists")

        future = self._executor.submit(self._wrap_function, func, task_id, *args, **kwargs)

        with self._lock:
            self._futures[task_id] = future
            self._results[task_id] = TaskResult(task_id=task_id, success=False)

        return task_id

    def _wrap_function(self, func: Callable[..., T], task_id: str, *args, **kwargs) -> T:
        """Wrap function with error handling."""
        result = self._results[task_id]

        try:
            start_time = datetime.now()
            task_result = func(*args, **kwargs)
            end_time = datetime.now()

            result.success = True
            result.result = task_result
            result.end_time = end_time

            logger.debug("Thread task completed successfully", task_id=task_id, duration=result.duration)

            return task_result

        except Exception as e:
            result.success = False
            result.error = str(e)
            result.end_time = datetime.now()

            logger.error("Thread task failed", task_id=task_id, error=str(e))
            raise

    def get_result(self, task_id: str, timeout: Optional[float] = None) -> TaskResult[T]:
        """Get result for a specific task."""
        if task_id not in self._results:
            raise ValueError(f"Task {task_id} not found")

        future = self._futures.get(task_id)
        if future:
            try:
                future.result(timeout=timeout)
            except Exception:
                pass

        return self._results[task_id]

    def cancel_task(self, task_id: str) -> bool:
        """Cancel a specific task."""
        future = self._futures.get(task_id)
        if future:
            cancelled = future.cancel()
            if cancelled:
                result = self._results[task_id]
                result.error = "Task was cancelled"
                result.end_time = datetime.now()
            return cancelled
        return False


class ConcurrencyManager:
    def __init__(self, limits: Optional[ConcurrencyLimits] = None):
        self.limits = limits or ConcurrencyLimits()
        self.async_manager = AsyncTaskManager(self.limits)
        self.thread_manager = ThreadPoolManager(self.limits)

    async def __aenter__(self):
        self.thread_manager.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.async_manager.cancel_all()
        self.thread_manager.shutdown()

    async def submit_async_task(
        self, coro: Awaitable[T], task_id: Optional[str] = None, rate_limit_key: Optional[str] = None
    ) -> str:
        return await self.async_manager.submit_task(coro, task_id, rate_limit_key)

    def submit_thread_task(self, func: Callable[..., T], *args, task_id: Optional[str] = None, **kwargs) -> str:
        return self.thread_manager.submit_task(func, *args, task_id=task_id, **kwargs)

    async def wait_for_async_task(self, task_id: str, timeout: Optional[float] = None) -> TaskResult[T]:
        return await self.async_manager.wait_for_task(task_id, timeout)

    async def wait_for_thread_task(self, task_id: str, timeout: Optional[float] = None) -> TaskResult[T]:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.thread_manager.get_result, task_id, timeout)

    def get_stats(self) -> Dict[str, Any]:
        return {
            "async_tasks": {
                "active": self.async_manager.get_active_task_count(),
                "completed": self.async_manager.get_completed_task_count(),
            },
            "thread_tasks": {"futures": len(self.thread_manager._futures)},
            "limits": {
                "max_concurrent_tasks": self.limits.max_concurrent_tasks,
                "max_async_connections": self.limits.max_async_connections,
                "max_threads": self.limits.max_threads,
            },
        }


concurrency_manager = ConcurrencyManager()
