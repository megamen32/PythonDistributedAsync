import asyncio
import inspect
import time
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Optional
from uuid import uuid4

import dill
import peewee as pw


db = pw.SqliteDatabase("tasks.db")


class TaskStates(Enum):
    WAITING = "waiting"
    IN_PROGRESS = "processing"
    DONE = "done"
    FAILED = "failed"


class Task(pw.Model):
    func = pw.BlobField()
    args = pw.BlobField()
    kwargs = pw.BlobField()
    execution_time = pw.DateTimeField()
    status = pw.CharField(default=TaskStates.WAITING.name, index=True)
    result = pw.BlobField(null=True)
    error_message = pw.TextField(null=True)
    uuid = pw.CharField(default=lambda: str(uuid4()), unique=True, index=True)

    class Meta:
        database = db
        table_name = "tasks"

    @staticmethod
    def create_task(
        func: Callable[..., Any],
        execution_time: datetime,
        task_uuid: Optional[str] = None,
        *args: Any,
        **kwargs: Any,
    ) -> "Task":
        payload = {
            "func": dill.dumps(func),
            "args": dill.dumps(args),
            "kwargs": dill.dumps(kwargs),
            "execution_time": execution_time,
        }
        if task_uuid is not None:
            payload["uuid"] = task_uuid
        return Task.create(**payload)

    async def run(self) -> Any:
        delay = (self.execution_time - datetime.now()).total_seconds()
        if delay > 0:
            await asyncio.sleep(delay)

        func = dill.loads(self.func)
        args = dill.loads(self.args)
        kwargs = dill.loads(self.kwargs)
        result = func(*args, **kwargs)

        if inspect.isawaitable(result):
            return await result
        return result

    async def wait_for_completion(self, check_interval: float = 1, timeout: Optional[float] = None) -> bytes:
        start_time = time.monotonic()
        while self.status not in (TaskStates.DONE.name, TaskStates.FAILED.name):
            if timeout is not None and time.monotonic() - start_time > timeout:
                raise TimeoutError("Waiting for task completion timed out.")
<<<<<<< HEAD
            await asyncio.sleep(check_interval)
            fresh_task = type(self).get(self._pk_expr())  # Here we fetch the latest task data from DB
            self.status = fresh_task.status  # Update the status of self
            if fresh_task.status == TaskStates.DONE.name or fresh_task.status == TaskStates.FAILED.name:
                return fresh_task.result

    async def __call__(self, *args, **kwargs):
        return await self.run()

    def __str__(self):
        return f"Task: {self.func} {self.args} {self.kwargs} {self.execution_time}"

    def __repr__(self):
        return f"Task: {self.func} {self.args} {self.kwargs} {self.execution_time}"

    def __eq__(self, other):
        return self.func == other.func and self.args == other.args and self.kwargs == other.kwargs and self.execution_time == other.execution_time

    def __hash__(self):
        return hash((self.func, self.args, self.kwargs, self.execution_time))

=======

            await asyncio.sleep(check_interval)
            fresh_task = type(self).get(type(self).id == self.id)
            self.status = fresh_task.status
            self.result = fresh_task.result
            self.error_message = fresh_task.error_message

        return self.result

    async def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return await self.run()

    def __str__(self) -> str:
        return f"Task(uuid={self.uuid}, status={self.status}, execution_time={self.execution_time})"

    def __repr__(self) -> str:
        return self.__str__()
>>>>>>> 4458406 (Improve README and project structure)
