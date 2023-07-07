import asyncio
# Task object storing the serialized function, args, kwargs and the execution time.
import time
from enum import Enum

import peewee as pw
import dill
from datetime import datetime

import uuid as uuid

db = pw.SqliteDatabase('tasks.db')

class TaskStates(Enum):
    WAITING='waiting'
    IN_PROGRESS= 'processing'
    DONE='done'
    FAILED='failed'

class Task(pw.Model):
    func = pw.BlobField()
    args = pw.BlobField()
    kwargs = pw.BlobField()
    execution_time = pw.DateTimeField()
    status = pw.CharField(default=TaskStates.WAITING.name)
    result = pw.BlobField(null=True)
    error_message = pw.TextField(null=True)
    uuid= pw.CharField(default=uuid.uuid4,unique=True)

    class Meta:
        database = db

    @staticmethod
    def create_task(func, execution_time: datetime,uuid=None,*args, **kwargs ):
        if uuid is not None:
            return Task.create(
                func=dill.dumps(func),
                args=dill.dumps(args),
                kwargs=dill.dumps(kwargs),
                execution_time=execution_time,
                uuid=uuid
            )
        else:
            return Task.create(
                func=dill.dumps(func),
                args=dill.dumps(args),
                kwargs=dill.dumps(kwargs),
                execution_time=execution_time
            )


    async def run(self):
        current_time = datetime.now()
        if current_time < self.execution_time:
            # If it's not time to run the task, return None
            await asyncio.sleep((current_time-self.execution_time).total_seconds())
        if current_time >= self.execution_time:
            func = dill.loads(self.func)
            args = dill.loads(self.args)
            kwargs = dill.loads(self.kwargs)
            return await func(*args, **kwargs)

    async def wait_for_completion(self, check_interval=1, timeout=None):
        """
        Wait until the task is completed.
        :param check_interval: the interval (in seconds) between each check
        :param timeout: if not None, waiting will stop after this number of seconds
        :return: None
        """
        start_time = time.monotonic()
        while self.status not in (TaskStates.DONE.name, TaskStates.FAILED.name):
            if timeout is not None and time.monotonic() - start_time > timeout:
                raise TimeoutError("Waiting for task completion timed out.")
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

