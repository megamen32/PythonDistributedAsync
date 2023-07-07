<<<<<<< HEAD
import traceback
from datetime import datetime

import dill
import requests
import asyncio
from task.task import Task
import base64
SERVER_URL = "http://localhost:8001"

class Client:
    def __init__(self, server_url: str):
        self.server_url = server_url

    async def get_task(self, id: int):
        response = requests.get(f"{self.server_url}/get_task/{id}")
        task_data = response.json()['task']
        task = await self.parse_taskdata(task_data)
        return task

    async def parse_taskdata(self, task_data):
        func = dill.loads(base64.b64decode(task_data["func"]))
        args = dill.loads(base64.b64decode(task_data["args"]))
        kwargs = dill.loads(base64.b64decode(task_data["kwargs"]))
        uuid = task_data["uuid"]
        task=Task.get_or_none(Task.uuid==uuid)
        if task is None:
            task = Task.create_task(func, datetime.strptime(task_data["execution_time"], '%Y-%m-%dT%H:%M:%S.%f'), uuid, *args, **kwargs)
        return task

    async def execute_task_by_id(self, id: int):
        task = await self.get_task(id)
        return await self.execute_task(task)

    async def execute_task(self, task: Task):
        try:
            req = requests.post(f"{self.server_url}/start_task/{task.uuid}", json={"uuid": task.uuid})
            if req.status_code == 409:
                print("Task is already in progress, returning...")
                return None

            result = await task.run()
            if result is None:
                print("The task did not return any result.")
                result='None'

            response = requests.post(f"{self.server_url}/set_task_result/{task.uuid}", json={"result": base64.b64encode(dill.dumps(result)).decode()})
            print(response.content)


        except Exception as e:
            traceback.print_exc()
            result = str(e)
            response=requests.post(f"{self.server_url}/set_task_result/{task.uuid}", json={"result": 'error', "error_message": result})
        print(result,response,response.content)

        return result

    async def get_all_tasks(self):
        response = requests.get(f"{self.server_url}/get_all_tasks")
        tasks_data = response.json()['tasks']
        tasks = []
        for task_data in tasks_data:
            task = await self.parse_taskdata(task_data)
            tasks.append(task)
        return tasks

async def main():
    client = Client(SERVER_URL)
=======
import asyncio
import base64
import logging
import traceback
from datetime import datetime
from typing import Optional

import dill
import requests

from task.task import Task, db

SERVER_URL = "http://localhost:8001"
POLL_INTERVAL_SECONDS = 10

logger = logging.getLogger(__name__)


class Client:
    def __init__(self, server_url: str = SERVER_URL):
        self.server_url = server_url.rstrip("/")

    async def get_task(self, uuid: str) -> Task:
        response = requests.get(f"{self.server_url}/get_task/{uuid}", timeout=30)
        response.raise_for_status()
        return await self.parse_taskdata(response.json()["task"])

    async def parse_taskdata(self, task_data: dict) -> Task:
        func = dill.loads(base64.b64decode(task_data["func"]))
        args = dill.loads(base64.b64decode(task_data["args"]))
        kwargs = dill.loads(base64.b64decode(task_data["kwargs"]))
        task_uuid = task_data["uuid"]

        task = Task.get_or_none(Task.uuid == task_uuid)
        if task is None:
            task = Task.create_task(
                func,
                datetime.fromisoformat(task_data["execution_time"]),
                task_uuid,
                *args,
                **kwargs,
            )
        return task

    async def execute_task_by_uuid(self, uuid: str):
        task = await self.get_task(uuid)
        return await self.execute_task(task)

    async def execute_task_by_id(self, uuid: str):
        # Backward-compatible alias: the public API uses UUIDs, not integer IDs.
        return await self.execute_task_by_uuid(uuid)

    async def execute_task(self, task: Task):
        try:
            response = requests.post(f"{self.server_url}/start_task/{task.uuid}", json={}, timeout=30)
            if response.status_code == 409:
                logger.info(f"Task {task.uuid} is already in progress, done, or failed")
                return None
            response.raise_for_status()

            result = await task.run()
            encoded_result = base64.b64encode(dill.dumps(result)).decode("utf-8")
            response = requests.post(
                f"{self.server_url}/set_task_result/{task.uuid}",
                json={"result": encoded_result},
                timeout=30,
            )
            response.raise_for_status()
            logger.info(f"Task {task.uuid} completed")
            return result

        except Exception as exc:
            traceback.print_exc()
            error_result = base64.b64encode(dill.dumps(str(exc))).decode("utf-8")
            try:
                requests.post(
                    f"{self.server_url}/set_task_result/{task.uuid}",
                    json={"result": error_result, "error_message": str(exc)},
                    timeout=30,
                )
            except Exception:
                logger.exception(f"Cannot report failure for task {task.uuid}")
            return None

    async def get_all_tasks(self) -> list[Task]:
        response = requests.get(f"{self.server_url}/get_all_tasks", timeout=30)
        response.raise_for_status()
        return [await self.parse_taskdata(task_data) for task_data in response.json()["tasks"]]


async def main(server_url: Optional[str] = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    client = Client(server_url or SERVER_URL)
    db.connect(reuse_if_open=True)
    db.create_tables([Task], safe=True)

>>>>>>> 4458406 (Improve README and project structure)
    while True:
        try:
            tasks = await client.get_all_tasks()
            for task in tasks:
                await client.execute_task(task)
<<<<<<< HEAD
        except:
            traceback.print_exc()
        await asyncio.sleep(10)

if __name__ == "__main__":
    if not Task.table_exists():
        Task.create_table()
=======
        except Exception:
            logger.exception("Worker loop failed")
        await asyncio.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
>>>>>>> 4458406 (Improve README and project structure)
    asyncio.run(main())
