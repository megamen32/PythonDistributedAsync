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
    while True:
        tasks = await client.get_all_tasks()
        for task in tasks:
            await client.execute_task(task)
        await asyncio.sleep(10)

if __name__ == "__main__":
    if not Task.table_exists():
        Task.create_table()
    asyncio.run(main())
