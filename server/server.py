import asyncio
import base64
from datetime import datetime

import dill

from fastapi import FastAPI, Body, HTTPException
from task.task import Task, db,TaskStates
import uvicorn

app = FastAPI()


@app.post("/create_task")
async def create_task(
    func: str = Body(..., embed=True),
    args: str = Body(..., embed=True),
    kwargs: str = Body(..., embed=True),
    execution_time: str = Body(..., embed=True)
):
    func = dill.loads(base64.b64decode(func))
    args = dill.loads(base64.b64decode(args))
    kwargs = dill.loads(base64.b64decode(kwargs))
    execution_time = datetime.strptime(execution_time, '%Y-%m-%dT%H:%M:%S.%f')

    task = Task.create_task(func, execution_time, None,*args, **kwargs)
    return {"message": "Task created", "task_uuid": task.uuid}





@app.get("/get_task/{uuid}")
async def get_task(uuid: str):
    task = Task.get(Task.uuid == uuid)
    return {
        "task": {
            "func": base64.b64encode(task.func).decode(),
            "args": base64.b64encode(task.args).decode(),
            "kwargs": base64.b64encode(task.kwargs).decode(),
            "execution_time": task.execution_time,
            "status": task.status,
            "result": base64.b64encode(task.result).decode() if task.result else None,
            "error_message": task.error_message,
            "uuid": task.uuid
        }
    }
from fastapi import FastAPI, Body

from fastapi import Body, APIRouter
from pydantic import BaseModel
from typing import Optional
class TaskResult(BaseModel):
    message: str
    task_id: int
@app.get("/get_task_result/{uuid}")
async def get_task_result(uuid: str):
    task = Task.get(Task.uuid == uuid)
    try:
         await task.wait_for_completion(timeout=30)
    except TimeoutError:
        return {"status":task.status}
    task = Task.get(Task.uuid == uuid)
    if task.status == TaskStates.DONE.name:
        return {"result": dill.loads(task.result)}
    elif task.status == TaskStates.FAILED.name:
        try:
            return {"error": task.error_message,"result":dill.loads(task.result)}
        except:
            return {"error": task.error_message, "result": task.result}

@app.post("/set_task_result/{uuid}", response_model=TaskResult)
async def set_task_result(uuid: str, result: str = Body(default=...), error_message: Optional[str] = Body(default=None)):
    task = Task.get(Task.uuid == uuid)
    if error_message:
        task.status = TaskStates.FAILED.name
        task.error_message = error_message
    else:
        task.status = TaskStates.DONE.name
        task.result = base64.b64decode(result) # this is now ready for dill.loads when retrieved
    task.save()
    return TaskResult(message="Task result updated", task_id=task.id)



@app.post("/start_task/{uuid}")
async def start_task(uuid: str):
    task = Task.get(Task.uuid == uuid)
    if task.status != TaskStates.WAITING.name:
        raise HTTPException(status_code=409, detail="Task is already in progress or done")
    task.status = TaskStates.IN_PROGRESS.name
    task.save()
    return {"message": "Task status updated to IN_PROGRESS", "task_id": task.id}


import base64

@app.get("/get_all_tasks")
async def get_all_tasks():
    tasks = Task.select().where(Task.status == TaskStates.WAITING.name)
    tasks_info = []
    for task in tasks:
        tasks_info.append({
                "func": base64.b64encode(task.func).decode(),
                "args": base64.b64encode(task.args).decode(),
                "kwargs": base64.b64encode(task.kwargs).decode(),
                "execution_time": task.execution_time,
                "status": task.status,
                "result": base64.b64encode(task.result).decode() if task.result else None,
                "error_message": task.error_message,
                "uuid": task.uuid
        })
    return {"tasks": tasks_info}

if __name__ == "__main__":
    if not db.table_exists('tasks'):
        Task.create_table()
    uvicorn.run("server:app", host="0.0.0.0", port=8001)
