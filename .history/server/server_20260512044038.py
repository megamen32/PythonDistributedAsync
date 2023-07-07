import base64
from datetime import datetime
from typing import Optional

import dill
import uvicorn
from fastapi import Body, FastAPI, HTTPException
from pydantic import BaseModel

from task.task import Task, TaskStates, db

app = FastAPI(title="Python Distributed Async", version="0.2.0")


class TaskResult(BaseModel):
    message: str
    task_uuid: str


def ensure_database() -> None:
    db.connect(reuse_if_open=True)
    db.create_tables([Task], safe=True)


def encode_blob(value: Optional[bytes]) -> Optional[str]:
    if value is None:
        return None
    return base64.b64encode(value).decode("utf-8")


def serialize_task(task: Task) -> dict:
    return {
        "func": encode_blob(task.func),
        "args": encode_blob(task.args),
        "kwargs": encode_blob(task.kwargs),
        "execution_time": task.execution_time.isoformat(),
        "status": task.status,
        "result": encode_blob(task.result),
        "error_message": task.error_message,
        "uuid": task.uuid,
    }


def parse_iso_datetime(value: str) -> datetime:
    try:
        return datetime.fromisoformat(value)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=f"Invalid execution_time: {value}") from exc


@app.on_event("startup")
async def startup() -> None:
    ensure_database()


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}


@app.post("/create_task")
async def create_task(
    func: str = Body(..., embed=True),
    args: str = Body(..., embed=True),
    kwargs: str = Body(..., embed=True),
<<<<<<< HEAD
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
=======
    execution_time: str = Body(..., embed=True),
) -> dict:
    try:
        decoded_func = dill.loads(base64.b64decode(func))
        decoded_args = dill.loads(base64.b64decode(args))
        decoded_kwargs = dill.loads(base64.b64decode(kwargs))
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Cannot decode task payload") from exc

    task = Task.create_task(
        decoded_func,
        parse_iso_datetime(execution_time),
        None,
        *decoded_args,
        **decoded_kwargs,
    )
    return {"message": "Task created", "task_uuid": task.uuid, "uuid": task.uuid}


@app.get("/get_task/{uuid}")
async def get_task(uuid: str) -> dict:
    task = Task.get_or_none(Task.uuid == uuid)
    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")
    return {"task": serialize_task(task)}


@app.get("/get_task_result/{uuid}")
async def get_task_result(uuid: str) -> dict:
    task = Task.get_or_none(Task.uuid == uuid)
    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")

    try:
        await task.wait_for_completion(timeout=30)
    except TimeoutError:
        return {"status": task.status}

    fresh_task = Task.get(Task.uuid == uuid)
    if fresh_task.status == TaskStates.DONE.name:
        return {"result": dill.loads(fresh_task.result)}

    if fresh_task.status == TaskStates.FAILED.name:
        result = None
        if fresh_task.result:
            try:
                result = dill.loads(fresh_task.result)
            except Exception:
                result = fresh_task.result.decode("utf-8", errors="replace")
        return {"error": fresh_task.error_message, "result": result}

    return {"status": fresh_task.status}


@app.post("/set_task_result/{uuid}", response_model=TaskResult)
async def set_task_result(
    uuid: str,
    result: str = Body(default=..., embed=True),
    error_message: Optional[str] = Body(default=None, embed=True),
) -> TaskResult:
    task = Task.get_or_none(Task.uuid == uuid)
    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")

    if error_message:
        task.status = TaskStates.FAILED.name
        task.error_message = error_message
        task.result = base64.b64decode(result) if result and result != "error" else None
    else:
        task.status = TaskStates.DONE.name
        task.result = base64.b64decode(result)
        task.error_message = None
    task.save()
    return TaskResult(message="Task result updated", task_uuid=task.uuid)


@app.post("/start_task/{uuid}")
async def start_task(uuid: str) -> dict:
    task = Task.get_or_none(Task.uuid == uuid)
    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")
    if task.status != TaskStates.WAITING.name:
        raise HTTPException(status_code=409, detail="Task is already in progress, done, or failed")

    task.status = TaskStates.IN_PROGRESS.name
    task.save()
    return {"message": "Task status updated to IN_PROGRESS", "task_uuid": task.uuid}


@app.get("/get_all_tasks")
async def get_all_tasks() -> dict:
    tasks = Task.select().where(Task.status == TaskStates.WAITING.name).order_by(Task.execution_time.asc())
    return {"tasks": [serialize_task(task) for task in tasks]}


if __name__ == "__main__":
    ensure_database()
    uvicorn.run("server.server:app", host="0.0.0.0", port=8001)
>>>>>>> 4458406 (Improve README and project structure)
