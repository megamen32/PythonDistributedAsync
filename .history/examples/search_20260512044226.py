import base64
import datetime as dt
import time
import traceback
from typing import Any, Callable, Optional

import dill
import requests


async def opennet_news(url: str) -> list[str]:
    from io import StringIO

    from lxml import etree

    response = requests.get(url, timeout=30)
    response.raise_for_status()
    parsed_html = etree.parse(StringIO(response.text), etree.HTMLParser())
    titles = parsed_html.xpath(r"//a[@class='title2']/text()")
    print(titles)
    return titles


class TaskRunner:
    def __init__(self, server_url: str):
        self.server_url = server_url.rstrip("/")

    def create_task(self, func: Callable[..., Any], execution_time: Optional[dt.datetime] = None, *args: Any, **kwargs: Any) -> str:
        payload = {
            "func": base64.b64encode(dill.dumps(func)).decode("utf-8"),
            "args": base64.b64encode(dill.dumps(args)).decode("utf-8"),
            "kwargs": base64.b64encode(dill.dumps(kwargs)).decode("utf-8"),
            "execution_time": (execution_time or dt.datetime.now()).isoformat(),
        }
        response = requests.post(f"{self.server_url}/create_task", json=payload, timeout=30)
        response.raise_for_status()
        return response.json()["task_uuid"]

    def wait_for_result(self, task_uuid: str, poll_interval: float = 2):
        while True:
            try:
                response = requests.get(f"{self.server_url}/get_task_result/{task_uuid}", timeout=35)
                response.raise_for_status()
                payload = response.json()
                if "result" in payload or "error" in payload:
                    return response
            except Exception:
                traceback.print_exc()
            time.sleep(poll_interval)


if __name__ == "__main__":
    runner = TaskRunner("http://localhost:8001")
    task_uuid = runner.create_task(opennet_news, None, "https://www.opennet.ru/opennews/")
    result_response = runner.wait_for_result(task_uuid)
    print(result_response, result_response.text)
