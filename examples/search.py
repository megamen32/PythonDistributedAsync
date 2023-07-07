import datetime
import base64
import time
import traceback

import requests
import dill



async def opennet_news( url):
    from lxml import etree
    from lxml.html import HTMLParser
    from io import StringIO
    res = requests.get(url)
    response_text = res.text  # Use .text to get a string instead of bytes
    parser = etree.HTMLParser()
    response = etree.parse(StringIO(response_text), parser)
    res = (response.xpath(r"//a[@class='title2']/text()"))
    print(res)
    return res

class TaskRunner:
    def __init__(self, server_url):
        self.server_url = server_url

    def create_task(self, func,execution_time=None, *args, **kwargs):
        func = base64.b64encode(dill.dumps(func)).decode('utf-8')  # encode in base64
        args = base64.b64encode(dill.dumps(args)).decode('utf-8')  # encode in base64
        kwargs = base64.b64encode(dill.dumps(kwargs)).decode('utf-8')  # encode in base64
        execution_time = execution_time.isoformat() if execution_time else datetime.datetime.now().isoformat()
        response = requests.post(f"{self.server_url}/create_task", json={
            'func': func,
            'args': args,
            'kwargs': kwargs,
            'execution_time': execution_time
        })
        return response.json()['task_uuid']

    def wait_for_result(self, task_uuid):
        while True:
            try:
                response = requests.get(f'{self.server_url}/get_task_result/{task_uuid}')
                if 'result' in response.json():
                    return response
            except:
                traceback.print_exc()
            time.sleep(10)


if __name__ == "__main__":
    runner = TaskRunner("http://localhost:8001")
    task_uuid = runner.create_task(opennet_news, None,'https://www.opennet.ru/opennews/')

    response = runner.wait_for_result(task_uuid)

    print(response, response.text)
