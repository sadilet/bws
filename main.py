import os
import json
import logging

from multiprocessing import Process, Lock, Manager
from multiprocessing.pool import ThreadPool
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.request import urlopen
from urllib.parse import urlparse, parse_qs
from functools import reduce

from entities import Task
from http_decorators import check_params_and_path
from utils import qsort, merge

THIS_PATH = os.getcwd()
FULL_PATH_TO_RAW_DATA = THIS_PATH + '/raw_data/raw_data_for_{task_id}'
FULL_PATH_TO_RESULT_DATA = THIS_PATH + '/result_data/result_data_for_{task_id}'

logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', level=logging.DEBUG)


class HttpProcessor(BaseHTTPRequestHandler):
    queue = None
    map_job_id_to_total_info = None
    lock = None

    def response(self, status_code, data, content_type='application/json'):
        self.send_response(status_code)
        self.send_header('content-type', content_type)
        self.end_headers()
        self.wfile.write(bytes(data, 'utf-8'))

    @check_params_and_path
    def do_GET(self):
        parsed_url = urlparse(self.path)
        job_id = int(parse_qs(parsed_url.query)['get'][0])
        with self.lock:
            state = self.map_job_id_to_total_info.get(job_id, {}).get('state')
        if state:
            if state == Task.READY:
                full_path_to_result = FULL_PATH_TO_RESULT_DATA.format(task_id=job_id)
                with open(full_path_to_result, 'r') as file:
                    numbers = list(map(int, file.readline().split(',')))
                self.response(200, data=json.dumps({'state': Task.STATES[state], 'data': numbers}))
                with self.lock:
                    self.map_job_id_to_total_info.pop(job_id)
                os.remove(full_path_to_result)
            elif state == Task.ERROR:
                with self.lock:
                    self.map_job_id_to_total_info.pop(job_id)
                self.response(200, json.dumps({'state': Task.STATES[state], 'data': []}))
            else:
                self.response(200, data=json.dumps({'state': Task.STATES[state], 'data': []}))
        else:
            self.response(404, data=json.dumps({'state': 'eexists', 'data': []}))

    @check_params_and_path
    def do_POST(self):
        parsed_url = urlparse(self.path)
        parsed_query_params = parse_qs(parsed_url.query)
        concurrency = int(parsed_query_params['concurrency'][0])
        url = parsed_query_params['url'][0]
        task = Task(concurrency=concurrency, url=url)
        self.queue.put(task)
        logging.info('Put task to queue with id: {id}'.format(id=task.id))
        with self.lock:
            self.map_job_id_to_total_info[task.id] = {'state': Task.QUEUED}
        self.response(200, data=json.dumps({'job_id': task.id}))


class Server:
    def __init__(self, bind_address):
        self.manager = Manager()
        self.queue = self.manager.Queue()
        self.map_job_id_to_total_info = self.manager.dict()
        self.lock = Lock()
        self.bind_address = bind_address
        self.httpd = HTTPServer(bind_address, HttpProcessor)

    def run(self):
        HttpProcessor.queue = self.queue
        HttpProcessor.lock = self.lock
        HttpProcessor.map_job_id_to_total_info = self.map_job_id_to_total_info
        logging.info('Starting server at 127.0.0.1:{port}'.format(port=self.bind_address[1]))
        self.httpd.serve_forever()

    def stop(self):
        logging.info('Stopping server.')
        self.httpd.shutdown()


class BaseWorker(Process):
    def __init__(self, queue, lock, map_job_id_to_total_info, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.queue = queue
        self.map_job_id_to_total_info = map_job_id_to_total_info
        self.lock = lock

    @staticmethod
    def _chunks(data, n_parts):
        data_length = len(data)
        if data_length % n_parts != 0:
            elements_count_for_each_thread = data_length // n_parts
            for enum, i in enumerate(range(0, data_length, elements_count_for_each_thread)):
                if enum == n_parts - 1:
                    yield data[i:]
                    break
                yield data[i:i + elements_count_for_each_thread]
        else:
            for i in range(0, len(data), n_parts):
                yield data[i:i + n_parts]

    def run(self):
        while True:
            task = self.queue.get()
            logging.info('Fetching task with id: {id}'.format(id=task.id))
            with self.lock:
                self.map_job_id_to_total_info[task.id] = {'state': Task.PROGRESS}
            try:
                with urlopen(task.url) as connection:
                    logging.info('Connected to url: {url}, trying to fetch data.'.format(url=task.url))
                    get_int_from_line = list(map(int, connection.readline().decode('utf-8').strip().split(', ')))
                    assert len(get_int_from_line) >= task.concurrency
                    result = ','.join(str(i) for i in get_int_from_line)
                    full_path_to_raw = FULL_PATH_TO_RAW_DATA.format(task_id=task.id)
                    with open(full_path_to_raw, 'w') as file:
                        file.write(result)
            except Exception as e:
                logging.error('Caught the exception: {e}. Set "error" status for task.'.format(e=e))
                with self.lock:
                    self.map_job_id_to_total_info[task.id] = {'state': Task.ERROR}
            else:
                with open(full_path_to_raw, 'r') as file:
                    numbers = list(map(int, file.readline().split(',')))
                pool = ThreadPool(processes=task.concurrency)
                sorted_lists = pool.map(qsort, self._chunks(numbers, task.concurrency))
                pool.close()
                pool.join()
                result = ','.join(str(i) for i in reduce(merge, sorted_lists))
                with open(FULL_PATH_TO_RESULT_DATA.format(task_id=task.id), 'w') as file:
                    file.write(result)
                with self.lock:
                    self.map_job_id_to_total_info[task.id] = {'state': Task.READY}
                os.remove(full_path_to_raw)


def main():
    bind_address = ('', 8888)
    server = Server(bind_address=bind_address)
    worker = BaseWorker(queue=server.queue, lock=server.lock, map_job_id_to_total_info=server.map_job_id_to_total_info)
    worker.daemon = True
    try:
        worker.start()
        server.run()
    except KeyboardInterrupt:
        print('Shutting down due to Keyboard interrupt')
    finally:
        worker.terminate()
        worker.join()
        server.stop()


if __name__ == '__main__':
    main()
