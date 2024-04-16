import re
import requests
import time
from PyQt6.QtCore import (
    QRunnable
)
from queue import Empty, PriorityQueue, Queue

from globals import Globals

class Smmsky(QRunnable):
    def __init__(self):
        super().__init__()

        self.is_running = False
        self.queue = PriorityQueue()
        self.session = requests.Session()
        self.setAutoDelete(False)
        self.user = 'Smmsky'
        # 'Pending', 'In progress', 'Completed', 'Canceled', 'Partial'

        Globals._Log.info(self.user, 'Successfully initialized.')

    def add_order(self, payload):
        try:
            res = self.session.post(Globals._SMMSKY_URL, json=payload)
            print(res.text)
            now = int(time.time())
            link = payload['link']
            uniqueId, videoId = self.extract_tiktok_info(link)
            datas = {
                "orderId": res.json()['order'],
                "uniqueId": uniqueId,
                "videoId": videoId,
                "link": link,
                "service": payload['service'],
                "charge": '',
                "start_count": 0,
                "quantity": payload['quantity'],
                "remains": payload['quantity'],
                "status": '',
                "currency": '',
                "cancel": False,
                "createTime": now,
                "updateTime": now
            }
            print(datas)
            Globals._WS.database_operation_signal.emit('insert', {
                'table_name': 'smmsky_orders',
                'columns': list(datas.keys()),
                'data': datas
            }, None)
        except Exception as e:
            Globals._Log.error(self.user, f'{e}')

    def extract_tiktok_info(self, url):
        match = re.search(r'https://www.tiktok.com/@([^/]+)(?:/video/(\d+))?', url)
        if match:
            username = match.group(1)
            video_id = match.group(2) if match.group(2) else ''
            return username, int(video_id)
        return '', ''

    def get_orders(self):
        q = Queue()
        Globals._WS.database_operation_signal.emit('read', {
            'table_name': 'smmsky_orders',
            'columns': 'orderId, updateTime',
            'condition': 'status NOT IN (?, ?, ?)',
            'params': ['Completed', 'Canceled', 'Partial']
        }, q)
        res = q.get()
        self.time_latest_status = time.time()
        if not res:
            return
        sorted_data = sorted(res, key=lambda x: x[1])

        orders = ''
        count = 0
        for idx, item in enumerate(sorted_data):
            orders += f'{item[0]},'
            count += 1
            if count % 100:
                continue
            self.queue.put((sorted_data[idx-100], ('update_status', {
                'action': 'status',
                'orders': orders[:-1]
            })))
            orders = ''
        if count % 100:
            self.queue.put((sorted_data[idx-(count%100)], ('update_status', {
                'action': 'status',
                'orders': orders[:-1]
            })))

    def run(self):
        self.is_running = True
        while self.is_running and Globals.is_app_running:
            try:
                weight, (func, params) = self.queue.get(timeout=3)
                if weight > int(time.time()):
                    time.sleep(3)
                    self.queue.put((weight, (func, params)))
                    continue
            except Empty:
                # self.get_orders()
                time.sleep(3)
                continue
            payload = {'key': Globals._SMMSKY_KEY}
            if params:
                payload.update(params)
            print(payload)
            getattr(self, func)(payload)
        self.is_running = False

    def start_task(self):
        if not self.is_running:
            Globals.thread_pool.start(self)

    def stop_task(self):
        self.is_running = False

    def update_services(self, payload):
        try:
            res = self.session.post(Globals._SMMSKY_URL, json=payload)
            datas = res.json()
            columns = list(datas[0].keys())
            Globals._WS.database_operation_signal.emit('clear_bulk_insert', {
                'table_name': 'smmsky_services',
                'columns': columns,
                'data': datas
            }, None)
        except Exception as e:
            Globals._Log.error(self.user, f'{e}')

    def update_status(self, payload):
        try:
            res = self.session.post(Globals._SMMSKY_URL, json=payload)
            datas = []
            for order, data in res.json().items():
                if 'error' in data:
                    continue
                datas.append({'orderId': order})
                datas[-1].update(data)
            Globals._WS.database_operation_signal.emit('bulk_upsert', {
                'table_name': 'smmsky_orders',
                'columns': datas[0].keys(),
                'data': datas,
                'unique_columns': ['orderId'],
            }, None)
        except Exception as e:
            Globals._Log.error(self.user, f'{e}')