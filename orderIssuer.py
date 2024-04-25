import asyncio
import pandas as pd
import re
import requests
import time
import websockets
from PyQt6.QtCore import (
    QRunnable
)
from queue import Empty, PriorityQueue, Queue
from random import randint, uniform

from globals import Globals

class OrderIssuer(QRunnable):
    def __init__(self):
        super().__init__()

        self.is_running = False
        self.columns = []
        self.binding_columns = []
        self.binding_queue = Queue()
        self.queue = PriorityQueue()
        self.queue_targets = PriorityQueue()
        self.server_follower = [4057]
        self.server_digg = [2327, 3558]
        # self.server_play = [1790]
        # self.server_play = [1757]
        self.server_play = [2830]
        # self.server_play = [2787]
        self.session = requests.Session()
        self.setAutoDelete(False)
        self.temp_dict = {}
        self.user = 'OrderIssuer'

        # self.OI = OrderIssuerWs()
        # 'Processing', 'Pending', 'In progress', 'Completed', 'Canceled', 'Partial'

        Globals._Log.info(self.user, 'Successfully initialized.')

    def add_order(self, payload):
        try:
            Globals._Log.debug(self.user, f'{payload}')
            payload['key'] = Globals._ORDERISSUER_PARAMS['smmsky']['key']
            res = self.session.post(Globals._ORDERISSUER_PARAMS['smmsky']['url'], json=payload)
            data = res.json()
            Globals._Log.debug(self.user, f'{data}')
            if 'error' in data:
                Globals._Log.error(self.user, f'add_order: {data["error"]}')
                return
            now = int(time.time())
            link = payload['link']
            uniqueId, videoId = self.extract_tiktok_info(link)
            datas = {
                "client": Globals._CLIENT_ID,
                "platform": 'smmsky',
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
                "updateTime": now,
                "rate": '0'
            }
            Globals._WS.database_operation_signal.emit('insert', {
                'table_name': 'orderIssuer_orders',
                'columns': list(datas.keys()),
                'data': datas
            }, None)
            Globals._Log.debug(self.user, f"add_order: {payload['service']} - {payload['quantity']} - {link}")
        except Exception as e:
            Globals._Log.error(self.user, f'add_order: {e}')

    def binding_check(self, params):
        try:
            uniqueId = params['videos'][-1]['account']
            Globals._WS.database_operation_signal.emit('execute_query', {
                'query': f"""
                        SELECT vt.*, a.followerCount, v.diggCount, v.playCount
                        FROM video_targets vt
                        JOIN accounts a ON vt.uniqueId = a.uniqueId
                        JOIN videos v ON vt.videoId = v.videoId
                        WHERE vt.uniqueId = '{uniqueId}' AND vt.finished IS NOT TRUE;
                    """
            }, self.binding_queue)
            bindings = self.binding_queue.get()
            target = {}
            if len(bindings) != 1:
                Globals._Log.error(self.user, f'binding_check error: {bindings}')
                return
            for idx, column in enumerate(self.binding_columns):
                target[column] = bindings[0][idx]
            Globals._Log.debug(self.user, f'binding_check: {target}')
            videoId = target['videoId']
            followerTarget = target['followerTarget']
            follower = target['followerCount'] - target['followerInit']
            diggTarget = target['diggTarget']
            digg = target['diggCount'] - target['diggInit']
            playTarget = target['playTarget']
            play = target['playCount'] - target['playInit']
            if ((follower > followerTarget) and
                (digg > diggTarget) and
                (play > playTarget)
            ):
                Globals._WS.database_operation_signal.emit('update', {
                    'table_name': 'video_targets',
                    'updates': {
                            'diggCurrent': digg,
                            'followerCurrent': follower,
                            'playCurrent': play,
                            'finished': True,
                            'updateTime': int(time.time())
                        },
                        'condition': f'id={target["id"]}'
                    }, None)
                Globals._MUTEX_BINDING.lock()
                del Globals.binging_dict[uniqueId]
                Globals._MUTEX_BINDING.unlock()
                return
            Globals._Log.debug(self.user, f'{follower} - {digg} - {play}')
            if not self.check_video(videoId, 'play'):
                self.make_play_order(uniqueId, videoId, follower, digg, play, followerTarget, diggTarget, playTarget)
            if not self.check_video(videoId, 'digg'):
                self.make_digg_order(uniqueId, videoId, follower, digg, play, diggTarget, followerTarget)
            if not self.check_video(uniqueId, 'follower'):
                self.make_follower_order(uniqueId, follower, digg, followerTarget)

            Globals._WS.database_operation_signal.emit('update', {
                'table_name': 'video_targets',
                'updates': {
                    'diggCurrent': digg,
                    'followerCurrent': follower,
                    'playCurrent': play,
                    'updateTime': int(time.time())
                },
                'condition': f'id={target["id"]}'
            }, None)

            self.calculate_orders(params)
            
        except Exception as e:
            Globals._Log.error(self.user, f'binding_check: {e}')

    def check_video(self, Id, action):
        q = Queue()
        if action == 'play':
            service = self.server_play[0]
            Globals._WS.database_operation_signal.emit('read', {
                'table_name': 'orderIssuer_orders',
                'condition': f"videoId = '{Id}' AND service = {service} AND status NOT IN (?, ?, ?)",
                'params': ['Completed', 'Canceled', 'Partial']
            }, q)
        elif action == 'digg':
            service = self.server_digg[0]
            Globals._WS.database_operation_signal.emit('read', {
                'table_name': 'orderIssuer_orders',
                'condition': f"videoId = '{Id}' AND service = {service} AND status NOT IN (?, ?, ?)",
                'params': ['Completed', 'Canceled', 'Partial']
            }, q)
        elif action == 'follower':
            service = self.server_follower[0]
            Globals._WS.database_operation_signal.emit('read', {
                'table_name': 'orderIssuer_orders',
                'condition': f"uniqueId = '{Id}' AND service = {service} AND status NOT IN (?, ?, ?)",
                'params': ['Completed', 'Canceled', 'Partial']
            }, q)

        else:
            Globals._Log.error(self.user, f'check_video: {action}')
            return 1
        res = q.get()
        Globals._Log.debug(self.user, f'check_video: {res}')
        return res
    
    def calculate_orders(self, params):
        videos = params['videos']
        orders = self.get_active_orders()
        df_orders = pd.DataFrame(orders, columns=self.columns)
        now = int(time.time())

        try:
            for video in videos:
                createTime = video['createTime']
                if (now - createTime) > (60*60*24):
                    continue
                videoId = video['videoId']
                account = video['account']
                link = f'https://www.tiktok.com/@{account}/video/{videoId}'
                if video['playCount'] < 400:
                    df_play = df_orders[(df_orders['service'].isin(self.server_play)) & (df_orders['videoId']==videoId)]
                    if df_play.empty:
                        self.safe_put((5, ('add_order', {
                            'action': 'add',
                            'service': self.server_play[0],
                            'link': link,
                            'quantity': max(100, randint(500, 1000) - video['playCount'])
                        })))
                    # else:
                    #     df_play_process = df_play
                if video['playCount'] > 200 and video['diggCount'] < 10:
                    df_digg = df_orders[(df_orders['service'].isin(self.server_digg)) & (df_orders['videoId']==videoId)]
                    if df_digg.empty:
                        self.safe_put((5, ('add_order', {
                            'action': 'add',
                            'service': self.server_digg[0],
                            'link': link,
                            'quantity': max(10, randint(10, 40) - video['diggCount'])
                        })))

        except Exception as e:
            Globals._Log.error(self.user, f'calculate_orders: {e}')

    def extract_tiktok_info(self, url):
        match = re.search(r'https://www.tiktok.com/@([^/]+)(?:/video/(\d+))?', url)
        if match:
            username = match.group(1)
            video_id = match.group(2) if match.group(2) else ''
            try:
                video_id = int(video_id)
            except:
                video_id = 0
            return username, video_id
        return '', ''
    
    def get_bindings(self):
        q = Queue()
        Globals._WS.database_operation_signal.emit('read', {
            'table_name': 'video_targets',
            'condition': "finished IS NOT TRUE"
        }, q)
        res = q.get()
        Globals._MUTEX_BINDING.lock()
        for r in res:
            Globals.binging_dict[r[self.binding_columns.index('uniqueId')]] = r
        Globals._MUTEX_BINDING.unlock()
    
    def get_fields(self):
        q =Queue()
        Globals._WS.database_operation_signal.emit('get_table_fields', {
            'table_name': 'orderIssuer_orders'
        }, q)
        self.columns = q.get()

        Globals._WS.database_operation_signal.emit('get_table_fields', {
            'table_name': 'video_targets'
        }, q)
        self.binding_columns = q.get() + ['followerCount', 'diggCount', 'playCount']

    def get_active_orders(self):
        q = Queue()
        Globals._WS.database_operation_signal.emit('read', {
            'table_name': 'orderIssuer_orders',
            'condition': 'status NOT IN (?, ?, ?)',
            'params': ['Completed', 'Canceled', 'Partial']
        }, q)
        return q.get()
    
    def make_digg_order(self, uniqueId, videoId, follower, digg, play, diggTarget, followerTarget):
        try:
            link = f'https://www.tiktok.com/@{uniqueId}/video/{videoId}'
            if play < 300:
                Globals._Log.debug(self.user, f'play < 300')
                return
            elif digg > diggTarget * 1.2:
                Globals._Log.debug(self.user, f'diggTarget > digg * 1.2')
                return
            elif digg * 6 > play:
                Globals._Log.debug(self.user, f'digg * 6 > play')
                return
            elif (digg > diggTarget) or (digg > max(100, follower) * 8) or (digg * 8 > play):
                Globals._Log.debug(self.user, f'(digg > diggTarget) or (digg > max(100, follower) * 8) or (digg * 8 > play)')
                self.safe_put((5, ('add_order', {
                    'action': 'add',
                    'service': self.server_digg[0],
                    'link': link,
                    'quantity': 10
                })))
            elif digg > max(100, follower) * 6:
                Globals._Log.debug(self.user, f'digg > max(100, follower) * 6')
                self.safe_put((5, ('add_order', {
                    'action': 'add',
                    'service': self.server_digg[0],
                    'link': link,
                    'quantity': max(10, min(randint(50, 60), diggTarget - digg))
                })))
            else:
                Globals._Log.debug(self.user, f'else')
                self.safe_put((5, ('add_order', {
                    'action': 'add',
                    'service': self.server_digg[0],
                    'link': link,
                    'quantity': max(100, min(randint(500, 600), diggTarget - digg))
                })))
        except Exception as e:
            Globals._Log.error(self.user, f'make_digg_order: {e}')

    def make_follower_order(self, uniqueId, follower, digg, followerTarget):
        try:
            link = f'https://www.tiktok.com/@{uniqueId}'
            if digg < 50 or follower > followerTarget:
                Globals._Log.debug(self.user, f'digg < 50 or follower > followerTarget')
                return
            elif follower * 2 > digg:
                Globals._Log.debug(self.user, f'follower * 2 > digg')
                return
            elif follower * 3 > digg:
                Globals._Log.debug(self.user, f'follower * 3 > digg')
                self.safe_put((5, ('add_order', {
                    'action': 'add',
                    'service': self.server_follower[0],
                    'link': link,
                    'quantity': 100
                })))
            else:
                Globals._Log.debug(self.user, f"else")
                count = followerTarget - follower
                if count < 300:
                    need = count
                else:
                    need = randint(200, min(300, count - 100))
                self.safe_put((5, ('add_order', {
                    'action': 'add',
                    'service': self.server_follower[0],
                    'link': link,
                    'quantity': max(100, need)
                })))
        except Exception as e:
            Globals._Log.error(self.user, f'make_follower_order: {e}')

    def make_play_order(self, uniqueId, videoId, follower, digg, play, followerTarget, diggTarget, playTarget):
        try:
            link = f'https://www.tiktok.com/@{uniqueId}/video/{videoId}'
            if play < 5000:
                Globals._Log.debug(self.user, f'play < 5000')
                self.safe_put((5, ('add_order', {
                    'action': 'add',
                    'service': self.server_play[0],
                    'link': link,
                    'quantity': randint(5000, 6000)
                })))
            elif play > playTarget * 1.5:
                Globals._Log.debug(self.user, f'play > playTarget * 1.5: {play} {playTarget}')
                return
            elif play > max(50, digg) * 30 or play > max(100, follower) * 180:
                Globals._Log.debug(self.user, f'play > max(50, digg) * 30 or play > max(100, follower) * 180')
                return
            elif follower > followerTarget and digg > diggTarget:
                Globals._Log.debug(self.user, f'follower > followerTarget and digg > diggTarget')
                self.safe_put((5, ('add_order', {
                    'action': 'add',
                    'service': self.server_play[0],
                    'link': link,
                    'quantity': max(100, playTarget - play)
                })))
            elif play > playTarget or play > max(50, digg) * 22 or play > max(100, follower) * 132:
                Globals._Log.debug(self.user, f'play > playTarget or play > max(50, digg) * 20 or play > max(100, follower) * 120')
                self.safe_put((5, ('add_order', {
                    'action': 'add',
                    'service': self.server_play[0],
                    'link': link,
                    'quantity': 100
                })))
            elif play > max(50, digg) * 18 or play > max(100, follower) * 108:
                Globals._Log.debug(self.user, f'play > max(50, digg) * 10 or play > max(100, follower) * 80')
                self.safe_put((5, ('add_order', {
                    'action': 'add',
                    'service': self.server_play[0],
                    'link': link,
                    'quantity': max(100, min(randint(500, 600), playTarget - play))
                })))
            else:
                Globals._Log.debug(self.user, f'else')
                self.safe_put((5, ('add_order', {
                    'action': 'add',
                    'service': self.server_play[0],
                    'link': link,
                    'quantity': max(100, min(randint(5000, 6000), playTarget - play))
                })))
        except Exception as e:
            Globals._Log.error(self.user, f'make_play_order: {e}')

    def make_tasks(self, params={}):
        res = self.get_active_orders()
        if not res:
            return
        orderId_col = self.columns.index('orderId')
        updateTime_col = self.columns.index('updateTime')
        sorted_data = sorted(res, key=lambda x: x[updateTime_col])
        self.temp_dict.clear()
        for i in range(int(len(sorted_data) / 100)+1):
            if sorted_data[(100*i):(100*(i+1))]:
                orders = ''
                for data in sorted_data[(100*i):(100*(i+1))]:
                    orders += f'{str(data[orderId_col])},'
                    self.temp_dict[str(data[orderId_col])] = data
                orders = orders[:-1]
                # orders = ','.join([str(data[orderId_col]) for data in sorted_data[(100*i):(100*(i+1))]])
                self.safe_put((sorted_data[i][updateTime_col]+10, ('update_status', {
                    'action': 'status',
                    'orders': orders
                })))

    def run(self):
        self.is_running = True
        if not self.columns:
            self.get_fields()
        self.get_bindings()
        while self.is_running and Globals.is_app_running:
            try:
                now = time.time()
                weight, (func, params) = self.queue.get(timeout=3)
                if weight > now:
                    time.sleep(3)
                    self.safe_put((weight, (func, params)))
                    continue
                getattr(self, func)(params)
            except Empty:
                self.make_tasks()
                continue
        self.is_running = False

    def safe_put(self, t):
        weight, (func, params) = t
        while True:
            try:
                w = weight + uniform(0, 1)
                self.queue.put((w, (func, params)))
                return
            except TypeError as t:
                Globals._Log.warning(self.user, f'safe_put retry: {e}')
                continue
            except Exception as e:
                Globals._Log.error(self.user, f'safe_put: {e}')
                return

    def start_task(self):
        if not self.is_running:
            Globals.thread_pool.start(self)

    def stop_task(self):
        self.is_running = False

    def update_services(self, payload):
        try:
            for platform in Globals._ORDERISSUER_PARAMS.keys():
                payload['key'] = Globals._ORDERISSUER_PARAMS[platform]['key']
                res = self.session.post(Globals._ORDERISSUER_PARAMS[platform]['url'], json=payload)
                datas = res.json()
                if 'error' in datas:
                    Globals._Log.error(self.user, f'update_services-{platform}: {datas}')
                    continue

                Globals._WS.database_operation_signal.emit('delete', {
                    'table_name': 'orderIssuer_services',
                    'condition': f'platform = "{platform}"'
                }, None)
                for data in datas:
                    data['platform'] = platform
                    Globals._WS.database_operation_signal.emit('insert', {
                        'table_name': 'orderIssuer_services',
                        'columns': list(data.keys()),
                        'data': data
                    }, None)
        except Exception as e:
            Globals._Log.error(self.user, f'update_services: {e}')

    def update_status(self, payload):
        try:
            payload['key'] = Globals._ORDERISSUER_PARAMS['smmsky']['key']
            res = self.session.post(Globals._ORDERISSUER_PARAMS['smmsky']['url'], json=payload)
            datas = []
            duration = 0
            rate = 0
            now = int(time.time())
            quantityIndex = self.columns.index('quantity')
            createTimeIndex = self.columns.index('createTime')
            for order, data in res.json().items():
                if 'error' in data:
                    Globals._Log.warning(self.user, f'update_status: {data["error"]}')
                    continue
                old_data = self.temp_dict.get(order)
                if old_data:
                    duration = now - old_data[createTimeIndex]
                    rate = str(round(((old_data[quantityIndex] - int(data['remains'])) / duration) * 60, 3))
                datas.append({'client': Globals._CLIENT_ID, 'platform': 'smmsky', 'orderId': order, 'updateTime': now, 'duration': duration, 'rate': rate})
                datas[-1].update(data)
            Globals._WS.database_operation_signal.emit('bulk_upsert', {
                'table_name': 'orderIssuer_orders',
                'columns': datas[0].keys(),
                'data': datas,
                'unique_columns': ['platform', 'orderId'],
            }, None)

        except Exception as e:
            Globals._Log.error(self.user, f'update_status: {e}')


class OrderIssuerWs(object):
    def __init__(self):
        self.uri = Globals._SYNC_ORDERS_SERVER
        self.is_active = False
        self.send_queue = asyncio.Queue()
        self.user = 'OrderIssuerWs'
        self.uuid = Globals._SYNC_ORDERS_UUID
        self.websocket = None

    async def connect(self):
        self.is_active = True
        while self.is_active and Globals.is_app_running:
            if self.websocket is None or self.websocket.closed:
                try:
                    self.websocket = await websockets.connect(self.uri)
                    Globals._Log.info(self.user, 'WebSocket connection established.')
                except Exception as e:
                    Globals._Log.error(self.user, f'Failed to connect: {e}')
                    await asyncio.sleep(5)
                    continue

            receive_task = asyncio.create_task(self.receive_messages())
            send_task = asyncio.create_task(self.send_messages())
            await asyncio.wait([receive_task, send_task], return_when=asyncio.FIRST_COMPLETED)

            if self.websocket.closed:
                Globals._Log.error(self.user, 'WebSocket connection closed unexpectedly.')
                self.websocket = None

    async def on_message(self, message):
        Globals._Log.info(self.user, f"Received from server: {message}")

    async def receive_messages(self):
        while self.is_active and not self.websocket.closed:
            try:
                message = await self.websocket.recv()
                await self.on_message(message)
            except websockets.ConnectionClosed:
                break

    async def send_messages(self):
        while self.is_active:
            message = await self.send_queue.get()
            if message is None:
                break
            if not self.websocket.closed:
                try:
                    await self.websocket.send(message)
                except websockets.ConnectionClosed:
                    Globals._Log.error(self.user, 'Failed to send message: Connection closed')
                    break

    async def send_message(self, message):
        await self.send_queue.put(message)

    async def handle_messages(self, websocket):
        while self.is_active and Globals.is_app_running:
            message = await websocket.recv()
            Globals._Log.info(self.user, f"Received from server: {message}")
            # Here, you can process the message

    def stop(self):
        self.is_active = False
        self.send_queue.put_nowait(None)
        Globals._Log.info(self.user, 'Stopping WebSocket client...')