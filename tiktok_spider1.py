import asyncio
import psutil
import random
import time
from queue import PriorityQueue, Queue
from PyQt6.QtCore import (
    QMutex,
    QRunnable
)

from dytiktokapi import DyTikTokApi
from globals import Globals
from operate_sqlite import DBSchema

class TikTokSpider(QRunnable):
    def __init__(self):
        super().__init__()

        self.api = DyTikTokApi()
        self.accounts_columns = list(DBSchema.tables['accounts'].keys())
        self.deleted_accounts = set()
        self.lock_session = QMutex()
        self.video_columns = list(DBSchema.tables['videos'].keys())
        self.is_running = False
        self.queue = PriorityQueue()
        self.proxies = [
            {'server': 'http://127.0.0.1:30001'},
            {'server': 'http://127.0.0.1:30002'},
            {'server': 'http://127.0.0.1:30003'},
            {'server': 'http://127.0.0.1:30004'},
            {'server': 'http://127.0.0.1:30005'},
            {'server': 'http://127.0.0.1:30006'},
            {'server': 'http://127.0.0.1:30007'},
            {'server': 'http://127.0.0.1:30008'},
            {'server': 'http://127.0.0.1:30009'},
            {'server': 'http://127.0.0.1:30010'},
        ]
        self.proxies_used = set()
        self.session_indexies = Queue(5)
        self.setAutoDelete(False)
        self.user = 'TikTokSpider'

        Globals._Log.info(self.user, 'Successfully initialized.')

    async def get_user_info(self, session_index, account, id, status):
        user_info = await self.api.user(username=account).info(session_index)
        now = int(time.time())
        updateTime = now + 43200 if status == '5.退學' else now
        user_data = user_info['userInfo']['user']
        stats = user_info['userInfo']['stats']
        datas = {
            'id': id,
            'userId': user_data['id'],
            'uniqueId': user_data['uniqueId'],
            'nickname': user_data['nickname'],
            'logid': user_info['extra']['logid'],
            'diggCount': stats['diggCount'],
            'followerCount': stats['followerCount'],
            'followingCount': stats['followingCount'],
            'friendCount': stats['friendCount'],
            'heart': stats['heart'],
            'heartCount': stats['heartCount'],
            'videoCount': stats['videoCount'],
            'link': user_data.get('bioLink', {}).get('link', ''),
            'risk': user_data.get('bioLink', {}).get('risk', ''),
            'signature': user_data['signature'],
            'secUid': user_data['secUid'],
            'ttSeller': user_data['ttSeller'],
            'verified': user_data['verified'],
            'updateTime': updateTime
        }

        Globals._WS.update_account_table_signal.emit({account: datas})

        Globals._WS.database_operation_signal.emit('upsert',{
            'table_name': 'accounts',
            'columns': list(datas.keys()),
            'values': list(datas.values()),
            'unique_columns': ['id']
        }, None)

    async def get_user_videos(self, session_index, account, status):
        videos = []
        createTimeInit = int(time.time())
        async for video in self.api.user(username=account).videos(session_index):
            video_info = video.as_dict
            title = ''
            if 'contents' in video_info:
                title = video_info['contents'][0]['desc']
            stats = video_info['stats']
            createTime = video_info['createTime']
            values = {
                'videoId': video_info['id'],
                'account': account,
                'title': title,
                'collectCount': stats['collectCount'],
                'commentCount': stats['commentCount'],
                'diggCount': stats['diggCount'],
                'playCount': stats['playCount'],
                'shareCount': stats['shareCount'],
                'createTime': createTime,
                'updateTime': int(time.time())
            }
            videos.append(values)
            if createTime < createTimeInit:
                createTimeInit = createTime
        if not len(videos):
            return
        Globals._WS.database_operation_signal.emit('bulk_upsert',{
            'table_name': 'videos',
            'columns': list(videos[0].keys()),
            'data': videos,
            'unique_columns': ['videoId']
        }, None)
        accounts_createTime = Globals.accounts_dict.get(account, {}).get('createTime', 0)
        if createTimeInit < accounts_createTime:
            Globals._WS.database_operation_signal.emit('update',{
                'table_name': 'accounts',
                'updates': {'createTime': createTimeInit},
                'condition': f'account="{account}"'
            }, None)
        if status == '5.退學':
            return
        Globals._WS.update_orderIssuer_order_signal.emit(videos)
        Globals._MUTEX_BINDING.lock()
        if account in Globals.binging_dict:
            Globals._WS.orderIssuer_binding_check_signal.emit(account)
        Globals._MUTEX_BINDING.unlock()

    def choice_proxy(self):
        indexies = set([i for i in range(len(self.proxies))])
        proxy_index = random.choice(list(indexies - self.proxies_used))
        self.proxies_used.add(proxy_index)
        Globals._Log.debug(self.user, f'Successfully selected a proxy.')
        return self.proxies[proxy_index]

    def get_accounts(self):
        Globals._MUTEX_ACDICT.lock()
        accounts = Globals.accounts_dict.keys()
        Globals._MUTEX_ACDICT.unlock()
        for account in accounts:
            self.queue.put((self.get_weight(account), account))

    def get_weight(self, account):
        Globals._MUTEX_ACDICT.lock()
        try:
            data = Globals.accounts_dict[account]
            updateTime = data.get('updateTime', 0)
            if not updateTime:
                return 5
            return updateTime

        except Exception as e:
            Globals._Log.error(self.user, f'get_weight: {e}')

        finally:
            Globals._MUTEX_ACDICT.unlock()

    def insert_account(self, account):
        if self.is_running:
            self.queue.put((1, account))
            Globals._Log.info(self.user, f'Successfully inserted {account}')
        else:
            Globals._Log.warning(self.user, f'The spider is not running and cannot insert {account}')

    async def run_session(self, account, session_index):
        try:
            Globals._MUTEX_ACDICT.lock()
            id = Globals.accounts_dict[account]['id']
            status = Globals.accounts_dict[account]['status']
            Globals._MUTEX_ACDICT.unlock()
            await self.get_user_info(session_index, account, id, status)
            await self.get_user_videos(session_index, account, status)
            self.queue.put((self.get_weight(account), account))

        except KeyError as k:
            if 'user' in str(k):
                Globals._Log.warning(self.user, f'The homepage of {account} cannot be found: {k}')
                # updateTime = int(time.time()) + 43200
                updateTime = int(time.time())
                Globals._WS.update_account_table_signal.emit({account: {'updateTime': updateTime}})
                Globals._WS.database_operation_signal.emit('upsert', {
                    'table_name': 'accounts',
                    'columns': ['id', 'updateTime'],
                    'values': [Globals.accounts_dict[account]['id'], updateTime],
                    'unique_columns': ['id']
                }, None)
                self.queue.put((updateTime, account))
            else:
                Globals._Log.error(self.user, f'type: {type(k)}, details: {k}')
                self.queue.put((self.get_weight(account), account))

        except Exception as e:
            Globals._Log.error(self.user, f'type: {type(e)}, details: {e}')
            self.queue.put((self.get_weight(account), account))

        finally:
            self.lock_session.lock()
            self.session_indexies.put(session_index)
            self.lock_session.unlock()

    async def main(self):
        self.is_running = True
        try:
            await self.api.create_chromium(headless=False, override_browser_args=['--window-position=9999,9999'])
        except Exception as e:
            print(str(e))
        try:
            while self.is_running and Globals.is_app_running:
                if self.queue.empty():
                    self.get_accounts()

                weight, account = self.queue.get()
                if account in self.deleted_accounts:
                    self.deleted_accounts.discard(account)
                    Globals._Log.info(self.user, f'Deleted {account}')
                    continue
                if time.time() - weight < 60:
                    self.queue.put((weight, account))
                    continue

                Globals._Log.debug(self.user, f"total: {self.queue.qsize()+1}, weight: {weight}, account: {account}")

                self.lock_session.lock()
                try:
                    if self.session_indexies.qsize():
                        session_index = self.session_indexies.get()
                    else:
                        if len(self.api.sessions) < 5:
                            await self.api.create_session(self.choice_proxy())
                            session_index = len(self.api.sessions) - 1
                        else:
                            Globals._Log.debug(self.user, f'{self.api.sessions}')

                except Exception as e:
                    Globals._Log.error(self.user, f'main: {e}')

                finally:
                    self.lock_session.unlock()

                asyncio.run(asyncio.run, self.run_session(account, session_index))

                asyncio.sleep(1)
        finally:
            self.is_running = False
            self.api.close_sessions()

    def run(self):
        asyncio.run(self.main())

    def start_task(self):
        if not self.is_running:
            Globals.thread_pool.start(self)

    def stop_task(self):
        self.is_running = False

    def terminate_playwright_processes(self):
        for process in psutil.process_iter(['pid', 'name', 'exe']):
            try:
                if 'playwright' in process.info['exe']:
                    process.terminate()
                    process.wait()
            except:
                continue