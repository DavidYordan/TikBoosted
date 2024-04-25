import asyncio
import glob
import numpy as np
import os
import psutil
import shutil
import time
from queue import PriorityQueue, Queue
from PyQt6.QtCore import (
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
        self.is_running = False
        self.used_ports = {}
        self.queue = PriorityQueue()
        self.session_indexies = Queue(5)
        self.setAutoDelete(False)
        self.video_columns = list(DBSchema.tables['videos'].keys())
        self.user = 'TikTokSpider'

        Globals._Log.info(self.user, 'Successfully initialized.')

    async def get_user_info(self, session_index, account, id, status):
        Globals._Log.debug(self.user, f'test4: {session_index}')
        Globals._Log.debug(self.user, f'test4.1: {self.api.sessions}')
        start = time.time()
        user_info = await self.api.user(username=account).info(session_index=session_index)
        now = time.time()
        Globals._Log.debug(self.user, 'test5')

        Globals._MUTEX_XRAY.lock()
        Globals._Log.debug(self.user, 'test5.1')
        Globals.xray_dict[self.used_ports[session_index]]['response_times'].append(now - start)
        Globals._Log.debug(self.user, 'test5.2')
        Globals._MUTEX_XRAY.unlock()

        Globals._Log.debug(self.user, 'test6')
        updateTime = int(now) + 43200 if status == '5.退學' else int(now)
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

        Globals._Log.debug(self.user, 'test7')
        Globals._WS.update_account_table_signal.emit({account: datas})

        Globals._Log.debug(self.user, 'test8')
        Globals._WS.database_operation_signal.emit('upsert',{
            'table_name': 'accounts',
            'columns': list(datas.keys()),
            'values': list(datas.values()),
            'unique_columns': ['id']
        }, None)

    async def get_user_videos(self, session_index, account, status):
        Globals._Log.debug(self.user, 'test9')
        videos = []
        createTimeInit = int(time.time())
        Globals._Log.debug(self.user, 'test10')
        async for video in self.api.user(username=account).videos(session_index=session_index):
            video_info = video.as_dict
            title = ''
            if 'contents' in video_info:
                title = video_info['contents'][0]['desc']
            stats = video_info['stats']
            createTime = video_info['createTime']
            values = {
                'videoId': int(video_info['id']),
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
        Globals._Log.debug(self.user, 'test11')
        if not len(videos):
            return
        Globals._WS.database_operation_signal.emit('bulk_upsert',{
            'table_name': 'videos',
            'columns': list(videos[0].keys()),
            'data': videos,
            'unique_columns': ['videoId']
        }, None)

        Globals._Log.debug(self.user, 'test12')
        Globals._MUTEX_ACDICT.lock()
        accounts_createTime = Globals.accounts_dict.get(account, {}).get('createTime', 0)
        if createTimeInit < accounts_createTime:
            Globals._WS.database_operation_signal.emit('update',{
                'table_name': 'accounts',
                'updates': {'createTime': createTimeInit},
                'condition': f'account="{account}"'
            }, None)
            Globals.accounts_dict[account]['createTime'] = createTimeInit
        Globals._MUTEX_ACDICT.unlock()

        Globals._Log.debug(self.user, 'test13')
        if status == '5.退學':
            return
        
        Globals._Log.debug(self.user, 'test14')
        Globals._MUTEX_BINDING.lock()
        if account in Globals.binging_dict:
            Globals._WS.orderIssuer_binding_check_signal.emit(videos)
        else:
            Globals._WS.update_orderIssuer_order_signal.emit(videos)
        Globals._MUTEX_BINDING.unlock()

    def choice_proxy(self, session_index):
        Globals._MUTEX_XRAY.lock()
        try:
            best_port = None
            best_score = float('inf')
            anti_crawl_weight = 2.0
            speed_weight = 1.0

            max_anti_crawl = max((data['anti_crawl_count'] for data in Globals.xray_dict.values()), default=1)
            max_speed = max((max(data['response_times'], default=0) for data in Globals.xray_dict.values()), default=1)

            for port, data in Globals.xray_dict.items():
                if port in self.used_ports.values():
                    continue

                if not data['response_times']:
                    self.used_ports[session_index] = port
                    return

                avg_speed = np.mean(data['response_times'])
                normalized_anti_crawl = data['anti_crawl_count'] / max_anti_crawl
                normalized_speed = avg_speed / max_speed
                
                score = (normalized_anti_crawl * anti_crawl_weight) + (normalized_speed * speed_weight)

                Globals._Log.debug(self.user, f'caculate score {port} - {data}: {score}')

                if score < best_score:
                    best_score = score
                    best_port = port

            self.used_ports[session_index] = best_port
            Globals._Log.debug(self.user, f'Successfully selected a proxy: {best_port}')
    
        except Exception as e:
            Globals._Log.debug(self.user, f'Successfully selected a proxy: {best_port}')

        finally:
            Globals._MUTEX_XRAY.unlock()

    def get_accounts(self):
        Globals._MUTEX_ACDICT.lock()
        accounts = Globals.accounts_dict.keys()
        Globals._MUTEX_ACDICT.unlock()
        now = int(time.time())
        for account in accounts:
            weight = self.get_weight(account)
            if weight - now > 60:
                continue
            self.queue.put((weight, account))

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
            Globals._Log.debug(self.user, 'test1')
            await self.get_user_info(session_index, account, id, status)
            Globals._Log.debug(self.user, 'test2')
            await self.get_user_videos(session_index, account, status)
            Globals._Log.debug(self.user, 'test3')

        except KeyError as k:
            if 'user' in str(k):
                Globals._Log.warning(self.user, f'The homepage of {account} cannot be found: {k}')
                # updateTime = int(time.time()) + 43200
                updateTime = int(time.time()) + 300
                Globals._WS.update_account_table_signal.emit({account: {'updateTime': updateTime}})
                Globals._WS.database_operation_signal.emit('upsert', {
                    'table_name': 'accounts',
                    'columns': ['id', 'updateTime'],
                    'values': [id, updateTime],
                    'unique_columns': ['id']
                }, None)
                self.queue.put((updateTime, account))
            else:
                Globals._Log.error(self.user, f'KeyError: {type(k)}, details: {k}')
                self.queue.put((self.get_weight(account), account))

        except Exception as e:
            Globals._Log.error(self.user, f'Exception: {type(e)}, details: {e}')
            self.queue.put((self.get_weight(account), account))

        finally:
            self.session_indexies.put(session_index)

    async def main(self):
        self.is_running = True
        try:
            # await self.api.create_chromium(headless=False, override_browser_args=['--window-position=9999,9999'])
            await self.api.create_chromium(headless=False, override_browser_args=['--incognito'])
        except Exception as e:
            Globals._Log.error(self.user, f'create_chromium: {e}')
        try:
            while self.is_running and Globals.is_app_running:
                if self.queue.empty():
                    self.get_accounts()
                    if self.queue.empty:
                        time.sleep(3)
                        continue

                weight, account = self.queue.get()

                Globals._Log.debug(self.user, f"total: {self.queue.qsize()+1}, weight: {weight}, account: {account}")

                try:
                    if self.session_indexies.qsize():
                        session_index = self.session_indexies.get()
                    else:
                        if len(self.api.sessions) < 5:
                            session_index = len(self.api.sessions)
                            self.choice_proxy(session_index)
                            await self.api.create_session()
                            # await self.api.create_session({'server': f'http://127.0.0.1:40011'})
                            # await self.api.create_session({'server': f'http://127.0.0.1:{self.used_ports[session_index]}'})
                        else:
                            self.queue.put((weight, account))
                            Globals._Log.debug(self.user, f'{account} wait session')
                            time.sleep(1)
                            continue

                except Exception as e:
                    Globals._Log.error(self.user, f'main: {e}')

                await self.run_session(account, session_index)

                time.sleep(1)
        finally:
            self.is_running = False
            await self.api.close_sessions()

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
                path = process.info['exe']
                if 'playwright' in path:
                    process.terminate()
                    process.wait()
                elif 'chrome' in path and 'chrome-win' in path:
                    process.terminate()
                    process.wait()
            except:
                continue

        temp_folders = glob.glob(os.path.join(os.getenv('TEMP'), 'playwright*'))
        for folder in temp_folders:
            try:
                shutil.rmtree(folder)
                print(f'Deleted folder: {folder}')
            except Exception as e:
                print(f'Error deleting folder {folder}: {e}')