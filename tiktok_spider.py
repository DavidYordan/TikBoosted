import asyncio
import glob
import os
import psutil
import pygetwindow
import random
import shutil
from queue import PriorityQueue, Queue
import time
from PyQt6.QtCore import (
    QRunnable
)
from TikTokApi import TikTokApi

from globals import Globals
from operate_sqlite import DBSchema

class TikTokSpider(QRunnable):
    def __init__(self):
        super().__init__()

        self.api = TikTokApi()
        self.accounts_columns = list(DBSchema.tables['accounts'].keys())
        self.video_columns = list(DBSchema.tables['videos'].keys())
        self.is_running = False
        self.queue = PriorityQueue()
        self.setAutoDelete(False)
        self.user = 'TikTokSpider'

        Globals._Log.info(self.user, 'Successfully initialized.')

    async def ensure_browser(self):
        if hasattr(self.api, 'browser'):
            if self.api.browser.is_connected():
                return
        await self.reboot_browser()

    async def get_user_info(self, account, id, status):
        user_info = await self.api.user(username=account).info()
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

    async def get_user_videos(self, account, id, status):
        videos = []
        createTimeInit = int(time.time())
        async for video in self.api.user(username=account).videos():
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
        if not len(videos):
            return
        Globals._WS.database_operation_signal.emit('bulk_upsert',{
            'table_name': 'videos',
            'columns': list(videos[0].keys()),
            'data': videos,
            'unique_columns': ['videoId']
        }, None)

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

        if status == '5.退學':
            return
        
        Globals._MUTEX_BINDING.lock()
        if account in Globals.binging_dict:
            Globals._WS.orderIssuer_binding_check_signal.emit(videos)
        else:
            Globals._WS.update_orderIssuer_order_signal.emit(videos)
        Globals._MUTEX_BINDING.unlock()

    async def spider(self):
        while self.is_running and Globals.is_app_running and not self.queue.empty():
            weight, account = self.queue.get()
            if time.time() - weight < 60:
                self.queue.put((weight, account))
                continue
            Globals._Log.debug(self.user, f"total: {self.queue.qsize()+1}, weight: {weight}, account: {account}")
            await self.ensure_browser()
            try:
                Globals._MUTEX_ACDICT.lock()
                id = Globals.accounts_dict[account]['id']
                status = Globals.accounts_dict[account]['status']
                Globals._MUTEX_ACDICT.unlock()
                await self.get_user_info(account, id, status)
                await self.get_user_videos(account, id, status)
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
                    await self.reboot_browser()
                    self.queue.put((self.get_weight(account), account))

            except Exception as e:
                Globals._Log.error(self.user, f'type: {type(e)}, details: {e}')
                await self.reboot_browser()
                self.queue.put((self.get_weight(account), account))

            time.sleep(random.uniform(1,2))

    async def reboot_browser(self):
        try:
            Globals._Log.info(self.user, 'Attempting to reboot the browser.')

            if hasattr(self.api, 'browser') and self.api.browser.is_connected():
                await self.api.close_sessions()

            self.terminate_playwright_processes()
            del self.api
            time.sleep(1)
            self.api = TikTokApi()
            
            await self.api.create_sessions(num_sessions=1, sleep_after=5, headless=False, override_browser_args=['--window-position=9999,9999'])
            self.minimize_chromium()
            Globals._Log.info(self.user, 'Browser rebooted successfully.')
        except Exception as e:
            Globals._Log.error(self.user, f'Failed to reboot the browser: {e}')

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

    def minimize_chromium(self):
        try:
            windows = pygetwindow.getAllWindows()
            for window in windows:
                if 'Chromium' not in window.title:
                    continue
                window.minimize()
                return
        except Exception as e:
            Globals._Log.warning(self.user, f'{e}')

    def run(self):
        self.is_running = True
        try:
            while self.is_running and Globals.is_app_running:
                if self.queue.empty():
                    self.get_accounts()
                asyncio.run(self.spider())
                time.sleep(1)
        except Exception as e:
            Globals._Log.error(self.user, f'{e}')
        finally:
            self.is_running = False

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

        temp_folders = glob.glob(os.path.join(os.getenv('TEMP'), 'chromium*', 'chrome-win', 'temp', 'playwright*'))
        for folder in temp_folders:
            try:
                shutil.rmtree(folder)
                print(f'Deleted folder: {folder}')
            except Exception as e:
                print(f'Error deleting folder {folder}: {e}')