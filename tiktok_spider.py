import asyncio
import os
import playwright
import pygetwindow
from queue import PriorityQueue, Queue
import time
from PyQt6.QtCore import (
    QRunnable
)
from TikTokApi import TikTokApi
from TikTokApi.exceptions import EmptyResponseException

from globals import Globals
from operate_sqlite import DBSchema

class TikTokSpider(QRunnable):
    def __init__(self):
        super().__init__()

        self.api = TikTokApi()
        self.accounts_columns = list(DBSchema.tables['accounts_detail'].keys())
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

    async def spider(self):
        while self.is_running and Globals.is_app_running and not self.queue.empty():
            weight, account = self.queue.get()
            Globals._Log.debug(self.user, f"total: {self.queue.qsize()+1}, weight: {weight}, account: {account}")
            await self.ensure_browser()
            try:
                user_info = await self.api.user(username=account).info()
                user_data = user_info['userInfo']['user']
                stats = user_info['userInfo']['stats']
                values = {
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
                    'updateTime': int(time.time())
                }
                Globals._WS.database_operation_signal.emit('upsert',{
                    'table_name': 'accounts_detail',
                    'columns': self.accounts_columns,
                    'values': [values[key] for key in self.accounts_columns],
                    'unique_columns': ['userId']
                }, None)

                async for video in self.api.user(username=account).videos():
                    video_info = video.as_dict
                    title = ''
                    if 'contents' in video_info:
                        title = video_info['contents'][0]['desc']
                    stats = video_info['stats']
                    values = {
                        'videoId': video_info['id'],
                        'account': account,
                        'title': title,
                        'collectCount': stats['collectCount'],
                        'commentCount': stats['commentCount'],
                        'diggCount': stats['diggCount'],
                        'playCount': stats['playCount'],
                        'shareCount': stats['shareCount'],
                        'createTime': video_info['createTime'],
                        'updateTime': int(time.time())
                    }
                    Globals._WS.database_operation_signal.emit('upsert',{
                        'table_name': 'videos',
                        'columns': self.video_columns,
                        'values': [values[key] for key in self.video_columns],
                        'unique_columns': ['videoId']
                    }, None)
                    self.queue.put((int(time.time()), account))

            except AttributeError as a:
                Globals._Log.warning(self.user, f'Invalid data of {account}: {a}')
                await self.reboot_browser()
                self.queue.put((5, account))
            except KeyError as k:
                if 'user' in str(k):
                    Globals._Log.warning(self.user, f'The homepage of {account} cannot be found: {k}')
                    self.queue.put((int(time.time()+21600), account))
                elif 'id' in str(k):
                    Globals._Log.warning(self.user, f'Invalid data of {account}: {k}')
                    await self.reboot_browser()
                    self.queue.put((5, account))
                else:
                    Globals._Log.error(self.user, f'type: {type(k)}, details: {k}')
            except EmptyResponseException as e:
                Globals._Log.warning(self.user, f'EmptyResponseException: details: {e}')
                await self.reboot_browser()
                self.queue.put((5, account))
            except playwright._impl._errors.TargetClosedError as p:
                Globals._Log.warning(self.user, f'type: {type(p)}, details: {p}')
                await self.reboot_browser()
                self.queue.put((5, account))
            except TypeError as t:
                Globals._Log.warning(self.user, f'TypeError: details: {t}')
                await self.reboot_browser()
                self.queue.put((5, account))
            except Exception as e:
                Globals._Log.error(self.user, f'type: {type(e)}, details: {e}')
            time.sleep(1)

    async def reboot_browser(self):
        try:
            Globals._Log.info(self.user, 'Attempting to reboot the browser.')

            if hasattr(self.api, 'browser') and self.api.browser.is_connected():
                await self.api.browser.close()

            self.terminate_playwright_processes()
            del self.api
            self.api = TikTokApi()
            
            await self.api.create_sessions(num_sessions=1, sleep_after=3, headless=False)
            self.minimize_chromium()
            Globals._Log.info(self.user, 'Browser rebooted successfully.')
        except Exception as e:
            Globals._Log.error(self.user, f'Failed to reboot the browser: {e}')

    def get_accounts(self):
        q = Queue()
        accounts_list = list(Globals.accounts_dict.keys())
        accounts = set(accounts_list)
        placeholders = ','.join('?' * len(accounts))
        Globals._WS.database_operation_signal.emit('read',{
            'table_name': 'accounts_detail',
            'columns': 'updateTime,uniqueId',
            'condition': f'uniqueId IN ({placeholders})',
            'params': accounts_list
        }, q)

        items = q.get()
        for item in items:
            if item[1] not in accounts:
                continue
            if item[0]:
                self.queue.put(item)
            else:
                self.queue.put((10, item[1]))
            accounts.discard(item[1])
        for account in accounts:
            self.queue.put((10, account))

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
        os.system("taskkill /f /im chrome.exe /T")
        os.system("taskkill /f /im node.exe /T")
        Globals._Log.info(self.user, "All related browser processes have been terminated.")