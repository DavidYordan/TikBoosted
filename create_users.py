import asyncio
import requests
import random
import os
import pandas as pd
import string
import time
from datetime import datetime
from PyQt6.QtCore import (
    QRunnable
)

import firstnames
import lastnames

from globals import Globals

class CreateUsers(QRunnable):
    def __init__(self):
        super().__init__()

        self.email_domains = {
            '@gmail.com': 0.44,
            '@outlook.com': 0.25,
            '@yahoo.com': 0.12,
            '@hotmail.com': 0.07,
            '@aol.com': 0.02,
            '@zoho.com': 0.02,
            '@icloud.com': 0.02,
            '@mail.com': 0.02,
            '@email.com': 0.02,
            '@fastmail.com': 0.02
        }
        self.domains, self.probabilities = zip(*self.email_domains.items())
        self.is_running = False
        self.records_path = 'records/records.json'
        self.setAutoDelete(False)
        self.url_zh = 'https://asia.reelshors.com/sqx_fast/app/Login/registerCode?password=d135246&phone={}&msg=9999&inviterCode={}&inviterType=0&inviterUrl=&platform=h5'
        self.url_en = 'https://www.reelshors.com/sqx_fast/app/Login/registerCode?password=d135246&phone={}&msg=9999&inviterCode={}&inviterType=0&inviterUrl=&platform=h5'
        self.url_update_zh = 'https://asia.reelshors.com/sqx_fast/app/user/updateUsers'
        self.url_update_en = 'https://www.reelshors.com/sqx_fast/app/user/updateUsers'
        self.url_invitation = ''
        self.user = 'CreateUsers'

        Globals._Log.info(self.user, 'Successfully initialized.')

    def run(self):
        self.is_running = True
        # try:
        while self.is_running and Globals.is_app_running:
            print(Globals.thread_pool.activeThreadCount())
            if self.queue.empty():
                self.get_accounts()
            asyncio.run(self.spider())
            time.sleep(1)
        # except Exception as e:
        #     Globals._Log.error(self.user, f'{e}')
        # finally:
        self.is_running = False

    def start_task(self):
        if not self.is_running:
            Globals.thread_pool.start(self)

    def stop_task(self):
        self.is_running = False

    def create_account(self):
        urls = self.url_invitation.strip().split('\n')
        print(len(urls))
        while True:
            for url in urls:
                url = url.replace('https://', '')
                number = self.random_call()
                if 'www.reelshors.com' in url:
                    formatted_url = self.url_en.format(number, url[-6:])
                elif 'asia.reelshors.com' in url:
                    formatted_url = self.url_zh.format(number, url[-6:])
                else:
                    print(f'error:{url}')
                    continue
                max_retries = 5
                attempts = 0
                while attempts < max_retries:
                    try:
                        res = requests.post(formatted_url)
                        break
                    except Exception as e:
                        print(f'error: {e}')
                        time.sleep(5)
                        attempts += 1
                        continue
                if res.status_code != 200:
                    print(res.text)
                    continue
                datas = res.json()
                print(datas)
                if datas.get('msg') != 'success':
                    print(datas)
                    continue
                user = datas.get('user')
                if not user:
                    print(datas)
                    continue
                if '@' not in user['userName']:
                    print(f'{url[-6:]}: {number}')
                    time.sleep(random.randint(300, 500))
                    continue
                newname = self.mask_email(user['userName'])
                body = {
                    "avatar": None,
                    "userName": newname,
                    "phone": user['phone'],
                }
                headers = {
                    'Content-Type': 'application/json',
                    'Token': datas['token']
                }
                if url.startswith('www'):
                    formatted_url = self.url_update_en.format(number, url[-6:])
                elif url.startswith('asia'):
                    formatted_url = self.url_update_zh.format(number, url[-6:])
                else:
                    print(f'error:{url}')
                    continue
                max_retries = 5
                attempts = 0
                while attempts < max_retries:
                    try:
                        res = requests.post(formatted_url, json=body, headers=headers)
                        break
                    except Exception as e:
                        print(f'error: {e}')
                        time.sleep(5)
                        attempts += 1
                        continue
                print(f'{url[-6:]}: {number}')
                time.sleep(random.randint(300, 500))

    def get_email(self):

        def _get_email_domain():
            return random.choices(self.domains, weights=self.probabilities, k=1)[0]
        
        def _get_firstname():
            return random.choice(list(firstnames.firstnames.keys())).lower()
        
        def _get_lastname():
            return random.choice(lastnames.lastnames).lower()
        
        def _get_tail():
            if random.choices([0, 1], weights=[0.2, 0.8], k=1)[0]:
                return ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(random.randint(1, 3)))
            return ''

        return f'{_get_firstname()}{_get_lastname()}{_get_tail()}{_get_email_domain()}'

    def get_number(self):
        return random.randint(100000000, 9999999999)
    
    def mask_email(self, email):
        local, domain = email.split('@')
        if len(local) > 7:
            masked_local = local[:3] + "****" + local[-4:]
        else:
            front_len = 1 if len(local) > 4 else len(local) // 2
            back_len = len(local) - front_len
            masked_local = local[:front_len] + "****" + local[-back_len:]
        return masked_local + '@' + domain
    
    def random_call(self):
        selected_method = random.choices([self.get_email, self.get_number], [0.6, 0.4], k=1)[0]
        return selected_method()
    
    def weighted_time_generation(region):
        weights = {
            'morning': 1,
            'afternoon': 2,
            'evening': 10,
            'night': 5
        }
        times = []
        base_time = datetime.now().replace(minute=0, second=0, microsecond=0)
        
        for hour in range(24):
            if 6 <= hour < 12:
                weight = weights['morning']
            elif 12 <= hour < 18:
                weight = weights['afternoon']
            elif 18 <= hour < 22:
                weight = weights['evening']
            else:
                weight = weights['night']
            
            times.extend([base_time.replace(hour=hour) for _ in range(weight)])
        
        return random.sample(times, 4)