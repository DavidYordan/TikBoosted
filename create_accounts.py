import requests
import random
import os
import re
import string
import time
from PyQt6.QtCore import QObject

import firstnames
import lastnames

class CreateAccounts(QObject):
    def __init__(self, parent = None):
        super().__init__(parent)

        self.user = 'CreateAccounts'
        self.email_domains = {
            '@gmail.com': 0.6,
            '@outlook.com': 0.2,
            '@yahoo.com': 0.2,
            '@hotmail.com': 0.05,
            '@aol.com': 0.05
        }
        self.domains, self.probabilities = zip(*self.email_domains.items())
        self.accounts_path = os.path.join('files', 'accounts.xls')
        if not os.path.exists('files'):
            os.makedirs('files')
        self.url_zh = 'https://asia.reelshors.com/sqx_fast/app/Login/registerCode?password=d135246&phone={}&msg=9999&inviterCode={}'
        self.url_en = 'https://www.reelshors.com/sqx_fast/app/Login/registerCode?password=d135246&phone={}&msg=9999&inviterCode={}'
        self.url_update_zh = 'https://asia.reelshors.com/sqx_fast/app/user/updateUsers'
        self.url_update_en = 'https://www.reelshors.com/sqx_fast/app/user/updateUsers'
        self.url_invitation = """
asia.reelshors.com/pages/login/login?inviterType=0&invitation=VZKSLF
"""

    def create_account(self):
        urls = self.url_invitation.strip().split('\n')
        print(len(urls))
        while True:
            for url in urls:
                url = url.replace('https://', '')
                match = re.search(r'\binvitation=([^&]+)', url)
                invitation_code = match.group(1)
                number = self.random_call()
                if 'www.reelshors.com' in url:
                    formatted_url = self.url_en.format(number, invitation_code)
                elif 'asia.reelshors.com' in url:
                    formatted_url = self.url_zh.format(number, invitation_code)
                else:
                    print(f'error:{url}')
                    continue
                tail = url.find('inviterType')
                if tail != -1:
                    formatted_url = f'{formatted_url}&{url[tail:]}'
                else:
                    formatted_url = f'{formatted_url}&inviterType=0&inviterUrl=&platform=h5'
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
                    time.sleep(random.randint(3600, 21600))
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
                    formatted_url = self.url_update_en
                elif url.startswith('asia'):
                    formatted_url = self.url_update_zh
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
                print(f'{invitation_code}: {number}')
                time.sleep(random.randint(3600, 21600))

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
    
if __name__=='__main__':
    CA = CreateAccounts()
    CA.create_account()