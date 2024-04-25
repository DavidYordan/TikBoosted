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
        self.url_zh = 'https://asia.reelshors.com/sqx_fast/app/Login/registerCode?password=&phone={}&msg=9999&inviterCode={}'
        self.url_en = 'https://www.reelshors.com/sqx_fast/app/Login/registerCode?password={}&phone={}&msg=9999&inviterCode={}'
        self.url_update_zh = 'https://asia.reelshors.com/sqx_fast/app/user/updateUsers'
        self.url_update_en = 'https://www.reelshors.com/sqx_fast/app/user/updateUsers'
        self.url_invitation = """
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=JCNKSL
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=JNHKSL
asia.reelshors.com/pages/login/login?inviterType=0&invitation=VZKSLF
https://asia.reelshors.com/pages/login/login?inviterType=0&invitation=DFJKSL
www.reelshors.com/pages/login/login?inviterType=0&invitation=JRNKSL
https://asia.reelshors.com/pages/login/login?inviterType=0&invitation=U2XKSL
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=JKCKSL
asia.reelshors.com/pages/login/login?inviterType=0&invitation=KNKSLF
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=JP6KSL
asia.reelshors.com/pages/login/login?inviterType=0&invitation=3GKSLF
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=UQFKSL
https://www.reelshors.com/pages/login/login?invitation=67&inviterType=1&inviterUrl=%2Fme%2Fdetail%2Fdetail%3Fid%3D67%26courseDetailsId%3D6287
www.reelshors.com/pages/login/register?invitation=UVKSLF
https://www.reelshors.com/pages/login/login?invitation=80&inviterType=1&inviterUrl=%2Fme%2Fdetail%2Fdetail%3Fid%3D183%26courseDetailsId%3D16748
www.reelshors.com/pages/login/login?inviterType=0&invitation=UYKSLF
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=J6BKSL
https://asia.reelshors.com/pages/login/login?inviterType=0&invitation=DY4KSL
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=3HKSLF
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=JM6KSL
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=JJDKSL
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=J54KSL
asia.reelshors.com/pages/login/login?inviterType=0&invitation=WTKSLF
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=JKMKSL
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=UQRKSL
https://asia.reelshors.com/pages/login/login?inviterType=0&invitation=UVFKSL
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=JCVKSL
www.reelshors.com/pages/login/login?inviterType=0&invitation=JCBKSL
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=D8KSLF
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=U36KSL
https://asia.reelshors.com/pages/login/login?inviterType=0&invitation=J4XKSL
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=JKFKSL
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=JZ3KSL
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=42KSLF
www.reelshors.com/pages/login/login?inviterType=0&invitation=UKKSLF
https://asia.reelshors.com/pages/login/login?inviterType=0&invitation=JTXKSL
www.reelshors.com/pages/login/login?inviterType=0&invitation=JMRKSL
www.reelshors.com/pages/login/login?inviterType=0&invitation=JW4KSL
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=J7CKSL
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=JX7KSL
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=JEBKSL
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=J7BKSL
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=J78KSL
https://asia.reelshors.com/pages/login/login?inviterType=0&invitation=V9KSLF
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=DF7KSL
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=JPNKSL
www.reelshors.com/pages/login/login?inviterType=0&invitation=JRVKSL
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=U84KSL
https://asia.reelshors.com/pages/login/login?inviterType=0&invitation=J4GKSL
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=U63KSL
https://asia.reelshors.com/pages/login/login?inviterType=0&invitation=UG2KSL
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=JKKKSL
www.reelshors.com/pages/login/login?inviterType=0&invitation=JDBKSL
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=J9GKSL
https://asia.reelshors.com/pages/login/login?inviterType=0&invitation=DV9KSL
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=U7XKSL
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=JEKKSL
asia.reelshors.com/pages/login/login?inviterType=0&invitation=DFEKSL
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=J7EKSL
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=JE4KSL
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=U94KSL
https://asia.reelshors.com/pages/login/login?inviterType=0&invitation=JQFKSL
https://asia.reelshors.com/pages/login/login?inviterType=0&invitation=JQMKSL
https://asia.reelshors.com/pages/login/login?inviterType=0&invitation=JFCKSL
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=DEKKSL
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=ZKDKSL
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=DV7KSL
https://asia.reelshors.com/pages/login/login?inviterType=0&invitation=DQEKSL
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=U4KSLF
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=DG7KSL
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=ZC2KSL
www.reelshors.com/pages/login/login?inviterType=0&invitation=JNKKSL
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=D6MKSL
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=UJXKSL
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=U48KSL
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=UWMKSL
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=U64KSL
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=U6KKSL
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=U6SKSL
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=UHRKSL
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=U3GKSL
www.reelshors.com/pages/login/login?inviterType=0&invitation=J4VKSL
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=UJMKSL
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=J76KSL
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=UY9KSL
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=UXMKSL
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=JPBKSL
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=UZVKSL
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=J7SKSL
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=ZK7KSL
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=JK6KSL
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=ZC7KSL
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=ZPPKSL
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=J4BKSL
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=UK4KSL
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=ZK3KSL
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=DGZKSL
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=UH7KSL
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=ZK9KSL
https://www.reelshors.com/pages/login/login?inviterType=0&invitation=JW3KSL
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
                password = f'{invitation_code}{number}'
                if 'www.reelshors.com' in url:
                    formatted_url = self.url_en.format(password, number, invitation_code)
                elif 'asia.reelshors.com' in url:
                    continue
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
                    time.sleep(random.randint(100, 300))
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
                if 'www.reelshors.com' in url:
                    formatted_url = self.url_update_en
                elif 'asia.reelshors.com' in url:
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
                time.sleep(random.randint(100, 300))

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
        head = random.choice([
            '0939', '0958', '0980', '0916', '0930', '0988', '0987', '0975', '0926', '0920', '0972', '0911', '0917', '0936', '0989', '0931', '0937', '0981', '0983'
        ])
        number = random.randint(10000, 99999)
        return f'{head}{number}'
    
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
        selected_method = random.choices([self.get_email, self.get_number], [0.5, 0.5], k=1)[0]
        return selected_method()
    
if __name__=='__main__':
    CA = CreateAccounts()
    CA.create_account()