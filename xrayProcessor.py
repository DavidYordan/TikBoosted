import base64
import json
import os
import re
import requests
import subprocess
from datetime import datetime, timedelta
from PyQt6.QtCore import QRunnable
from urllib.parse import unquote

from globals import Globals

class XrayProcessor(QRunnable):
    def __init__(self):
        super().__init__()

        self.conf = {
            'log': {
                'access': f'access_{datetime.now().strftime("%Y-%m-%d")}.log',
                'error': f'error_{datetime.now().strftime("%Y-%m-%d")}.log',
                'loglevel': 'debug'
            },
            'routing': {
                'domainStrategy': 'AsIs',
                'rules': []
            },
            'inbounds': [],
            'outbounds': []
        }
        self.conf_parse = {}
        self.isInit = False
        self.is_running = False
        self.port = 40001
        self.user = 'XrayProcessor'

        Globals._Log.info(self.user, 'Successfully initialized.')

    def base64_decode(self, source):
        try:
            text = source.replace('_', '/').replace('-', '+')
            padding = -len(text) % 4
            text += '=' * padding
            return base64.urlsafe_b64decode(text).decode()
        except:
            return source
        
    def delete_log(self):
        try:
            xray_dir = os.path.join(os.getcwd(), 'xray')
            cutoff_date = datetime.now() - timedelta(days=7)
            for filename in os.listdir(xray_dir):
                match = re.match(r"(access|error)_(\d{4}-\d{2}-\d{2})\.log", filename)
                if match:
                    file_date_str = match.group(2)
                    file_date = datetime.strptime(file_date_str, '%Y-%m-%d')
                    if file_date < cutoff_date:
                        file_path = os.path.join(xray_dir, filename)
                        os.remove(file_path)
                        Globals._Log.info(self.user, f'Deleted old log file: {filename}')

        except Exception as e:
            Globals._Log.error(self.user, f'Failed to delete log: {e}')

    def kill_process_on_ports(self):
        try:
            ports = list(Globals.xray_dict.keys())
            for port in ports:
                command = f'netstat -ano | findstr :{port}'
                proc = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
                output, errors = proc.communicate()
                if output:
                    for line in output.decode('utf-8').split('\n'):
                        if line.strip():
                            parts = line.strip().split()
                            pid = parts[-1]
                            subprocess.run(['taskkill', '/PID', pid, '/F'], shell=True)
                            Globals._Log.info(self.user, f'Killed process with PID {pid} on port {port}')
        except Exception as e:
            Globals._Log.error(self.user, f'Failed to kill processes on ports: {e}')


        
    def parse_shadowsocks(self, rest):
        try:
            match = re.match(r'(?P<params>.+)@(?P<server>[^:]+):(?P<port>\d+)(?:/\?(?P<extra_params>[^#]+))?(?:#(?P<tag>.+))?', rest)
            if not match:
                Globals._Log.error(self.user, f'Link format invalid, failed to parse: {rest}')
                return
            encryption_password = self.base64_decode(match.group('params'))
            if encryption_password.count(':') != 1:
                Globals._Log.error(self.user, f'Invalid encryption-password format in link: {rest}')
                return
            encryption, password = encryption_password.split(':')
            address = match.group('server')
            if address == '9.9.9.9':
                return
            port = match.group('port')
            tag = unquote(match.group('tag')).strip() if match.group('tag') else f'{self.port}out'
            data = {
                'protocol': 'shadowsocks',
                'address': address,
                'port': int(port),
                'tag': tag,
                'settings': {
                    'servers': [{
                        'address': address,
                        'port': int(port),
                        'method': encryption,
                        'password': password,
                        'uot': True,
                        'UoTVersion': 2
                    }]
                }
            }
            if match.group('extra_params'):
                Globals._Log.warning(self.user, f'Additional parameters detected in the link: {match.group("extra_params")}')
            self.conf_parse['inbounds'].append({
                    'tag': f'{self.port}in',
                    'listen': '0.0.0.0',
                    'port': self.port,
                    'protocol': 'http',
                    'settings': {
                        'allowTransparent': False
                    }
            })
            self.conf_parse['outbounds'].append(data)
            self.conf_parse['routing']['rules'].append({
                'type': 'field',
                'inboundTag': [f'{self.port}in'],
                'outboundTag': tag
            })
            Globals._MUTEX_XRAY.lock()
            Globals.xray_dict[self.port] = {'anti_crawl_count': 0, 'response_times': LimitedList(10)}
            Globals._MUTEX_XRAY.unlock()
            self.port += 1

        except Exception as e:
            Globals._Log.error(self.user, f'Error parsing {rest}: {e}')

    def parse_subscribe(self, source):
        try:
            res = requests.get(source)
            if res.status_code != 200:
                return
            urls = res.text
            if not urls:
                return
            if '://' not in urls:
                urls = self.base64_decode(urls)
            if '://' not in urls:
                return
            self.conf_parse = self.conf.copy()
            self.port = 40001
            url_list = urls.split('\n')
            for url in url_list:
                if not url:
                    continue
                if '://' not in url:
                    url = self.base64_decode(url)
                if 'ss://' not in url:
                    Globals._Log.warning(self.user, f'Unsupported link: {url}')
                    continue
                Globals._Log.debug(self.user, f'Parse link: {url}')
                self.parse_shadowsocks(url[5:])

            Globals._Log.debug(self.user, f'{Globals.xray_dict.keys()}')

        except Exception as e:
            Globals._Log.info(self.user, f'Failed to parse subscription: {source}, Error: {e}')

    def run(self):
        try:
            if not self.isInit:
                self.subscribe()

            self.kill_process_on_ports()

            self.process = subprocess.Popen(['xray/xray.exe', 'run'], cwd='./xray')
            self.process.wait()

        except Exception as e:
            print(f'Failed to start Xray: {e}')

    def start_task(self):
        if not self.is_running:
            Globals.thread_pool.start(self)

    def stop_task(self):
        self.is_running = False
        if self.process:
            self.process.terminate()
            self.process.wait()

    def subscribe(self):
        with open('xray/subscribe.txt', 'r') as f:
            urls = f.read().split('\n')

        for url in urls:
            self.parse_subscribe(url)

        with open('xray/config.json', 'w') as f:
            json.dump(self.conf_parse, f, indent=4)

        self.isInit = True

class LimitedList(list):
    def __init__(self, limit):
        super().__init__()

        self.limit = limit

    def append(self, object):
        super().append(object)

        while len(self) > self.limit:
            del self[0]