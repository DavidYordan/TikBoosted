import time
from queue import Queue
from PyQt6.QtWidgets import (
    QDialog,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QTabWidget,
    QVBoxLayout,
    QWidget
)

from globals import Globals

class ConfigDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)

        self.user = 'DefineConfig'
        self.setModal(True)
        self.setWindowTitle('Config')
        # self.resize(960, 640)
        self.initUI()
        self.smmsky_config_reload()

        Globals._Log.info(self.user, 'Successfully initialized.')

    def initUI(self):
        main_layout = QVBoxLayout(self)
        
        tabs = QTabWidget()
        tab_smmsky_servers = QWidget()
        tabs.addTab(tab_smmsky_servers, "Smmsky Servers")
        
        smmsky_servers_layout = QVBoxLayout()
        main_layout.addLayout(smmsky_servers_layout)

        smmsky_servers_play_layout = QVBoxLayout()
        smmsky_servers_layout.addLayout(smmsky_servers_play_layout)
        smmsky_servers_play_layout.addWidget(QLabel('PlayCount'))
        self.smmsky_servers_play_level1 = QLineEdit()
        smmsky_servers_play_layout.addWidget(self.smmsky_servers_play_level1)
        self.smmsky_servers_play_level2 = QLineEdit()
        smmsky_servers_play_layout.addWidget(self.smmsky_servers_play_level2)
        self.smmsky_servers_play_level3 = QLineEdit()
        smmsky_servers_play_layout.addWidget(self.smmsky_servers_play_level3)

        smmsky_servers_digg_layout = QVBoxLayout()
        smmsky_servers_layout.addLayout(smmsky_servers_digg_layout)
        smmsky_servers_digg_layout.addWidget(QLabel('DiggCount'))
        self.smmsky_servers_digg_level1 = QLineEdit()
        smmsky_servers_digg_layout.addWidget(self.smmsky_servers_digg_level1)
        self.smmsky_servers_digg_level2 = QLineEdit()
        smmsky_servers_digg_layout.addWidget(self.smmsky_servers_digg_level2)
        self.smmsky_servers_digg_level3 = QLineEdit()
        smmsky_servers_digg_layout.addWidget(self.smmsky_servers_digg_level3)

        smmsky_servers_follower_layout = QVBoxLayout()
        smmsky_servers_layout.addLayout(smmsky_servers_follower_layout)
        smmsky_servers_follower_layout.addWidget(QLabel('FollowerCount'))
        self.smmsky_servers_follower_level1 = QLineEdit()
        smmsky_servers_follower_layout.addWidget(self.smmsky_servers_follower_level1)
        self.smmsky_servers_follower_level2 = QLineEdit()
        smmsky_servers_follower_layout.addWidget(self.smmsky_servers_follower_level2)
        self.smmsky_servers_follower_level3 = QLineEdit()
        smmsky_servers_follower_layout.addWidget(self.smmsky_servers_follower_level3)
        
        smmsky_servers_layout.addStretch()

        smmsky_servers_bottom_layout = QHBoxLayout()
        smmsky_servers_layout.addLayout(smmsky_servers_bottom_layout)
        smmsky_servers_reloadButton = QPushButton("Reload")
        smmsky_servers_reloadButton.clicked.connect(self.smmsky_config_reload)
        self.smmsky_servers_applyButton = QPushButton("Apply")
        smmsky_servers_bottom_layout.addWidget(self.smmsky_servers_applyButton)
        self.smmsky_servers_applyButton.clicked.connect(self.smmsky_config_apply)
        
    def smmsky_config_apply(self):
        columns = ['name', 'category', 'department', 'value', 'updatetime']
        now = int(time.time())
        values = [
            ('playCount1', 'servers', 'smmsky', self.smmsky_servers_play_level1.text(), now),
            ('playCount2', 'servers', 'smmsky', self.smmsky_servers_play_level2.text(), now),
            ('playCount3', 'servers', 'smmsky', self.smmsky_servers_play_level3.text(), now),
            ('diggCount1', 'servers', 'smmsky', self.smmsky_servers_digg_level1.text(), now),
            ('diggCount2', 'servers', 'smmsky', self.smmsky_servers_digg_level2.text(), now),
            ('diggCount3', 'servers', 'smmsky', self.smmsky_servers_digg_level3.text(), now),
            ('followerCount1', 'servers', 'smmsky', self.smmsky_servers_follower_level1.text(), now),
            ('followerCount2', 'servers', 'smmsky', self.smmsky_servers_follower_level2.text(), now),
            ('followerCount3', 'servers', 'smmsky', self.smmsky_servers_follower_level3.text(), now)
        ]

        datas = []
        for value in values:
            datas.append({})
            for idx, v in enumerate(value):
                datas[-1][columns[idx]] = v
        print(datas)

        q = Queue()
        Globals._WS.database_operation_signal.emit('bulk_upsert', {
            'table_name': 'define_config',
            'columns': columns,
            'data': datas,
            'unique_columns': ['name']
        }, q)

        res = q.get()
        print(res)
        
    def smmsky_config_reload(self):
        q = Queue()
        Globals._WS.database_operation_signal.emit('get_table_fields', {
            'table_name': 'define_config'
        }, q)
        columns = q.get()

        q = Queue()
        Globals._WS.database_operation_signal.emit('read', {
            'table_name': 'define_config',
            'condition': 'department = "smmsky"'
        }, q)
        datas = q.get()

        for data in datas:
            if data[columns.index('name')] == 'playCount1':
                self.smmsky_servers_play_level1.setText(data[columns.index('value')])
            elif data[columns.index('name')] == 'playCount2':
                self.smmsky_servers_play_level2.setText(data[columns.index('value')])
            elif data[columns.index('name')] == 'playCount3':
                self.smmsky_servers_play_level3.setText(data[columns.index('value')])
            elif data[columns.index('name')] == 'diggCount1':
                self.smmsky_servers_digg_level1.setText(data[columns.index('value')])
            elif data[columns.index('name')] == 'diggCount2':
                self.smmsky_servers_digg_level2.setText(data[columns.index('value')])
            elif data[columns.index('name')] == 'diggCount3':
                self.smmsky_servers_digg_level3.setText(data[columns.index('value')])
            elif data[columns.index('name')] == 'followerCount1':
                self.smmsky_servers_follower_level1.setText(data[columns.index('value')])
            elif data[columns.index('name')] == 'followerCount2':
                self.smmsky_servers_follower_level2.setText(data[columns.index('value')])
            elif data[columns.index('name')] == 'followerCount3':
                self.smmsky_servers_follower_level3.setText(data[columns.index('value')])