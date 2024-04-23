import sqlite3
from threading import Lock
from PyQt6.QtCore import (
    pyqtSlot,
    QObject
)

from globals import Globals

class OperateSqlite(QObject):
    _lock = Lock()

    def __init__(self, db_path):
        super().__init__()
        self.db_path = db_path
        self.connection = None
        self.cursor = None
        Globals._WS.database_operation_signal.connect(self._perform_database_operation)
        self.user = 'OperateSqlite'

        self._connect()
        self._setup_database()

        Globals._Log.info(self.user, 'Successfully initialized.')

    def _connect(self):
        if not self.connection or not self.cursor:
            try:
                self.connection = sqlite3.connect(self.db_path)
                self.cursor = self.connection.cursor()
            except sqlite3.Error as error:
                self.connection = None
                self.cursor = None
                Globals._Log.error(self.user, f'Error occurred during database connection: {error}')

    @pyqtSlot(str, dict, object)
    def _perform_database_operation(self, operation_type, kwargs, queue=None):
        with self._lock:
            table_name = kwargs.get('table_name')
            query = kwargs.get('query')
            columns = kwargs.get('columns')
            values = kwargs.get('values')
            condition = kwargs.get('condition')
            params = kwargs.get('params', [])
            unique_columns = kwargs.get('unique_columns')
            data = kwargs.get('data')
            many = kwargs.get('many', False)

            try:
                if operation_type == 'execute_query':
                    result = self.execute_query(query, params, many)
                elif operation_type == 'bulk_insert':
                    result = self.bulk_insert(table_name, columns, data)
                elif operation_type == 'bulk_upsert':
                    result = self.bulk_upsert(table_name, columns, data, unique_columns)
                elif operation_type == 'clear_bulk_insert':
                    result = self.clear_bulk_insert(table_name, columns, data)
                elif operation_type == 'clear_table':
                    result = self.clear_table(table_name)
                elif operation_type == 'create':
                    result = self.create(table_name, columns, values)
                elif operation_type == 'delete':
                    result = self.delete(table_name, condition, params)
                elif operation_type == 'get_table_fields':
                    result = self.get_table_fields(table_name)
                elif operation_type == 'insert':
                    result = self.insert(table_name, columns, data)
                elif operation_type == 'read':
                    result = self.read(table_name, columns or "*", condition, params)
                elif operation_type == 'update':
                    result = self.update(table_name, kwargs.get('updates'), condition)
                elif operation_type == 'upsert':
                    result = self.upsert(table_name, columns, values, unique_columns)
                else:
                    Globals._Log.error(self.user, f'Invalid operation type: {operation_type}')
                    result = None
            except Exception as error:
                Globals._Log.error(self.user, f'Error occurred during database operation {operation_type}: {error}')
                result = None
            finally:
                if queue:
                    queue.put(result)

    def _setup_database(self):
        self._connect()

        for table_name, columns_info in DBSchema.tables.items():
            columns_definitions = ', '.join(f"{column} {data_type}" for column, data_type in columns_info.items())

            self.cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';")
            if not self.cursor.fetchone():
                self.cursor.execute(f"CREATE TABLE {table_name} ({columns_definitions});")
                self.connection.commit()
                Globals._Log.info(self.user, f'Table {table_name} created successfully.')
            else:
                self.cursor.execute(f"PRAGMA table_info({table_name});")
                existing_columns = set(column[1] for column in self.cursor.fetchall())

                for column_name, column_type in columns_info.items():
                    if column_name not in existing_columns and "PRIMARY KEY" not in column_type:
                        try:
                            self.cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type};")
                            self.connection.commit()
                            Globals._Log.info(self.user, f'Added column {column_name} to table {table_name}.')
                        except sqlite3.Error as error:
                            Globals._Log.error(self.user, f'Error occurred while adding column {column_name} to {table_name}: {error}')
                            self.connection.rollback()

    def bulk_insert(self, table_name, columns, data):
        columns_str = ", ".join(columns)
        placeholders = ", ".join("?" for _ in columns)
        query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
        values = [tuple(item[col] for col in columns) for item in data]
        return self.execute_query(query, values, many=True)
    
    def bulk_upsert(self, table_name, columns, data, unique_columns):
        columns_str = ', '.join(columns)
        placeholders = ', '.join(['?' for _ in columns])
        updates = ', '.join([f"{col}=excluded.{col}" for col in columns if col not in unique_columns])

        query = f'''
        INSERT INTO {table_name} ({columns_str})
        VALUES ({placeholders})
        ON CONFLICT({', '.join(unique_columns)})
        DO UPDATE SET {updates}
        '''

        values = [tuple(item[col] for col in columns) for item in data]
        return self.execute_query(query, values, many=True)
        
    def clear_bulk_insert(self, table_name, columns, data):
        self.clear_table(table_name)
        self.bulk_insert(table_name, columns, data)

    def clear_table(self, table_name):
        self._connect()
        if not self.connection or not self.cursor:
            return None
        try:
            self.cursor.execute(f"DELETE FROM {table_name};")
            self.connection.commit()
            Globals._Log.info(self.user, f'All data deleted from table {table_name}.')
            return self.cursor.rowcount
        except sqlite3.Error as error:
            self.connection.rollback()
            Globals._Log.error(self.user, f'Error occurred while deleting data from table {table_name}: {error}')
            return None

    def create(self, table_name, columns, values):
        columns_str = ", ".join(columns)
        placeholders = ", ".join(["?" for _ in values])
        query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
        return self.execute_query(query, values)

    def delete(self, table_name, condition, params=None):
        query = f"DELETE FROM {table_name} WHERE {condition}"
        return self.execute_query(query, params)
    
    def get_table_fields(self, table_name):
        self._connect()
        if not self.connection or not self.cursor:
            return []

        query = f"PRAGMA table_info({table_name});"
        try:
            self.cursor.execute(query)
            fields = [row[1] for row in self.cursor.fetchall()]
            return fields
        except sqlite3.Error as error:
            Globals._Log.error(self.user, f'Error occurred during fetching table fields: {error}')
            return []

    def execute_query(self, query, params=None, many=False):
        self._connect()
        if not self.connection or not self.cursor:
            return None
        try:
            if many:
                self.cursor.executemany(query, params)
            else:
                self.cursor.execute(query, params or [])
            if query.strip().upper().startswith('SELECT'):
                return self.cursor.fetchall()
            self.connection.commit()
            return self.cursor.rowcount
        except sqlite3.Error as error:
            self.connection.rollback()
            Globals._Log.error(self.user, f'Error occurred during query execution: {error}')
            return None

    def insert(self, table_name, columns, data):
        columns_str = ", ".join(columns)
        placeholders = ", ".join("?" for _ in columns)
        query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
        values = [data[col] for col in columns]
        return self.execute_query(query, values)

    def read(self, table_name, columns="*", condition=None, params=None):
        columns_str = ", ".join(columns) if columns else '*'
        query = f"SELECT {columns_str} FROM {table_name}"
        if condition:
            query += f" WHERE {condition}"
        return self.execute_query(query, params)

    def update(self, table_name, updates, condition=None):
        set_clause = ", ".join([f"{key} = ?" for key in updates])
        params = list(updates.values())
        query = f"UPDATE {table_name} SET {set_clause}"
        if condition:
            query += f" WHERE {condition}"
        return self.execute_query(query, params)

    def upsert(self, table_name, columns, values, unique_columns):
        columns_str = ", ".join(columns)
        placeholders = ", ".join("?" for _ in values)
        on_conflict_set = ", ".join([f"{col}=excluded.{col}" for col in columns if col not in unique_columns])

        query = f"""
        INSERT INTO {table_name} ({columns_str})
        VALUES ({placeholders})
        ON CONFLICT({", ".join(unique_columns)})
        DO UPDATE SET {on_conflict_set}
        """
        return self.execute_query(query, values)

class DBSchema(object):
    tables = {
        "accounts": {
            "id": "INTEGER PRIMARY KEY",
            "account": "TEXT",
            "position": "TEXT",
            "status": "TEXT",
            "team": "TEXT",
            "customer": "TEXT",
            "rsAccount": "TEXT",
            "tkAccount": "TEXT",
            "referralLink": "TEXT",
            "region": "TEXT",
            "referralCode": "TEXT",
            "clientRemark": "TEXT",
            "serverRemark": "TEXT",
            "todayEarnings": "TEXT",
            "earningDays": "INTEGER",
            "totalEarnings": "TEXT",
            "latestEarningDay": "TEXT",
            "userId": "INTEGER",
            "uniqueId": "TEXT",
            "nickname": "TEXT",
            "logid": "TEXT",
            "diggCount": "INTEGER",
            "followerCount": "INTEGER",
            "followingCount": "INTEGER",
            "friendCount": "INTEGER",
            "heart": "INTEGER",
            "heartCount": "INTEGER",
            "videoCount": "INTEGER",
            "link": "TEXT",
            "risk": "INTEGER",
            "signature": "TEXT",
            "secUid": "TEXT",
            "ttSeller": "BOOLEAN",
            "verified": "BOOLEAN",
            "updateTime": "INTEGER",
            "createTime": "INTEGER"
        },
        "agents_america": {
            "userId": "INTEGER PRIMARY KEY",
            "userName": "TEXT",
            "phone": "TEXT",
            "status": "TEXT",
            "salt": "TEXT",
            "roleIdList": "TEXT",
            "password": "TEXT",
            "mobile": "TEXT",
            "isAgent": "INTEGER",
            "email": "TEXT",
            "createUserId": "INTEGER",
            "createTime": "TEXT",
            "ausername": "TEXT",
            "appUserId": "INTEGER",
            "amobile": "TEXT",
            "agent0Money": "TEXT",
            "agentBankAccount": "TEXT",
            "agentBankAddress": "TEXT",
            "agentBankCode": "TEXT",
            "agentBankName": "TEXT",
            "agentBankUser": "TEXT",
            "agentCash": "TEXT",
            "agentId": "INTEGER",
            "agentRate": "TEXT",
            "agentType": "INTEGER",
            "agentWithdrawCash": "TEXT"
        },
        "agents_asia": {
            "userId": "INTEGER PRIMARY KEY",
            "userName": "TEXT",
            "phone": "TEXT",
            "status": "TEXT",
            "salt": "TEXT",
            "roleIdList": "TEXT",
            "password": "TEXT",
            "mobile": "TEXT",
            "isAgent": "INTEGER",
            "email": "TEXT",
            "createUserId": "INTEGER",
            "createTime": "TEXT",
            "ausername": "TEXT",
            "appUserId": "INTEGER",
            "amobile": "TEXT",
            "agent0Money": "TEXT",
            "agentBankAccount": "TEXT",
            "agentBankAddress": "TEXT",
            "agentBankCode": "TEXT",
            "agentBankName": "TEXT",
            "agentBankUser": "TEXT",
            "agentCash": "TEXT",
            "agentId": "INTEGER",
            "agentRate": "TEXT",
            "agentType": "INTEGER",
            "agentWithdrawCash": "TEXT"
        },
        "define_config": {
            "name": "TEXT PRIMARY KEY",
            "category": "TEXT",
            "department": "TEXT",
            "value": "TEXT",
            "updatetime": "INTEGER"
        },
        "products_america": {
            "courseId": "INTEGER PRIMARY KEY",
            "weekGoodNum": "INTEGER",
            "isRecommend": "INTEGER",
            "titleImg": "TEXT",
            "courseType": "INTEGER",
            "img": "TEXT",
            "msgType": "TEXT",
            "bannerId": "TEXT",
            "classifyId": "TEXT",
            "title": "TEXT",
            "payMoney": "TEXT",
            "payNum": "INTEGER",
            "price": "REAL",
            "classificationName": "TEXT",
            "details": "TEXT",
            "bannerImg": "TEXT",
            "goodNum": "INTEGER",
            "over": "TEXT",
            "courseLabel": "TEXT",
            "languageType": "TEXT",
            "isDelete": "INTEGER",
            "viewCounts": "INTEGER",
            "videoType": "INTEGER",
            "bannerName": "TEXT",
            "msgUrl": "TEXT",
            "updateTime": "TEXT",
            "courseDetailsId": "INTEGER",
            "courseCount": "INTEGER",
            "createTime": "TEXT",
            "isPrice": "INTEGER",
            "courseDetailsName": "TEXT",
            "status": "INTEGER"
        },
        "products_asia": {
            "courseId": "INTEGER PRIMARY KEY",
            "weekGoodNum": "INTEGER",
            "isRecommend": "INTEGER",
            "titleImg": "TEXT",
            "courseType": "INTEGER",
            "img": "TEXT",
            "msgType": "TEXT",
            "bannerId": "TEXT",
            "classifyId": "TEXT",
            "title": "TEXT",
            "payMoney": "TEXT",
            "payNum": "INTEGER",
            "price": "REAL",
            "classificationName": "TEXT",
            "details": "TEXT",
            "bannerImg": "TEXT",
            "goodNum": "INTEGER",
            "over": "TEXT",
            "courseLabel": "TEXT",
            "languageType": "TEXT",
            "isDelete": "INTEGER",
            "viewCounts": "INTEGER",
            "videoType": "INTEGER",
            "bannerName": "TEXT",
            "msgUrl": "TEXT",
            "updateTime": "TEXT",
            "courseDetailsId": "INTEGER",
            "courseCount": "INTEGER",
            "createTime": "TEXT",
            "isPrice": "INTEGER",
            "courseDetailsName": "TEXT",
            "status": "INTEGER"
        },
        "orderIssuer_orders": {
            "client": "TEXT",
            "platform": "TEXT NOT NULL",
            "orderId": "INTEGER NOT NULL",
            "uniqueId": "TEXT",
            "videoId": "INTEGER",
            "link": "TEXT",
            "service": "INTEGER",
            "charge": "TEXT",
            "quantity": "INTEGER",
            "start_count": "INTEGER",
            "remains": "INTEGER",
            "status": "TEXT",
            "duration": "INTEGER",
            "rate": "TEXT",
            "currency": "TEXT",
            "cancel": "BOOLEAN",
            "createTime": "INTEGER",
            "updateTime": "INTEGER"
        },
        "orderIssuer_services": {
            "platform": "TEXT",
            "service": "INTEGER",
            "name": "TEXT",
            "type": "TEXT",
            "rate": "TEXT",
            "min": "INTEGER",
            "max": "INTEGER",
            "dripfeed": "BOOLEAN",
            "refill": "BOOLEAN",
            "cancel": "BOOLEAN",
            "category": "TEXT"
        },
        "tokens": {
            "name": "TEXT PRIMARY KEY",
            "token": "TEXT",
            "expire": "INTEGER"
        },
        "users_america": {
            "userId": "INTEGER PRIMARY KEY",
            "userName": "TEXT",
            "phone": "TEXT",
            "avatar": "TEXT",
            "sex": "TEXT",
            "openId": "TEXT",
            "googleId": "TEXT",
            "wxId": "TEXT",
            "wxOpenId": "TEXT",
            "password": "TEXT",
            "createTime": "TEXT",
            "updateTime": "TEXT",
            "appleId": "TEXT",
            "sysPhone": "TEXT",
            "status": "INTEGER",
            "platform": "TEXT",
            "jifen": "TEXT",
            "invitationCode": "TEXT",
            "inviterCode": "TEXT",
            "bankCode": "TEXT",
            "clientid": "TEXT",
            "zhiFuBao": "TEXT",
            "recipient": "TEXT",
            "bankNumber": "TEXT",
            "bankName": "TEXT",
            "bankAddress": "TEXT",
            "zhiFuBaoName": "TEXT",
            "rate": "INTEGER",
            "twoRate": "INTEGER",
            "onLineTime": "TEXT",
            "invitationType": "INTEGER",
            "inviterType": "INTEGER",
            "inviterUrl": "TEXT",
            "inviterCustomId": "INTEGER",
            "agent0Money": "INTEGER",
            "agent1Money": "INTEGER",
            "agent0MoneyDelete": "INTEGER",
            "agent1MoneyDelete": "INTEGER",
            "member": "TEXT",
            "email": "TEXT",
            "firstName": "TEXT",
            "lastName": "TEXT",
            "counts": "TEXT",
            "money": "TEXT",
            "endTime": "TEXT",
            "ausername": "TEXT",
            "amobile": "TEXT",
            "asusername": "TEXT",
            "asmobile": "TEXT",
            "cusername": "TEXT",
            "cmobile": "TEXT"
        },
        "users_asia": {
            "userId": "INTEGER PRIMARY KEY",
            "userName": "TEXT",
            "phone": "TEXT",
            "avatar": "TEXT",
            "sex": "TEXT",
            "openId": "TEXT",
            "googleId": "TEXT",
            "wxId": "TEXT",
            "wxOpenId": "TEXT",
            "password": "TEXT",
            "createTime": "TEXT",
            "updateTime": "TEXT",
            "appleId": "TEXT",
            "sysPhone": "TEXT",
            "status": "INTEGER",
            "platform": "TEXT",
            "jifen": "TEXT",
            "invitationCode": "TEXT",
            "inviterCode": "TEXT",
            "bankCode": "TEXT",
            "clientid": "TEXT",
            "zhiFuBao": "TEXT",
            "recipient": "TEXT",
            "bankNumber": "TEXT",
            "bankName": "TEXT",
            "bankAddress": "TEXT",
            "zhiFuBaoName": "TEXT",
            "rate": "INTEGER",
            "twoRate": "INTEGER",
            "onLineTime": "TEXT",
            "invitationType": "INTEGER",
            "inviterType": "INTEGER",
            "inviterUrl": "TEXT",
            "inviterCustomId": "INTEGER",
            "agent0Money": "INTEGER",
            "agent1Money": "INTEGER",
            "agent0MoneyDelete": "INTEGER",
            "agent1MoneyDelete": "INTEGER",
            "member": "TEXT",
            "email": "TEXT",
            "firstName": "TEXT",
            "lastName": "TEXT",
            "counts": "TEXT",
            "money": "TEXT",
            "endTime": "TEXT",
            "ausername": "TEXT",
            "amobile": "TEXT",
            "asusername": "TEXT",
            "asmobile": "TEXT",
            "cusername": "TEXT",
            "cmobile": "TEXT"
        },
        "users_america_password": {
            "userId": "INTEGER PRIMARY KEY",
            "password": "TEXT",
            "realPasswork": "TEXT"
        },
        "users_asia_password": {
            "userId": "INTEGER PRIMARY KEY",
            "password": "TEXT",
            "realPasswork": "TEXT"
        },
        "videos": {
            "videoId": "INTEGER PRIMARY KEY",
            "account": "TEXT",
            "title": "TEXT",
            "collectCount": "INTEGER",
            "commentCount": "INTEGER",
            "diggCount": "INTEGER",
            "playCount": "INTEGER",
            "shareCount": "INTEGER",
            "createTime": "INTEGER",
            "updateTime": "INTEGER"
        },
        "video_targets": {
            "id": "INTEGER PRIMARY KEY",
            "uniqueId": "TEXT",
            "videoId": "INTEGER",
            "followerInit": "INTEGER",
            "followerCurrent": "INTEGER",
            "followerTarget": "INTEGER",
            "diggInit": "INTEGER",
            "diggCurrent": "INTEGER",
            "diggTarget": "INTEGER",
            "playInit": "INTEGER",
            "playCurrent": "INTEGER",
            "playTarget": "INTEGER",
            "finished": "BOOLEAN",
            "createTime": "INTEGER",
            "updateTime": "INTEGER"
        }
    }