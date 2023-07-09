import psycopg2
import pandas as pd
from pathlib import Path 


class manager_DB(object):
    """класс для управленеия DB postgres
    загрузку данных"""

    def __init__(self, ConfigSystem: dict, ConfigTable: dict):
        super().__init__(ConfigSystem)
        ConfigDB = ConfigSystem['PostgresParam']
        self.dbname_sql = ConfigDB.get('DBNAME')
        self.user_sql = ConfigDB.get('USER')
        self.password_sql = ConfigDB.get('PASSWORD')
        self.host_sql = ConfigDB.get('HOST')
        self.port_sql = ConfigDB.get('PORT')
        self.SchemaDB = ConfigDB.get('SCHEMA')

        # устанавливаем соединение с базой
        self.conn = psycopg2.connect(dbname=self.dbname_sql, 
        user=self.user_sql, 
        password=self.password_sql, 
        host=self.host_sql, 
        port=self.port_sql)

        self.cursor = self.conn.cursor()

        # считываем файл для загрузки контента
        self.ConfigTable = ConfigTable
        self.GroupIdForDB = ConfigSystem['VkApiParam'].get('GROUP_ID')

    def UploadRowDB(self, ConfigTable: dict, LoadConnent: list, dt):

        ColumnsName = ConfigTable.get('ColumnsName')
        TableName = ConfigTable.get('TableName')

        w = []
        for i in range(len(LoadConnent)):
            w.append(str(tuple(LoadConnent[i])))


        sql_query = f"""
        INSERT INTO {self.SchemaDB}.{TableName} ({", ".join([i for i in ColumnsName])})
        VALUES {", ".join([i for i in w])}
        """

        sql_query = sql_query.replace("'", "")

        if TableName == 'history_post' and LoadConnent[0][2] != 'NULL':
            sql_query = sql_query.replace(LoadConnent[0][2], f"'{LoadConnent[0][2]}'")

        sql_query = sql_query.replace("TIMESTAMP WITH TIME ZONE",f"TIMESTAMP WITH TIME ZONE '{dt}'")
       
        self.cursor.execute(sql_query)
        self.conn.commit()
    
        return
    
    def CoonClose(self):
        return self.conn.close() 