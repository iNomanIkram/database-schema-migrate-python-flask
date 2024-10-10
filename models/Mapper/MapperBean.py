import psycopg2

from modules.DynamicPrinter import dynamicPrinter
from constants.CONSTANT import CONSTANT
import traceback

class MapperBean():
    global con
    global queries

    def __init__(self):

        username = CONSTANT.QUERY_DATABASE_CRED.USERNAME
        password = CONSTANT.QUERY_DATABASE_CRED.PASSWORD
        host = CONSTANT.QUERY_DATABASE_CRED.HOST
        database = CONSTANT.QUERY_DATABASE_CRED.DATABASE
        port = CONSTANT.QUERY_DATABASE_CRED.PORT

        self.connection = self.connectToPostgres(username, password, host, database, port)

    def getListOfDatabases(self):
        arr = []
        cursor  = self.connection.cursor()
        try:
            cursor.execute('SELECT * FROM database_details')
            output = cursor.fetchall()

            for list in output:
                arr.append(list[0])
            return arr
        except:
            return []

    def connectToPostgres(self, user, password, host, database, port):

        try:
            global con
            try:
                con = psycopg2.connect(
                    host=host,
                    database=database,
                    user=user,
                    password=password,
                    port=port
                )
            except:
                con = psycopg2.connect(
                    host=host,
                    database=database,
                    user=user,
                    password=password
                )

            # dynamicPrinter('Database: Postgres \nStatus: Connected')
            return con
        except Exception as e:
            dynamicPrinter(f'Exception: {e}')
            return

    def getDatatypePriority(self,database_id,type):

        cursor = self.connection.cursor()
        query = f'''
                              select type,family,priority,haslength from datatype_info 
                              where database_id = {database_id} AND type = '{type}'
--                               order by id
'''

        try:
            cursor.execute(query)
            output = cursor.fetchall()

            if len(output) >= 1:
                dt = []
                for datatype in output:
                    dt.append(datatype[2])
                    break
                return dt[0]
            return
        except:
            return


    def getDatatypeFamily(self,database_id,type):
        cursor = self.connection.cursor()
        query = f'''
                      select type,family,priority,haslength from datatype_info 
                      where database_id = {database_id} AND type = '{type}'
--                       order by id
'''

        try:
            cursor.execute(query)
            output = cursor.fetchall()

            if len(output) >= 1:
                dt = []
                for datatype in output:
                    dt.append(datatype[1])
                    break
                return dt[0]
            return
        except:
            return


    def getTargetDatatype(self,source_database_id,target_database_id,datatype ):

        if source_database_id == target_database_id:
            return datatype


        cursor = self.connection.cursor()
        query = f'''
                select target_datatype from supported_datatype
                where database_id = {source_database_id}  and target_database_id = {target_database_id} and datatype = '{datatype}'
--                 order by id
                '''

        try:
            cursor.execute(query)
            output = cursor.fetchall()

            if len(output) >= 1:
                dt = []
                for datatype in output:
                    dt.append(datatype[0])
                return dt
            return
        except:
            traceback.print_exc()
            return



