import psycopg2


from constants.CONSTANT import SQLDatabase,CONSTANT
from pprint import pprint

from modules.DynamicPrinter import dynamicPrinter
import traceback

class DatabaseEngineGeneratorBean:

    global con
    global queries

    def __init__(self):

        username = CONSTANT.QUERY_DATABASE_CRED.USERNAME
        password = CONSTANT.QUERY_DATABASE_CRED.PASSWORD
        host     = CONSTANT.QUERY_DATABASE_CRED.HOST
        database = CONSTANT.QUERY_DATABASE_CRED.DATABASE
        port     = CONSTANT.QUERY_DATABASE_CRED.PORT

        self.connection = self.connectToPostgres(username,password,host,database,port)

    def connectToPostgres(self, user, password, host, database,port):

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

    def getDatatypeInfo(self,database,datatype):
        cursor = self.connection.cursor()

        try:
            cursor.execute(f"SELECT * FROM datatype_info where database_id = {database} and type = '{datatype}' ")
            output = cursor.fetchall()

            if len(output) == 1:
                output = output[0]
                json = {
                    CONSTANT.SERVICE.MAPPER.FAMILY:output[3],
                    CONSTANT.SERVICE.MAPPER.HAS_LENGTH : output[5],
                    CONSTANT.SERVICE.MAPPER.PRIORITY:output[4]
                }
                return json
            return
        except:
            traceback.print_exc()
            return


    def fetchingQuery(self,con,statement_type,sql_database):
        cur = self.connection.cursor()
        cur.execute(f'''
        select ds.database_id,spt.statement_part_type_id,spt.statement_part_type,spd.statement_part_details from database_statements ds
        join  statement_part_details spd on ds.statement_part_details_id = spd.statement_part_details_id
        join  statement_part_types spt on spt.statement_part_type_id = spd.statement_part_type_id
        join  statement_type st on st.statement_type_id = spt.statement_type_id
        where ds.database_id = {sql_database} and st.statement_type_id = {statement_type}
''')
        output = cur.fetchall()
        arr = [statement_type_part[3] for statement_type_part in output]

        return arr