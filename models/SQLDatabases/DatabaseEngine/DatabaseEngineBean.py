import psycopg2

# from constants import CONSTANT
from constants.CONSTANT import SQLDatabase,CONSTANT,VIEW_QUERY_TYPE
from pprint import pprint

from modules.DynamicPrinter import dynamicPrinter
import traceback

class DatabaseEngineBean:

    global con
    global queries

    # queries = {}

    def __init__(self,sql_database):

        username =  CONSTANT.QUERY_DATABASE_CRED.USERNAME
        password =  CONSTANT.QUERY_DATABASE_CRED.PASSWORD
        host =  CONSTANT.QUERY_DATABASE_CRED.HOST
        database = CONSTANT.QUERY_DATABASE_CRED.DATABASE
        port = CONSTANT.QUERY_DATABASE_CRED.PORT

        self.connection = self.connectToPostgres(username,password,host,database,port)
        self.fetchQueriesRelatedToDatabase(self.connection,sql_database)


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

    def fetchQueriesRelatedToDatabase(self,con, sql_database):
        global queries

        cursor = con.cursor()
        cursor.execute(f'''
                          select dvq.database_id,dvq.views_query_types_id,vq.view_query from db_views_queries dvq
                          join view_queries  vq on dvq.view_queries_id = vq.view_queries_id 
                          where dvq.database_id = {sql_database} 
                          order by dvq.views_query_types_id
                        ''')
        output = cursor.fetchall()
        queries = {}
        # if output[0][1] == VIEW_QUERY_TYPE.FETCH_ALL_TABLENAMES:
        #    queries.add({CONSTANT.SCHEMA_KEY.TABLENAMES:output[0][2]})
        #
        # if output[1][1] == VIEW_QUERY_TYPE.FETCH_TABLE_SCHEMA:
        #     queries.update({CONSTANT.TABLE.SCHEMA:output[1][2]})
        #
        # if output[2][1] == VIEW_QUERY_TYPE.FETCH_PRIMARY_KEY:
        #     queries.update({CONSTANT.SCHEMA_KEY.PRIMARY:output[2][2]})
        #
        # if output[3][1] == VIEW_QUERY_TYPE.FETCH_FOREIGN_KEYS:
        #     queries.update({CONSTANT.SCHEMA_KEY.FOREIGN:output[3][2]})
        #
        # if output[4][1] == VIEW_QUERY_TYPE.FETCH_UNIQUE_KEYS:
        #     queries.update({CONSTANT.SCHEMA_KEY.UNIQUE:output[4][2]})
        #
        # if output[5][1] == VIEW_QUERY_TYPE.FETCH_INDEXES:
        #     queries.update({CONSTANT.SCHEMA_KEY.INDEX:output[5][2]})




        queries = {
            CONSTANT.SCHEMA_KEY.TABLENAMES: output[0][2],
            CONSTANT.TABLE.SCHEMA: output[1][2],
            CONSTANT.SCHEMA_KEY.PRIMARY: output[2][2],
            CONSTANT.SCHEMA_KEY.FOREIGN: output[3][2],
            CONSTANT.SCHEMA_KEY.UNIQUE: output[4][2],
            CONSTANT.SCHEMA_KEY.INDEX: output[5][2]
        }

    def fetchingTable(self, con,database,json,sql_database):

        cur = con.cursor()
        query = queries[CONSTANT.SCHEMA_KEY.TABLENAMES]
        query = query.replace(CONSTANT.SERVICE.PLACEHOLDER.DATABASE, database)
        cur.execute(query)

        table_list = cur.fetchall()

        for table in table_list:
            object = {
                CONSTANT.TABLE.TABLE_NAME: table[0],
                CONSTANT.TABLE.ACTION: '',
                CONSTANT.TABLE.COMMENTS: '',
                CONSTANT.TABLE.STORAGE_ENGINE: '',
                CONSTANT.TABLE.CHARACTER_SET: CONSTANT.UTF8,
                CONSTANT.TABLE.COLLATION: CONSTANT.UTF8_GENERAL_CI,
                CONSTANT.TABLE.OWNER: ''
            }
            json.append(object)

    def fetchingValidateTable(self, con,database,json,sql_database):

        cur = con.cursor()
        query = queries[CONSTANT.SCHEMA_KEY.TABLENAMES]
        # query = """
        # select t.table_name from information_schema.tables t
        # where t.table_schema != \\'pg_catalog\\'  and
        # t.table_type = \\'BASE TABLE\\' and TABLE_SCHEMA != \\'information_schema\\'
        # and t.table_type != \\'VIEW\\'
        # order by t.table_name
        # """
        query = query.replace(CONSTANT.SERVICE.PLACEHOLDER.DATABASE, database)
        cur.execute(query)

        table_list = cur.fetchall()

        for table in table_list:
            object = {
                CONSTANT.TABLE.TABLE_NAME: table[0],
                CONSTANT.TABLE.ACTION: '',
                CONSTANT.TABLE.COMMENTS: '',
                CONSTANT.TABLE.STORAGE_ENGINE: '',
                CONSTANT.TABLE.CHARACTER_SET: CONSTANT.UTF8,
                CONSTANT.TABLE.COLLATION: CONSTANT.UTF8_GENERAL_CI,
                CONSTANT.TABLE.OWNER: ''
            }
            json.append(object)

    def fetchingIndexes(self, con,tablename,schema,sql_database):

        cur = con.cursor()
        query = queries[CONSTANT.SCHEMA_KEY.INDEX]
        query = query.replace(CONSTANT.SERVICE.PLACEHOLDER.TABLENAME_SCHEMA,tablename)
        query = query.replace(CONSTANT.SERVICE.PLACEHOLDER.SCHEMA,schema)
        query = query.replace('"', "'")

        cur.execute(query)

        result = cur.fetchall()
        indexes_array = []
        for index_info in result:

            object = {
                CONSTANT.TABLE.ACTION: '',
                CONSTANT.TABLE.INDEX_NAME:  index_info[0],
                CONSTANT.TABLE.INDEX_TYPE:  index_info[1],
                CONSTANT.TABLE.COLUMN_NAME: index_info[2]
            }

            indexes_array.append(object)

        return indexes_array

    def fetchingUnqiueKeys(self,con, tablename,schema,sql_database):
        cur   = con.cursor()
        query = queries[CONSTANT.SCHEMA_KEY.UNIQUE]
        query = query.replace(CONSTANT.SERVICE.PLACEHOLDER.TABLENAME_SCHEMA,tablename)
        query = query.replace(CONSTANT.SERVICE.PLACEHOLDER.SCHEMA, schema)
        query = query.replace('"', "'")

        cur.execute(query)

        result = cur.fetchall()
        unique_keys_array = []
        for uniquekey_info in result:
            object = {
                CONSTANT.TABLE.CONSTRAINT_NAME: uniquekey_info[0],
                CONSTANT.TABLE.COLUMN_NAME: uniquekey_info[1]
            }
            unique_keys_array.append(object)
        return unique_keys_array

    def fetchingForeignKeys(self,con,tablename,schema,sql_database):
        cur = con.cursor()

        query = queries[CONSTANT.SCHEMA_KEY.FOREIGN]
        query = query.replace(CONSTANT.SERVICE.PLACEHOLDER.TABLENAME_SCHEMA, tablename)
        query = query.replace(CONSTANT.SERVICE.PLACEHOLDER.SCHEMA, schema)
        query = query.replace('"', "'")

        cur.execute(query)

        tables = cur.fetchall()

        foreign_key_array = []
        for foreign_info in tables:
            object = {
                CONSTANT.TABLE.KEY_NAME: foreign_info[0],
                CONSTANT.TABLE.COLUMN_NAME: foreign_info[1],
                CONSTANT.TABLE.REFERENCE_TABLE_NAME: foreign_info[2],
                CONSTANT.TABLE.REFERENCE_COLUMN_NAME: foreign_info[3]
            }
            foreign_key_array.append(object)

        return foreign_key_array

    def fetchingPrimaryKeys(self, con, tablename,schema,sql_database):
        cur = con.cursor()

        query = queries[CONSTANT.SCHEMA_KEY.PRIMARY]
        # print(query)
        query = query.replace(CONSTANT.SERVICE.PLACEHOLDER.TABLENAME_SCHEMA, tablename)
        query = query.replace(CONSTANT.SERVICE.PLACEHOLDER.SCHEMA, schema)
        query = query.replace('"',"'")
        # print(query)
        cur.execute(query)

        res = cur.fetchall()
        primaryKey = []
        for r in res:
            obj = {

                CONSTANT.TABLE.CONSTRAINT_NAME: r[0],
                CONSTANT.TABLE.COLUMN_NAME: r[1]

            }
            primaryKey.append(obj)

        return primaryKey

    def fetchingAllTablesSchema(self,con,json,schema,database,sql_database):
        for table in json:
            tablename = table[CONSTANT.TABLE.TABLE_NAME]
            self.fetchingTableSchema(con,json,tablename,schema,sql_database)

    def fetchingTableSchema(self, con,json, tablename, table_schema,database,sql_database):
        cur = con.cursor()

        query = queries[CONSTANT.TABLE.SCHEMA]
        query = query.replace(CONSTANT.SERVICE.PLACEHOLDER.TABLENAME_SCHEMA,tablename)
        query = query.replace(CONSTANT.SERVICE.PLACEHOLDER.DATABASE ,database)
        query = query.replace('"', "'")

        cur.execute(query)

        table_schemas = cur.fetchall()
        columns = []
        global schema
        schema = ''

        for table in table_schemas:
            schema = ''

            if sql_database == 1:
                schema = table[5]
            else:
                schema = tablename

            autoincrement = CONSTANT.SERVICE.STATE.NO_LOWERCASE

            defaultVal = table[4]
            if defaultVal != None:
                defaultVal = defaultVal.replace("'",'').replace('(','').replace(')','')

            if table[5] == False:
                autoincrement = CONSTANT.SERVICE.STATE.NO_LOWERCASE
            elif type(table) == bool and table[5] == True:
                autoincrement = CONSTANT.SERVICE.STATE.YES
            elif CONSTANT.SERVICE.ACTION.NEXT_VAL in str(table[4]) and table[4] != None:
                autoincrement = CONSTANT.SERVICE.STATE.YES
            elif CONSTANT.SERVICE.STATE.ONE in str(table[5]) and table[5] != None:
                autoincrement = CONSTANT.SERVICE.STATE.YES
            elif CONSTANT.SERVICE.KEYWORD.AUTO_INCREMENT_LOWERCASE in str(table[5]) and table[5] != None:
                autoincrement = CONSTANT.SERVICE.STATE.YES

            object = {
                CONSTANT.TABLE.COLUMN_NAME: table[0],
                CONSTANT.TABLE.DATA_TYPE: table[1],
                CONSTANT.TABLE.LENGTH: table[2],
                CONSTANT.TABLE.NULL_CONSTRAINT: table[3],
               CONSTANT.TABLE.AUTO_INCREMENT: autoincrement,
                CONSTANT.TABLE.DEFAULT_VALUE: {
                    CONSTANT.TABLE.CONSTRAINT_NAME: '',
                    CONSTANT.TABLE.DEFAULT_VALUE: defaultVal#table[4]
                }

            }
            columns.append(object)
 #         columns.append(object)

        primaryKey = self.fetchingPrimaryKeys(con, tablename,database,sql_database)
        foreignKeys = self.fetchingForeignKeys(con, tablename,database,sql_database)
        uniqueKeys = self.fetchingUnqiueKeys(con, tablename,database,sql_database)
        indexes = self.fetchingIndexes(con, tablename,database,sql_database)

        for obj in json:
            if tablename == obj[CONSTANT.TABLE.TABLE_NAME]:

                if SQLDatabase.POSTGRES == sql_database or SQLDatabase.MSSQLSERVER == sql_database or SQLDatabase.ORACLE == sql_database:
                    obj[CONSTANT.TABLE.COLUMNS] = columns
                    obj[CONSTANT.TABLE.SCHEMA] = schema
                    obj[CONSTANT.TABLE.CONSTRAINTS] = {
                        CONSTANT.TABLE.PRIMARY_KEY: primaryKey,
                        CONSTANT.TABLE.FOREIGN_KEY: foreignKeys,
                        CONSTANT.TABLE.UNIQUE_KEY: uniqueKeys
                    }
                    obj[CONSTANT.TABLE.INDEXES] = indexes

                elif SQLDatabase.MYSQL == sql_database:
                    obj[CONSTANT.TABLE.COLUMNS] = columns
                    obj[CONSTANT.TABLE.SCHEMA] = table_schema
                    obj[CONSTANT.TABLE.CONSTRAINTS] = {
                        CONSTANT.TABLE.PRIMARY_KEY: primaryKey,
                        CONSTANT.TABLE.FOREIGN_KEY: foreignKeys,
                        CONSTANT.TABLE.UNIQUE_KEY: uniqueKeys
                    }
                    obj[CONSTANT.TABLE.INDEXES] = indexes

        cur.close()
