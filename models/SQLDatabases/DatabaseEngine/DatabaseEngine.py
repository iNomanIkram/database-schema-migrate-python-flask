import pyodbc
import sys
import cx_Oracle
import mysql.connector
import psycopg2
import traceback


# from constants import CONSTANT
from constants.CONSTANT import SQLDatabase,CONSTANT
from models.SQLDatabases.DatabaseEngine.DatabaseEngineBean import DatabaseEngineBean
from modules.DynamicPrinter import dynamicPrinter


class DatabaseEngine():
    con = None
    json = None

    def __init__(self, cred):

        try:
            # connect with related database and return con object
            self.con = self.connect(cred.username, cred.password, cred.hostname,cred.port, cred.database,cred.sql_database)

            if self.con == None:
                raise ConnectionError()
            try:
                # After Successful Connection, It will fetch tables and their schemas
                self.fetchingTable(con,cred.database,cred.sql_database)
                self.fetchingAllTablesSchema(con,cred.database,cred.sql_database)
            except:
                traceback.print_exc()
        except Exception as e:
            dynamicPrinter(f'{CONSTANT.EXCEPTION}: {e}')

            return

    def connect(self, user, password, host,port, database, sql_database):
        if SQLDatabase.POSTGRES == sql_database:
            return self.connectToPostgres(user, password, host, database,port)
        elif SQLDatabase.ORACLE == sql_database:
            return self.connectToOracle(user, password, host, port, database)# service <=> database
        elif SQLDatabase.MYSQL == sql_database:
           return  self.connectToMysql(user, password, host, database, port)
        elif SQLDatabase.MSSQLSERVER == sql_database:
            return self.connectToMsSqlServer(user, password, host, port , database)

    def connectToMsSqlServer(self, username, password, host, port, database):

        try:
            global schema
            global con

            driver = CONSTANT.SQL_SERVER.DRIVER_STRING
            con = pyodbc.connect(
                f'{CONSTANT.SQL_SERVER.DRIVER}' + driver + f'{CONSTANT.SQL_SERVER.SERVER}' + host + f'{CONSTANT.SQL_SERVER.PORT}{port}{CONSTANT.SQL_SERVER.DATABASE}' + database + f'{CONSTANT.SQL_SERVER.UID}' + username + f'{CONSTANT.SQL_SERVER.PASSWORD}' + password)
            schema = database

            dynamicPrinter(CONSTANT.CONNECTION_MESSAGE.SQL_SERVER)

            return con

        except Exception as e:
            traceback.print_exc()
            # dynamicPrinter(f'Exception: {e}')
            return

    def connectToMysql(self, user, password, host, database, port):
        global schema
        schema = database

        try:
            global con
            con = mysql.connector.connect(
                user=user,
                password=password,
                host=host,
                port=port,
                database=database
            )
            dynamicPrinter(CONSTANT.CONNECTION_MESSAGE.MYSQL)
            # print('connected')
            return con
        except Exception as e:
            dynamicPrinter(f'{CONSTANT.EXCEPTION}: {e}')
            return

    def connectToOracle(self, username, password, host, port, sid):
        # print()
        try:
            global con
            if port != None or port == '':

                dsn = cx_Oracle.makedsn(host, port, sid=sid)
                con = cx_Oracle.connect(username, password, dsn)
                # dPrinter(CONSTANT.CONNECTION_MESSAGE.ORACLE)

                # con = cx_Oracle.connect(f'{username}/{password}@{host}:{port}/{service}')
                # print(f'{username}/{password}@{host}:{port}/{service}')
                # return con
            else:
                port = CONSTANT.ORACLE.DEFAULT_PORT
                dsn = cx_Oracle.makedsn(host, port, sid=sid)
                con = cx_Oracle.connect(username, password, dsn)
                # con = cx_Oracle.connect(f'{username}/{password}@{host}:{port}/{service}')

            dynamicPrinter(CONSTANT.CONNECTION_MESSAGE.ORACLE)

            return con
        except Exception as e:
            dynamicPrinter(f'{CONSTANT.EXCEPTION}: {e}')
            return

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

            dynamicPrinter(CONSTANT.CONNECTION_MESSAGE.POSTGRES)
            return con
        except Exception as e:
            dynamicPrinter(f'{CONSTANT.EXCEPTION}: {e}')
            return

    def fetchingTable(self,con,database,sql_database):

        global json
        json = []

        bean = DatabaseEngineBean(sql_database)
        bean.fetchingTable(con,database,json,sql_database)

    def fetchingTableSchema(self,con, tablename, sql_database):

        global json

        # object for fetching Table Schema
        bean = DatabaseEngineBean(sql_database)
        bean.fetchingTableSchema(con,json, tablename, tablename,sql_database)

    def fetchingAllTablesSchema(self,con,database, sql_database):

        bean = DatabaseEngineBean(sql_database)

        # Loop for fetching all Table Schemas
        for table in json:
            tablename = table[CONSTANT.TABLE.TABLE_NAME]
            try:
                bean.fetchingTableSchema(con, json, tablename, tablename,database, sql_database)
            except Exception as e:
                dynamicPrinter(f'{CONSTANT.EXCEPTION}: {e}')

    def getJSON(self):
        return json

    def getConnection(self):
        return self.con


