import json as js
# from pprint import pprint
# import traceback
import traceback

import psycopg2

# from models.Mapper.MapperBean import MapperBean
# from models.SQLDatabases.DatabaseEngine.DatabaseEngine import DatabaseEngine
from models.SQLDatabases.DatabaseEngine.DatabaseEngineBean import DatabaseEngineBean
from models.SQLDatabases.DatabaseEngine.DatabaseQueryGeneratorBean import DatabaseEngineGeneratorBean
from modules.DynamicPrinter import dynamicPrinter
from constants.CONSTANT import *
# from static.datatype_info import datatype_info

class DatabaseQueryGenerator:

    db_queries = {}
    datatypes = {}

    def __init__(self,):
        global db_queries,datatypes
        db_queries = {}
        datatypes = {}

        # # credential to make connection
        # self.cred = cred
        # self.con = con #########
        #
        # # Creating Queries from JSON
        # self.queries = json
        #
        # # JSON for generating Add, Drop,Replace & change(update) Queries
        # self.add = js.loads(json)[CONSTANT.SERVICE.ACTION.ADD]
        # self.drop = js.loads(json)[CONSTANT.SERVICE.ACTION.DROP]
        # self.common = js.loads(json)[CONSTANT.SERVICE.ACTION.COMMON]



        # for keeping records of foreign keys that need to be deleted before removing any referred table
        self.keys_for_regeneration = None

        self.updateAllQueries = None
        self.drop_table_queries = None
        self.remove_constraints_queries_before_dropping = None
        self.add_table_queries = None
        self.create_pair_of_queries_for_common_tables = None
        # global self.replaceAllQueries,self.drop_table_before_replace
        self.drop_table_before_replace = None
        self.replaceAllQueries = None
        self.customQueries = None
        self.foreign_keys_for_add_table = None
        self.unique_keys_for_add_table = None
        self.indexes_for_add_table = None
        self.foreign_keys_for_common_table = None
        self.unique_keys_for_common_table = None
        self.indexes_for_common_table = None

        # # removing constraints
        # if action == CONSTANT.SERVICE.ACTION.DROP or action == CONSTANT.SERVICE.ACTION.CUSTOM:
        #     drop_tablenames            = self.getDropTableNames(sql_database)
        #     self.remove_constraints_queries_before_dropping = self.maintain_list_of_constraints_before_dropping_table(con,CONSTANT.SERVICE.ACTION.DROP)
        #     self.drop_table_queries         = self.generateDropQueries(con,CONSTANT.SERVICE.ACTION.DROP,drop_tablenames,sql_database)

        # if action == CONSTANT.SERVICE.ACTION.ADD or action == CONSTANT.SERVICE.ACTION.CUSTOM:
        #     add_tables                 = [table for table in self.add if table[CONSTANT.SERVICE.ACTION.CAN_PERFORM_ACTION] == CONSTANT.SERVICE.STATE.TRUE]
        #     add_tables = self.validateAddTablesNamesFromDatabase(add_tables) ############ validated tables
        #
        #
        #     self.add_table_queries          = self.getCreateTableQuery(con,add_tables,sql_database)
        #     self.foreign_keys_for_add_table = self.createForeignKeys(con,add_tables,sql_database)
        #     self.unique_keys_for_add_table  = self.createUniqueKeys(con,add_tables,sql_database)
        #     self.indexes_for_add_table      = self.createIndexes(con,add_tables,sql_database)

        # if action == CONSTANT.SERVICE.ACTION.REPLACE or action == CONSTANT.SERVICE.ACTION.UPDATE or action == CONSTANT.SERVICE.ACTION.CUSTOM:
        #     self.create_pair_of_queries_for_common_tables = self.maintain_list_of_constraints_before_dropping_table(con,CONSTANT.SERVICE.ACTION.REPLACE)
        #     common_tables = [table for table in self.common.keys()] # tablename keys
        #
        #     common_tables = self.validateCommonTablesNamesFromDatabase(common_tables) # changes made to fixes regeneration of queries
        #
        #     # table node
        #     # common_tbl = [self.common[tablenames][CONSTANT.SERVICE.ACTION.REPLACE] for tablenames in common_tables]
        #     common_tbl = [self.common[tablenames][CONSTANT.SERVICE.ACTION.REPLACE] for tablenames in common_tables ] # need to add condition
        #     if action == CONSTANT.SERVICE.ACTION.REPLACE:
        #
        #         self.drop_table_before_replace = self.generateDropQueries(con,CONSTANT.SERVICE.ACTION.REPLACE,common_tables,sql_database)
        #         self.replaceAllQueries         = self.getReplaceAllTablesQueries(con, common_tables, sql_database)
        #
        #         self.foreign_keys_for_common_table = self.createForeignKeys(con, common_tbl, sql_database)
        #         self.unique_keys_for_common_table  = self.createUniqueKeys(con, common_tbl, sql_database)
        #         self.indexes_for_common_table      = self.createIndexes(con, common_tbl, sql_database)
        #
        #     elif action == CONSTANT.SERVICE.ACTION.UPDATE :
        #         self.updateAllQueries = self.getUpdateAllTablesQueries(con,common_tables,sql_database)
        #
        #     elif action == CONSTANT.SERVICE.ACTION.CUSTOM:
        #         self.customQueries = self.performSpecificAction(con,common_tables,sql_database)

        # if self.remove_constraints_queries_before_dropping != None:
        #     print('********************************************\n'
        #           'remove constraints queries before dropping tables\n'
        #           '********************************************'.upper())
        #     for q in self.remove_constraints_queries_before_dropping :
        #         self.executeQueries(con,q[0])

        # if self.drop_table_queries != None:
        #     print('********************************************\n'
        #           'dropping tables\n'
        #           '********************************************'.upper())
        #     for q in self.drop_table_queries :
        #         self.executeQueries(con,q)

        # if self.add_table_queries!= None:
        #     print('********************************************\n'
        #           'add tables\n'
        #           '********************************************'.upper())
        #     for q in self.add_table_queries :
        #         self.executeQueries(con,q)
        #
        #     if self.foreign_keys_for_add_table != None:
        #         for query in self.foreign_keys_for_add_table:
        #             self.executeQueries(con,query)
        #
        #     if self.unique_keys_for_add_table != None:
        #         for query in self.unique_keys_for_add_table:
        #             self.executeQueries(con,query)
        #
        #     if self.indexes_for_add_table != None:
        #         for query in self.indexes_for_add_table[0]:
        #             self.executeQueries(con,query)

        # if self.create_pair_of_queries_for_common_tables != None:
        #     print('********************************************\n'
        #           'removing constraint queries for common tables\n'
        #           '********************************************'.upper())
        #     for q in self.create_pair_of_queries_for_common_tables :
        #         self.executeQueries(con,q[0])
        #
        # if self.drop_table_before_replace != None:
        #     print('********************************************\n'
        #           'dropping tables for replacing\n'
        #           '********************************************'.upper())
        #     for q in self.drop_table_before_replace:
        #         self.executeQueries(con,q)

        # if self.replaceAllQueries != None:
        #     print('********************************************\n'
        #           'creating tables for replace\n'
        #           '********************************************'.upper())
        #     for q in self.replaceAllQueries:
        #         self.executeQueries(con,q)
        #
        #     if self.foreign_keys_for_common_table != None:
        #         for query in self.foreign_keys_for_common_table:
        #             self.executeQueries(con,query)
        #
        #     if self.unique_keys_for_common_table != None:
        #         for query in self.unique_keys_for_common_table:
        #             self.executeQueries(con,query)
        #
        #     if self.indexes_for_common_table != None:
        #         for query in self.indexes_for_common_table[0]:
        #             self.executeQueries(con,query)
        #
        # if self.updateAllQueries != None:
        #     print('********************************************\n'
        #           'update tables\n'
        #           '********************************************'.upper())
        #     for q in self.updateAllQueries:
        #         self.executeQueries(con,q)
        #
        # if self.customQueries != None:
        #     print('********************************************\n'
        #           'Custom Queries\n'
        #           '********************************************'.upper())
        #     for q in self.customQueries:
        #         self.executeQueries(con,q)
        #
        # if self.create_pair_of_queries_for_common_tables != None:
        #     print('********************************************\n'
        #           'creating constraints again\n'
        #           '********************************************')
        #     for q in self.create_pair_of_queries_for_common_tables:
        #         self.executeQueries(con, q[1])
    #####################################################

    def getInfo(self,con,action,cred,json,sql_database):
        # credential to make connection
        self.cred = cred
        self.con = con  #########

        # Creating Queries from JSON
        self.queries = json

        # JSON for generating Add, Drop,Replace & change(update) Queries
        self.add = js.loads(json)[CONSTANT.SERVICE.ACTION.ADD]
        self.drop = js.loads(json)[CONSTANT.SERVICE.ACTION.DROP]
        self.common = js.loads(json)[CONSTANT.SERVICE.ACTION.COMMON]

    def fetchDropQueries(self,sql_database):
        # global self.remove_constraints_queries_before_dropping,self.drop_table_queries
        drop_tablenames = self.getDropTableNames(sql_database)
        self.remove_constraints_queries_before_dropping = self.maintain_list_of_constraints_before_dropping_table(self.con, CONSTANT.SERVICE.ACTION.DROP)
        self.drop_table_queries = self.generateDropQueries(self.con, CONSTANT.SERVICE.ACTION.DROP, drop_tablenames, sql_database)
        pass

    def fetchingAddQueries(self,sql_database):
        # global self.add_table_queries,self.foreign_keys_for_add_table,self.unique_keys_for_add_table,self.indexes_for_add_table

        add_tables = [table for table in self.add if
                      table[CONSTANT.SERVICE.ACTION.CAN_PERFORM_ACTION] == CONSTANT.SERVICE.STATE.TRUE]

        add_tables = self.validateAddTablesNamesFromDatabase(add_tables)  ############ validated tables

        self.add_table_queries = self.getCreateTableQuery(self.con, add_tables, sql_database)
        self.foreign_keys_for_add_table = self.createForeignKeys(self.con, add_tables, sql_database)
        self.unique_keys_for_add_table = self.createUniqueKeys(self.con, add_tables, sql_database)
        self.indexes_for_add_table = self.createIndexes(self.con, add_tables, sql_database)

    def fetchingCommonQueries(self,action,sql_database):

        # global self.create_pair_of_queries_for_common_tables,self.drop_table_before_replace,self.replaceAllQueries,self.foreign_keys_for_common_table,self.unique_keys_for_common_table,self.indexes_for_common_table,self.updateAllQueries,self.customQueries

        self.create_pair_of_queries_for_common_tables = self.maintain_list_of_constraints_before_dropping_table(self.con,
                                                                                                           CONSTANT.SERVICE.ACTION.REPLACE)
        common_tables = [table for table in self.common.keys()]  # tablename keys

        common_tables = self.validateCommonTablesNamesFromDatabase(common_tables)  # changes made to fixes regeneration of queries

        # table node
        # common_tbl = [self.common[tablenames][CONSTANT.SERVICE.ACTION.REPLACE] for tablenames in common_tables]
        common_tbl = [self.common[tablenames][CONSTANT.SERVICE.ACTION.REPLACE] for tablenames in common_tables]  # need to add condition
        if action == CONSTANT.SERVICE.ACTION.REPLACE:
            # global self.drop_table_before_replace

            self.drop_table_before_replace = self.generateDropQueries(self.con, CONSTANT.SERVICE.ACTION.REPLACE, common_tables, sql_database)
            self.replaceAllQueries = self.getReplaceAllTablesQueries(self.con, common_tables, sql_database)

            self.foreign_keys_for_common_table = self.createForeignKeys(self.con, common_tbl, sql_database)
            self.unique_keys_for_common_table = self.createUniqueKeys(self.con, common_tbl, sql_database)
            self.indexes_for_common_table = self.createIndexes(self.con, common_tbl, sql_database)

        elif action == CONSTANT.SERVICE.ACTION.UPDATE:
            self.updateAllQueries = self.getUpdateAllTablesQueries(self.con, common_tables, sql_database)

        elif action == CONSTANT.SERVICE.ACTION.CUSTOM:
            self.customQueries = self.performSpecificAction(self.con, common_tables, sql_database)

    def executeRemoveConstraintBeforeDropping(self):
        print('********************************************\n'
              'remove constraints queries before dropping tables\n'
              '********************************************'.upper())

        for q in self.remove_constraints_queries_before_dropping:
            self.executeQueries(self.con, q[0])

    def executeDropQueries(self):
        print('********************************************\n'
              'dropping tables\n'
              '********************************************'.upper())
        for q in self.drop_table_queries:
            self.executeQueries(self.con, q)

    def executeAddQueries(self):
        print('********************************************\n'
              'add tables\n'
              '********************************************'.upper())
        for q in self.add_table_queries:
            self.executeQueries(self.con, q)

        if self.foreign_keys_for_add_table != None:
            for query in self.foreign_keys_for_add_table:
                self.executeQueries(self.con, query)

        if self.unique_keys_for_add_table != None:
            for query in self.unique_keys_for_add_table:
                self.executeQueries(self.con, query)

        if self.indexes_for_add_table != None:
            for query in self.indexes_for_add_table:
                for single_query in query[0]:
                    self.executeQueries(self.con,single_query)

    def executePairOfConstraintForCommonTable(self):
        print('********************************************\n'
              'removing constraint queries for common tables\n'
              '********************************************'.upper())
        for q in self.create_pair_of_queries_for_common_tables:
            self.executeQueries(self.con, q[0])

    def executeDropTableBeforeReplace(self):
        print('********************************************\n'
              'dropping tables for replacing\n'
              '********************************************'.upper())
        # global self.drop_table_before_replace
        # print(self.drop_table_before_replace)
        # global self.drop_table_before_replace
        for q in self.drop_table_before_replace:
            self.executeQueries(self.con, q)

    def executeReplaceAllTableQueries(self):
        print('********************************************\n'
              'creating tables for replace\n'
              '********************************************'.upper())

        for q in self.replaceAllQueries:
            self.executeQueries(self.con, q)

        if self.foreign_keys_for_common_table != None:
            for query in self.foreign_keys_for_common_table:
                self.executeQueries(self.con, query)

        if self.unique_keys_for_common_table != None:
            for query in self.unique_keys_for_common_table:
                self.executeQueries(self.con, query)

        if self.indexes_for_common_table != None:
            for query in self.indexes_for_common_table[0]:
                self.executeQueries(self.con, query)

    def executeUpdateAllQueries(self):
        print('********************************************\n'
              'update tables\n'
              '********************************************'.upper())
        for q in self.updateAllQueries:
            self.executeQueries(self.con, q)

    def executeCustomQueries(self):
        print('********************************************\n'
              'Custom Queries\n'
              '********************************************'.upper())
        for q in self.customQueries:
            self.executeQueries(self.con, q)

    def executeCreateConstraintAgain(self):
        print('********************************************\n'
              'creating constraints again\n'
              '********************************************')
        for q in self.create_pair_of_queries_for_common_tables:
            self.executeQueries(self.con, q[1])

    def generateTables(self,con,json,sql_database):
        # global self.add_table_queries,self.foreign_keys_for_add_table,self.unique_keys_for_add_table,self.indexes_for_add_table
        self.con = con
        add_tables = json['tableInfo']
        # arr = []

        tables =  self.getCreateTableQuery(self.con, add_tables, sql_database)
        foreignKeys =  self.createForeignKeys(self.con, add_tables, sql_database)
        uniqueKeys = self.createUniqueKeys(self.con, add_tables, sql_database)
        indexes = self.createIndexes(self.con, add_tables, sql_database)

        for table in  tables:
            self.executeQueries(self.con,table)

        for fk in foreignKeys:
            self.executeQueries(self.con,fk)

        for uk in uniqueKeys:
            self.executeQueries(self.con,uk)

        for index in indexes:
            i = index[0]
            for ind in i:
                self.executeQueries(self.con, ind)


        # for query in arr:
        #     self.executeQueries(con,query)

    ######################################################

    def createForeignKeys(self,con,tables,sql_database):

        queries = []

        for table in tables:
            query = []
            query = self.createForeignKeyForTable(con,table,sql_database)
            queries += query

        return queries

    def createForeignKeyForTable(self,con,table,sql_database):
        queries = []
        foreign_key_query = self.getQuery(con, STATEMENT_TYPE.ADD_FOREIGN_KEY_CONSTRAINT_STATEMENT, sql_database)
        if len(foreign_key_query) == 1:
            foreign_key_query = foreign_key_query[0]
            # print(foreign_key_query)

            tablename = table[CONSTANT.TABLE.TABLE_NAME]
            constraints = table[CONSTANT.TABLE.CONSTRAINTS]
            foreignKeys = constraints[CONSTANT.TABLE.FOREIGN_KEY]

            for fk in foreignKeys:
                query = foreign_key_query

                key_name = fk[CONSTANT.TABLE.KEY_NAME]
                column_name = fk[CONSTANT.TABLE.COLUMN_NAME]
                referenceTableName = fk[CONSTANT.TABLE.REFERENCE_TABLE_NAME]
                reference_column_name = fk[CONSTANT.TABLE.REFERENCE_COLUMN_NAME]

                query = query.replace(CONSTANT.SERVICE.PLACEHOLDER.TABLENAME_GENERATOR, tablename) \
                    .replace(CONSTANT.SERVICE.PLACEHOLDER.CONSTRAINT_NAME, key_name) \
                    .replace(CONSTANT.SERVICE.PLACEHOLDER.COLUMN_NAME, column_name) \
                    .replace(CONSTANT.SERVICE.PLACEHOLDER.REFERENCE_TABLE_NAME, referenceTableName) \
                    .replace(CONSTANT.SERVICE.PLACEHOLDER.REFERENCE_COLUMN_NAME, reference_column_name)
                queries.append(query)
            return queries

    def createUniqueKeys(self, con, tables, sql_database):
        queries = []

        for table in tables:
            query = self.createUniqueKeyForTable(con, table, sql_database)
            queries += query

        return queries

    def createUniqueKeyForTable(self, con, table, sql_database):
        queries = []
        unique_key_query = self.getQuery(con, STATEMENT_TYPE.ADD_UNIQUE_KEY_CONSTRAINT_STATEMENT, sql_database)
        if len(unique_key_query) == 1:
            unique_key_query = unique_key_query[0]

            tablename = table[CONSTANT.TABLE.TABLE_NAME]
            constraints = table[CONSTANT.TABLE.CONSTRAINTS]
            uniqueKeys = constraints[CONSTANT.TABLE.UNIQUE_KEY]

            for uk in uniqueKeys:
                query = unique_key_query

                key_name = uk[CONSTANT.TABLE.CONSTRAINT_NAME]
                column_name = uk[CONSTANT.TABLE.COLUMN_NAME]

                query = query.replace(CONSTANT.SERVICE.PLACEHOLDER.TABLENAME_GENERATOR, tablename) \
                    .replace(CONSTANT.SERVICE.PLACEHOLDER.CONSTRAINT_NAME, key_name) \
                    .replace(CONSTANT.SERVICE.PLACEHOLDER.COLUMN_NAME, column_name) \

                queries.append(query)
            return queries

    def createIndexes(self,con,tables,sql_database):
        queries = []

        for table in tables:
            query = self.createIndexesForTable(con, table, sql_database)

            queries.append(query)

        return queries

    def createIndexesForTable(self,con, table, sql_database):
        add_queries = []
        drop_queries = []

        add_index_query = self.getQuery(con, STATEMENT_TYPE.CREATE_INDEX_STATEMENT, sql_database)
        drop_index_query = self.getQuery(con,STATEMENT_TYPE.DROP_INDEX_STATEMENT,sql_database)
        if len(add_index_query) == 1 and len(drop_index_query) == 1 :
            add_index_query = add_index_query[0]
            drop_index_query = drop_index_query[0]

            tablename = table[CONSTANT.TABLE.TABLE_NAME]
            indexes = table[CONSTANT.TABLE.INDEXES]

            for index in indexes:
                add_query = add_index_query
                drop_query = drop_index_query

                action = index[CONSTANT.TABLE.ACTION]
                index_name = index[CONSTANT.TABLE.INDEX_NAME]
                index_type = index[CONSTANT.TABLE.INDEX_TYPE]
                column_name = index[CONSTANT.TABLE.COLUMN_NAME]

                add_query = add_query.replace(CONSTANT.SERVICE.PLACEHOLDER.TABLENAME_GENERATOR, tablename) \
                    .replace(CONSTANT.SERVICE.PLACEHOLDER.INDEX_NAME, index_name) \
                    .replace(CONSTANT.SERVICE.PLACEHOLDER.COLUMN_NAME, column_name)

                drop_query = drop_query.replace(CONSTANT.SERVICE.PLACEHOLDER.INDEX_NAME,index_name).replace(CONSTANT.SERVICE.PLACEHOLDER.TABLENAME_GENERATOR,tablename)

                add_queries.append(add_query)
                drop_queries.append(drop_query)
            return (add_queries,drop_queries)

    def getUpdateAllTablesQueries(self,con,common_tables,sql_database):
        queries = []
        for common_table_name in common_tables:
            table = self.common[common_table_name][CONSTANT.SERVICE.ACTION.UPDATE]
            if table[CONSTANT.SERVICE.ACTION.CAN_PERFORM_ACTION] == CONSTANT.SERVICE.STATE.TRUE:####
                self.get_update_table_queries(con, table, queries, sql_database)

        return  queries

    def get_update_table_queries(self, con, table, queries, sql_database):
        tablename = table[CONSTANT.TABLE.TABLE_NAME]
        columns = table[CONSTANT.TABLE.COLUMNS]
        indexes = table[CONSTANT.TABLE.INDEXES]
        constraints = table[CONSTANT.TABLE.CONSTRAINTS]
        primaryKey = constraints[CONSTANT.TABLE.PRIMARY_KEY]
        foreignKey = constraints[CONSTANT.TABLE.FOREIGN_KEY]
        unqiueKey = constraints[CONSTANT.TABLE.UNIQUE_KEY]

        action = table[CONSTANT.TABLE.ACTION]
        canPerformAction = table[CONSTANT.SERVICE.ACTION.CAN_PERFORM_ACTION]
        for column in columns:
            action = column[CONSTANT.TABLE.ACTION]
            column_name = column[CONSTANT.TABLE.COLUMN_NAME]
            db =   sql_database#self.database_map_by_number(sql_database)
            datatype = None
            null_constraint = None

            self.getDatatypeInfo(sql_database,datatype)

            if action == CONSTANT.SERVICE.ACTION.ADD:
                add_column_query = self.getQuery(con, STATEMENT_TYPE.ADD_COLUMN_STATEMENT, sql_database)
                if len(add_column_query) == 1:
                    add_column_query = add_column_query[0]

                    query = add_column_query

                    datatype = " " + column[CONSTANT.TABLE.DATA_TYPE].upper()
                    null_constraint = column[CONSTANT.TABLE.NULL_CONSTRAINT]
                    auto_increment = column[CONSTANT.TABLE.AUTO_INCREMENT]
                    default_value = column[CONSTANT.TABLE.DEFAULT_VALUE][CONSTANT.TABLE.DEFAULT_VALUE]
                    length = ''

                    # if datatype is capable to mention length/limit
                    if self.getDatatypeInfo(sql_database, column[CONSTANT.TABLE.DATA_TYPE].upper())[CONSTANT.SERVICE.MAPPER.HAS_LENGTH] == CONSTANT.SERVICE.STATE.YES and column[CONSTANT.TABLE.LENGTH] != None:
                        length = "(" + str(column[CONSTANT.TABLE.LENGTH]) + ")"

                    if null_constraint == CONSTANT.SERVICE.STATE.N or null_constraint == CONSTANT.SERVICE.STATE.NO:
                        null_constraint = CONSTANT.SERVICE.KEYWORD.NOT_NULL
                    else:
                        null_constraint = CONSTANT.SERVICE.KEYWORD.NULL

                    if default_value == None:
                        default_value = ''
                    else:
                        default_value_query = self.getQuery(con, STATEMENT_TYPE.ADD_DEFAULY_VALUE_STATEMENT,
                                                            sql_database)
                        if len(default_value_query) == 1:
                            default_value_query = default_value_query[0]

                            if CONSTANT.SERVICE.ACTION.NEXT_VAL in default_value and sql_database == 1:
                                datatype = CONSTANT.SERVICE.KEYWORD.SERIAL
                            elif CONSTANT.SERVICE.ACTION.NEXT_VAL in default_value and sql_database > 1:
                                auto_increment = CONSTANT.SERVICE.STATE.YES

                        # if the family of datatype is text the but quote around value, else no need
                        if self.getDatatypeInfo(sql_database, column[CONSTANT.TABLE.DATA_TYPE].upper())[CONSTANT.SERVICE.MAPPER.FAMILY] == CONSTANT.SERVICE.MAPPER.DATATYPE.TEXT:
                            default_value = default_value_query.replace(CONSTANT.SERVICE.PLACEHOLDER.TABLENAME_GENERATOR, tablename).replace(
                                CONSTANT.SERVICE.PLACEHOLDER.CONSTRAINT_NAME, f'dk_{column_name}').replace(CONSTANT.SERVICE.PLACEHOLDER.COLUMN_NAME,
                                                                                     column_name).replace(
                                CONSTANT.SERVICE.PLACEHOLDER.DEFAULT_VALUE, f" '{default_value.strip()}' ")
                        else:
                            default_value = default_value_query.replace(CONSTANT.SERVICE.PLACEHOLDER.COLUMN_NAME, column_name).replace(
                                CONSTANT.SERVICE.PLACEHOLDER.DEFAULT_VALUE, default_value).replace(CONSTANT.SERVICE.PLACEHOLDER.CONSTRAINT_NAME,
                                                                             f'dk_{column_name}').replace(
                                CONSTANT.SERVICE.PLACEHOLDER.COLUMN_NAME, column_name)

                    if auto_increment == CONSTANT.SERVICE.STATE.NO_LOWERCASE:
                        auto_increment = ''
                    elif auto_increment == CONSTANT.SERVICE.STATE.YES:
                        auto_increment = CONSTANT.SERVICE.KEYWORD.AUTO_INCREMENT

                    query = query.replace(CONSTANT.SERVICE.PLACEHOLDER.TABLENAME_GENERATOR, tablename).replace(CONSTANT.SERVICE.PLACEHOLDER.COLUMN_NAME,
                                                                                column_name).replace(CONSTANT.SERVICE.PLACEHOLDER.DATATYPE,
                                                                                                     datatype).replace(
                        CONSTANT.SERVICE.PLACEHOLDER.LENGTH, length)
                    queries.append(query)

                    not_null_constraint_query = ''
                    if null_constraint == CONSTANT.SERVICE.KEYWORD.NOT_NULL:
                        not_null_constraint_query = self.getQuery(con, STATEMENT_TYPE.ADD_NULL_STATEMENT, sql_database)
                        if len(not_null_constraint_query) == 1:
                            not_null_constraint_query = not_null_constraint_query[0]
                            not_null_constraint_query = not_null_constraint_query.replace(CONSTANT.SERVICE.PLACEHOLDER.TABLENAME_GENERATOR,
                                                                                          tablename).replace(
                                CONSTANT.SERVICE.PLACEHOLDER.COLUMN_NAME, column_name).replace(CONSTANT.SERVICE.PLACEHOLDER.DATATYPE, datatype)
                            queries.append(not_null_constraint_query)

                    if default_value != None:

                        if default_value != '"' and default_value != '' and default_value != "''":
                            queries.append(default_value)  ###\\

            elif action == CONSTANT.SERVICE.ACTION.DROP:
                drop_query = self.getQuery(con, STATEMENT_TYPE.DROP_COLUMN_STATEMENT, sql_database)

                if len(drop_query) == 1:
                    drop_query = drop_query[0]
                    drop = drop_query.replace(CONSTANT.SERVICE.PLACEHOLDER.TABLENAME_GENERATOR, tablename).replace(CONSTANT.SERVICE.PLACEHOLDER.COLUMN_NAME, column_name)
                    queries.append(drop)

            elif action == CONSTANT.SERVICE.ACTION.UPDATE:

                default_value = column[CONSTANT.TABLE.DEFAULT_VALUE]


                if default_value != None:

                    if default_value[CONSTANT.TABLE.DEFAULT_VALUE] != None:
                        default_value = default_value[CONSTANT.TABLE.DEFAULT_VALUE]

                        default_value_query = self.getQuery(con, STATEMENT_TYPE.ADD_DEFAULY_VALUE_STATEMENT,
                                                            sql_database)
                        if len(default_value_query) == 1:
                            default_value_query = default_value_query[0]

                            if CONSTANT.SERVICE.ACTION.NEXT_VAL in default_value and sql_database == 1:
                                datatype = CONSTANT.SERVICE.KEYWORD.SERIAL
                            elif CONSTANT.SERVICE.ACTION.NEXT_VAL in default_value and sql_database > 1:
                                auto_increment = CONSTANT.SERVICE.STATE.YES

                        default_value = default_value_query.replace(CONSTANT.SERVICE.PLACEHOLDER.TABLENAME_GENERATOR, tablename).replace(
                            CONSTANT.SERVICE.PLACEHOLDER.COLUMN_NAME, column_name).replace(CONSTANT.SERVICE.PLACEHOLDER.DEFAULT_VALUE,
                                                                     f"'{default_value}' ").replace(
                            CONSTANT.SERVICE.PLACEHOLDER.CONSTRAINT_NAME, f'dk_{column_name}')
                        queries.append(default_value)

                if datatype != None and length != None:
                    change_datatype_query = self.getQuery(con, STATEMENT_TYPE.CHANGING_DATATYPE, sql_database)

                    if len(change_datatype_query) == 1:
                        change_datatype_query = change_datatype_query[0]

                        change_datatype_query = change_datatype_query.replace(CONSTANT.SERVICE.PLACEHOLDER.TABLENAME_GENERATOR, tablename).replace(
                            CONSTANT.SERVICE.PLACEHOLDER.COLUMN_NAME, column_name).replace(CONSTANT.SERVICE.PLACEHOLDER.DATATYPE, datatype).replace(CONSTANT.SERVICE.PLACEHOLDER.LENGTH,
                                                                                                         length)
                        queries.append(change_datatype_query)

                if null_constraint != None:

                    if null_constraint == CONSTANT.SERVICE.STATE.N or null_constraint == CONSTANT.SERVICE.STATE.NO:
                        null_constraint_query = self.getQuery(con, STATEMENT_TYPE.ADD_NULL_STATEMENT, sql_database)
                        if len(null_constraint_query) == 1:
                            null_constraint_query = null_constraint_query[0]
                            null_constraint_query = null_constraint_query.replace(CONSTANT.SERVICE.PLACEHOLDER.TABLENAME_GENERATOR, tablename).replace(
                                CONSTANT.SERVICE.PLACEHOLDER.COLUMN_NAME, column_name)
                            queries.append(null_constraint_query)
                    else:
                        not_null_constraint_query = self.getQuery(con, STATEMENT_TYPE.DROP_NULL_STATEMENT, sql_database)
                        if len(not_null_constraint_query) == 1:
                            not_null_constraint_query = not_null_constraint_query[0]
                            not_null_constraint_query = not_null_constraint_query.replace(CONSTANT.SERVICE.PLACEHOLDER.TABLENAME_GENERATOR, tablename)\
                                .replace(CONSTANT.SERVICE.PLACEHOLDER.COLUMN_NAME, column_name)
                            queries.append(not_null_constraint_query)

        self.updatePrimaryKeys(con,primaryKey,queries,tablename,sql_database)
        self.updateForeignKeys(con,foreignKey,queries,tablename,sql_database)
        self.updateUniqueKeys(con,unqiueKey,queries,tablename,sql_database)

        self.updateIndexes(con,indexes,queries,tablename,sql_database)

    def updatePrimaryKeys(self,con,primary_keys,queries,tablename,sql_database):

        add_query = self.getQuery(con, STATEMENT_TYPE.ADD_PRIMARY_KEY_CONSTRAINT_STATEMENT, sql_database)
        drop_query = self.getQuery(con, STATEMENT_TYPE.DROP_PRIMARY_KEY_CONSTRAINT_STATEMENT, sql_database)

        if len(add_query) == 1 and len(drop_query) == 1:
            add_query = add_query[0]
            drop_query = drop_query[0]

        for primary in primary_keys:
            action = primary[CONSTANT.TABLE.ACTION]
            constraint_name = primary[CONSTANT.TABLE.CONSTRAINT_NAME]
            column_name = primary[CONSTANT.TABLE.COLUMN_NAME]

            if action == CONSTANT.SERVICE.ACTION.UPDATE:
                add = add_query
                drop = drop_query
                drop = drop.replace(CONSTANT.SERVICE.PLACEHOLDER.CONSTRAINT_NAME, constraint_name).replace(CONSTANT.SERVICE.PLACEHOLDER.TABLENAME_GENERATOR, tablename)
                add = add.replace(CONSTANT.SERVICE.PLACEHOLDER.CONSTRAINT_NAME, constraint_name).replace(CONSTANT.SERVICE.PLACEHOLDER.TABLENAME_GENERATOR, tablename).replace(CONSTANT.SERVICE.PLACEHOLDER.COLUMN_NAME, column_name)
                queries.append(drop)
                queries.append(add)
            elif action == CONSTANT.SERVICE.ACTION.ADD:
                add = add_query
                add = add.replace(CONSTANT.SERVICE.PLACEHOLDER.CONSTRAINT_NAME, constraint_name).replace(CONSTANT.SERVICE.PLACEHOLDER.TABLENAME_GENERATOR, tablename).replace(
                    CONSTANT.SERVICE.PLACEHOLDER.COLUMN_NAME, column_name)
                queries.append(add)
            elif action == CONSTANT.SERVICE.ACTION.DROP:
                drop = drop_query
                drop = drop.replace(CONSTANT.SERVICE.PLACEHOLDER.CONSTRAINT_NAME, constraint_name).replace(CONSTANT.SERVICE.PLACEHOLDER.TABLENAME_GENERATOR, tablename)
                queries.append(drop)

    def updateForeignKeys(self,con,foreign_keys,queries,tablename,sql_database):
        add_query = self.getQuery(con, STATEMENT_TYPE.ADD_FOREIGN_KEY_CONSTRAINT_STATEMENT, sql_database)
        drop_query = self.getQuery(con, STATEMENT_TYPE.DROP_FOREIGN_KEY_CONSTRAINT_STATEMENT, sql_database)

        if len(add_query) == 1 and len(drop_query) == 1:
            add_query = add_query[0]
            drop_query = drop_query[0]

        for foreign in foreign_keys:
            action = foreign[CONSTANT.TABLE.ACTION]
            key_name = foreign[CONSTANT.TABLE.KEY_NAME]
            reference_table_name = foreign_keys[CONSTANT.TABLE.REFERENCE_TABLE_NAME]
            reference_column_name = foreign_keys[CONSTANT.TABLE.REFERENCE_COLUMN_NAME]
            column_name = foreign[CONSTANT.TABLE.COLUMN_NAME]

            if action == CONSTANT.SERVICE.ACTION.UPDATE:
                add = add_query
                drop = drop_query
                drop = drop.replace(CONSTANT.SERVICE.PLACEHOLDER.TABLENAME_GENERATOR, tablename).replace(CONSTANT.SERVICE.PLACEHOLDER.KEYNAME, key_name)
                add = add.replace(CONSTANT.SERVICE.PLACEHOLDER.KEYNAME, key_name)\
                         .replace(CONSTANT.SERVICE.PLACEHOLDER.TABLENAME_GENERATOR, tablename)\
                         .replace(CONSTANT.SERVICE.PLACEHOLDER.COLUMN_NAME, column_name)\
                         .replace(CONSTANT.SERVICE.PLACEHOLDER.REFERENCE_TABLE_NAME,reference_table_name)\
                         .replace(CONSTANT.SERVICE.PLACEHOLDER.REFERENCE_COLUMN_NAME,reference_column_name)
                queries.append(drop)
                queries.append(add)
            elif action == CONSTANT.SERVICE.ACTION.ADD:
                add = add_query
                add = add.replace(CONSTANT.SERVICE.PLACEHOLDER.KEYNAME, key_name)\
                         .replace(CONSTANT.SERVICE.PLACEHOLDER.TABLENAME_GENERATOR, tablename)\
                         .replace(CONSTANT.SERVICE.PLACEHOLDER.COLUMN_NAME, column_name)\
                         .replace(CONSTANT.SERVICE.PLACEHOLDER.REFERENCE_TABLE_NAME,reference_table_name)\
                         .replace(CONSTANT.SERVICE.PLACEHOLDER.REFERENCE_COLUMN_NAME,reference_column_name)
                queries.append(add)
            elif action == CONSTANT.SERVICE.ACTION.DROP:
                drop = drop_query
                drop = drop.replace(CONSTANT.SERVICE.PLACEHOLDER.TABLENAME_GENERATOR, tablename).replace(CONSTANT.SERVICE.PLACEHOLDER.KEYNAME, key_name)
                queries.append(drop)

    def updateUniqueKeys(self,con,unqiue_keys,queries,tablename,sql_database):
        add_query = self.getQuery(con, STATEMENT_TYPE.ADD_PRIMARY_KEY_CONSTRAINT_STATEMENT, sql_database)
        drop_query = self.getQuery(con, STATEMENT_TYPE.DROP_PRIMARY_KEY_CONSTRAINT_STATEMENT, sql_database)

        if len(add_query) == 1 and len(drop_query) == 1:
            add_query = add_query[0]
            drop_query = drop_query[0]

        for unique in unqiue_keys:
            action = unique[CONSTANT.TABLE.ACTION]
            constraint_name = unique[CONSTANT.TABLE.CONSTRAINT_NAME]
            column_name = unique[CONSTANT.TABLE.COLUMN_NAME]

            if action == CONSTANT.SERVICE.ACTION.UPDATE:
                add = add_query
                drop = drop_query
                drop = drop.replace(CONSTANT.SERVICE.PLACEHOLDER.CONSTRAINT_NAME, constraint_name).replace(CONSTANT.SERVICE.PLACEHOLDER.TABLENAME_GENERATOR, tablename)
                add = add.replace(CONSTANT.SERVICE.PLACEHOLDER.CONSTRAINT_NAME, constraint_name).replace(CONSTANT.SERVICE.PLACEHOLDER.TABLENAME_GENERATOR,tablename).replace(CONSTANT.SERVICE.PLACEHOLDER.COLUMN_NAME, column_name)
                queries.append(drop)
                queries.append(add)
            elif action == CONSTANT.SERVICE.ACTION.ADD:
                add = add_query
                add = add.replace(CONSTANT.SERVICE.PLACEHOLDER.CONSTRAINT_NAME, constraint_name).replace(CONSTANT.SERVICE.PLACEHOLDER.TABLENAME_GENERATOR, tablename).replace(CONSTANT.SERVICE.PLACEHOLDER.COLUMN_NAME, column_name)
                queries.append(add)
            elif action == CONSTANT.SERVICE.ACTION.DROP:
                drop = drop_query
                drop = drop.replace(CONSTANT.SERVICE.PLACEHOLDER.CONSTRAINT_NAME, constraint_name).replace(CONSTANT.SERVICE.PLACEHOLDER.TABLENAME_GENERATOR, tablename)
                queries.append(drop)

    def updateIndexes(self,con,indexes,queries,tablename,sql_database):
        add_query = self.getQuery(con,STATEMENT_TYPE.CREATE_INDEX_STATEMENT,sql_database)
        drop_query = self.getQuery(con,STATEMENT_TYPE.DROP_INDEX_STATEMENT,sql_database)

        if len(add_query) == 1 and len(drop_query) == 1:
            add_query = add_query[0]
            drop_query = drop_query[0]

        for index in indexes:
            action = index[CONSTANT.TABLE.ACTION]
            index_name = index[CONSTANT.TABLE.INDEX_NAME]
            index_type = index[CONSTANT.TABLE.INDEX_TYPE]
            column_name = index[CONSTANT.TABLE.COLUMN_NAME]

            if action == CONSTANT.SERVICE.ACTION.UPDATE:
                add = add_query
                drop = drop_query
                drop = drop.replace(CONSTANT.SERVICE.PLACEHOLDER.INDEX_NAME,index_name).replace(CONSTANT.SERVICE.PLACEHOLDER.TABLENAME_GENERATOR,tablename)
                add = add.replace(CONSTANT.SERVICE.PLACEHOLDER.INDEX_NAME,index_name).replace(CONSTANT.SERVICE.PLACEHOLDER.TABLENAME_GENERATOR,tablename).replace(CONSTANT.SERVICE.PLACEHOLDER.COLUMN_NAME,column_name)
                queries.append(drop)
                queries.append(add)
            elif action == CONSTANT.SERVICE.ACTION.ADD:
                add = add_query
                add = add.replace(CONSTANT.SERVICE.PLACEHOLDER.INDEX_NAME, index_name).replace(CONSTANT.SERVICE.PLACEHOLDER.TABLENAME_GENERATOR, tablename).replace(
                    CONSTANT.SERVICE.PLACEHOLDER.COLUMN_NAME, column_name)
                queries.append(add)
            elif action == CONSTANT.SERVICE.ACTION.DROP:
                drop = drop_query
                drop = drop.replace(CONSTANT.SERVICE.PLACEHOLDER.INDEX_NAME, index_name).replace(CONSTANT.SERVICE.PLACEHOLDER.TABLENAME_GENERATOR, tablename)
                queries.append(drop)

    def getReplaceAllTablesQueries(self, con, common_tables, sql_database):
        arr = []
        for common_table in common_tables:
            table = self.common[common_table][CONSTANT.SERVICE.ACTION.REPLACE]
            if table[CONSTANT.SERVICE.ACTION.CAN_PERFORM_ACTION] == CONSTANT.SERVICE.STATE.TRUE:
                arr.append(table)

        queries = self.getCreateTableQuery(con,arr,sql_database)
        return queries

    def getCreateTableQuery(self,con,add_tables,sql_database):
        # add_tables = [table for table in self.add]

        queries = []
        for table in add_tables:
            self.createIndividualTableCreateQueryPart(con, table, sql_database, queries)

        return queries

    def createIndividualTableCreateQueryPart(self, con, table, sql_database, queries):
        create_table_query = self.getQuery(con, STATEMENT_TYPE.CREATE_TABLE_STATEMENT, sql_database)
        create_table_part, add_column_part, null_constraint_part, default_value_part, auto_increment_part, unique_part, primary_part, foreign_part = \
            create_table_query[0], create_table_query[1], create_table_query[2], create_table_query[3], \
            create_table_query[4], create_table_query[5], create_table_query[6], create_table_query[7]

        db = sql_database #self.database_map_by_number(sql_database)
        tablename = table[CONSTANT.TABLE.TABLE_NAME]
        action = table[CONSTANT.TABLE.ACTION]

        # canPerformAction = table[CONSTANT.SERVICE.ACTION.CAN_PERFORM_ACTION]

        schema = table[CONSTANT.TABLE.SCHEMA]
        columns = table[CONSTANT.TABLE.COLUMNS]
        constraints = table[CONSTANT.TABLE.CONSTRAINTS]
        # index = table[CONSTANT.TABLE.INDEXES]
        primary_key_column_detail = self.get_primary_key_column_name_by_tablename(constraints)
        unique_keys = self.get_unique_keys_column_name_by_tablename(constraints)
        global query
        query = f'{create_table_part.replace(CONSTANT.SERVICE.PLACEHOLDER.TABLENAME_GENERATOR, tablename)}'
        number_of_columns = len(columns)
        is_pk_found = False  # to save the status, if primary key is found in table


        for (index, column) in enumerate(columns):

            column_name = column[CONSTANT.TABLE.COLUMN_NAME]
            datatype = " " + column[CONSTANT.TABLE.DATA_TYPE].upper()
            null_constraint = column[CONSTANT.TABLE.NULL_CONSTRAINT]
            auto_increment = column[CONSTANT.TABLE.AUTO_INCREMENT]
            default_value = column[CONSTANT.TABLE.DEFAULT_VALUE][CONSTANT.TABLE.DEFAULT_VALUE]
            length = ''  #################

            if column[CONSTANT.TABLE.DATA_TYPE] == 'character varying':
                column[CONSTANT.TABLE.DATA_TYPE] = 'varchar'

            # print(self.getDatatypeInfo(sql_database, column[CONSTANT.TABLE.DATA_TYPE].upper()))
            # if datatype is capable to mention length/limit
            if self.getDatatypeInfo(sql_database, column[CONSTANT.TABLE.DATA_TYPE].upper())[CONSTANT.SERVICE.MAPPER.HAS_LENGTH] == CONSTANT.SERVICE.STATE.YES and column[CONSTANT.TABLE.LENGTH] != None:
                length = "(" + str(column[CONSTANT.TABLE.LENGTH]) + ")"

            if null_constraint == CONSTANT.SERVICE.STATE.N or null_constraint == CONSTANT.SERVICE.STATE.NO:
                null_constraint = CONSTANT.SERVICE.KEYWORD.NOT_NULL
            else:
                null_constraint = CONSTANT.SERVICE.KEYWORD.NULL

            if default_value == None:
                default_value = ''
            else:
                default_value_query = self.getQuery(con, STATEMENT_TYPE.ADD_DEFAULY_VALUE_STATEMENT, sql_database)
                if len(default_value_query) == 1:
                    default_value_query = default_value_query[0]

                    if CONSTANT.SERVICE.ACTION.NEXT_VAL in default_value and sql_database == 1:
                        datatype = CONSTANT.SERVICE.KEYWORD.SERIAL
                    elif CONSTANT.SERVICE.ACTION.NEXT_VAL in default_value and sql_database > 1:
                        auto_increment = CONSTANT.SERVICE.STATE.YES

                # if the family of datatype is text the but quote around value, else no need
                if self.getDatatypeInfo(sql_database, column[CONSTANT.TABLE.DATA_TYPE].upper())[CONSTANT.SERVICE.MAPPER.FAMILY] == CONSTANT.SERVICE.MAPPER.DATATYPE.TEXT:
                    default_value = default_value_query.replace(CONSTANT.SERVICE.PLACEHOLDER.TABLENAME_GENERATOR, tablename).replace(
                        CONSTANT.SERVICE.PLACEHOLDER.CONSTRAINT_NAME, f'dk_{column_name}').replace(CONSTANT.SERVICE.PLACEHOLDER.COLUMN_NAME, column_name).replace(
                        CONSTANT.SERVICE.PLACEHOLDER.DEFAULT_VALUE, f"'{default_value.strip()}'")
                else:
                    default_value = default_value_query.replace(CONSTANT.SERVICE.PLACEHOLDER.COLUMN_NAME, column_name).replace(
                        CONSTANT.SERVICE.PLACEHOLDER.DEFAULT_VALUE, default_value).replace(CONSTANT.SERVICE.PLACEHOLDER.TABLENAME_GENERATOR, tablename)

            if auto_increment == CONSTANT.SERVICE.STATE.NO_LOWERCASE:
                auto_increment = ''
            elif auto_increment == CONSTANT.SERVICE.STATE.YES:
                auto_increment = CONSTANT.SERVICE.KEYWORD.AUTO_INCREMENT

            if datatype == CONSTANT.SERVICE.KEYWORD.SERIAL:
                auto_increment = ' '

            query += f"{add_column_part.replace(CONSTANT.SERVICE.PLACEHOLDER.COLUMN_NAME, column_name).replace(CONSTANT.SERVICE.PLACEHOLDER.DATATYPE,datatype).replace(CONSTANT.SERVICE.PLACEHOLDER.LENGTH, length)} {null_constraint_part.replace(CONSTANT.SERVICE.PLACEHOLDER.NULL_CONSTRAINT,null_constraint)} {auto_increment_part.replace(CONSTANT.SERVICE.PLACEHOLDER.AUTO_INCREMENT, auto_increment)} "

            # Found and Reserver for Later Append, if pk is not found only then it would be executed
            if is_pk_found == False:
                if primary_key_column_detail != None:

                    const_name = primary_key_column_detail[1]
                    if const_name == CONSTANT.SERVICE.KEYWORD.PRIMARY:
                        const_name = f'pk_{primary_key_column_detail[0]}'

                    pk = primary_part.replace(CONSTANT.SERVICE.PLACEHOLDER.CONSTRAINT_NAME,const_name).replace(CONSTANT.SERVICE.PLACEHOLDER.COLUMN_NAME, primary_key_column_detail[0])
                    is_pk_found = True

            # if there are some columns remaining
            if index < number_of_columns - 1:
                query += ','
            else:  # when all the columns are iterated, atlast primary key is appended, followed by unique keys if there is any
                if is_pk_found == True:
                    query += f' ,{pk}'

                number_of_unique_index = len(unique_keys)
                for (i, unique) in enumerate(unique_keys):
                    query += f" ,{unique_part.replace(CONSTANT.SERVICE.PLACEHOLDER.CONSTRAINT_NAME, unique[CONSTANT.TABLE.CONSTRAINT_NAME]).replace(CONSTANT.SERVICE.PLACEHOLDER.COLUMN_NAME, unique[CONSTANT.TABLE.COLUMN_NAME])}"
                    if i < number_of_unique_index - 1:
                        query += ','
        query += ")"

        queries.append(query)
        if default_value != None or default_value != '':
            queries.append(default_value)
            default_value = ''

    def get_primary_key_column_name_by_tablename(self,constraints):
        primary_key = constraints[CONSTANT.TABLE.PRIMARY_KEY]

        if len(primary_key) == 1 :
            return (primary_key[0][CONSTANT.TABLE.COLUMN_NAME],primary_key[0][CONSTANT.TABLE.CONSTRAINT_NAME])
        else:
            return None

    def get_unique_keys_column_name_by_tablename(self,constraints):
        unique_keys = constraints[CONSTANT.TABLE.UNIQUE_KEY]
        return unique_keys

    def maintain_list_of_constraints_before_dropping_table(self,con,action):
        drop_tablenames  = [table[CONSTANT.TABLE.TABLE_NAME] for table in self.drop  if table[CONSTANT.SERVICE.ACTION.CAN_PERFORM_ACTION] == CONSTANT.SERVICE.STATE.TRUE]
        common_tablenames = [key for key in self.common.keys()]

        drop_tablenames = self.validateDropTablesNamesFromDatabase(drop_tablenames, self.cred.sql_database)###########
        common_tablenames = self.validateCommonTablesNamesFromDatabase(common_tablenames)########

        # no need to add add_tablenames to the the tablenames list
        # add_tablenames    = [table[CONSTANT.TABLE.TABLE_NAME] for table in self.add  if table[CONSTANT.SERVICE.ACTION.CAN_PERFORM_ACTION] == CONSTANT.SERVICE.STATE.TRUE]
        tablenames = drop_tablenames + common_tablenames

        ## add validated drop and common table names instead

        bean = DatabaseEngineBean(self.cred.sql_database)
        foreignKeys = []
        queries = []
        for tablename in tablenames:
            foreignKeys += bean.fetchingForeignKeys(con, tablename, self.cred.database, self.cred.sql_database)

            if action == CONSTANT.SERVICE.ACTION.DROP:
                queries += self.generatePairsOfAddDropConstraint(con,foreignKeys, tablename,drop_tablenames,action)
            elif action == CONSTANT.SERVICE.ACTION.REPLACE or action == CONSTANT.SERVICE.ACTION.UPDATE or action == CONSTANT.SERVICE.ACTION.CUSTOM:
                queries += self.generatePairsOfAddDropConstraint(con, foreignKeys, tablename, tablenames, action)
        return queries

    def generatePairsOfAddDropConstraint(self,con,foreignKeys,tablename,tablenames,action):
        arr = []

        drop_constraint = self.getQuery(con, STATEMENT_TYPE.DROP_FOREIGN_KEY_CONSTRAINT_STATEMENT,  self.cred.sql_database)[0]
        add_constraint  = self.getQuery(con, STATEMENT_TYPE.ADD_FOREIGN_KEY_CONSTRAINT_STATEMENT,  self.cred.sql_database)[0]

        for obj in foreignKeys:
            drop = drop_constraint
            add  = add_constraint
            if obj[CONSTANT.TABLE.REFERENCE_TABLE_NAME] in tablenames:
                if action == CONSTANT.SERVICE.ACTION.DROP:
                    drop = drop.replace(CONSTANT.SERVICE.PLACEHOLDER.TABLENAME_GENERATOR,tablename).replace(CONSTANT.SERVICE.PLACEHOLDER.KEYNAME,obj[CONSTANT.TABLE.KEY_NAME])
                    #add = add.replace(CONSTANT.SERVICE.PLACEHOLDER.TABLENAME_GENERATOR,tablename).replace(CONSTANT.SERVICE.PLACEHOLDER.CONSTRAINT_NAME,obj[CONSTANT.TABLE.KEY_NAME]).replace('##referenceTablename@@##',obj[CONSTANT.TABLE.REFERENCE_TABLE_NAME]).replace(CONSTANT.SERVICE.PLACEHOLDER.REFERENCE_COLUMN_NAME,obj[CONSTANT.TABLE.REFERENCE_COLUMN_NAME])
                    arr.append( (drop,'')  )
                elif action == CONSTANT.SERVICE.ACTION.REPLACE or action == CONSTANT.SERVICE.ACTION.UPDATE:
                    drop = drop.replace(CONSTANT.SERVICE.PLACEHOLDER.TABLENAME_GENERATOR, tablename).replace(CONSTANT.SERVICE.PLACEHOLDER.KEYNAME, obj[CONSTANT.TABLE.KEY_NAME])
                    add = add.replace(CONSTANT.SERVICE.PLACEHOLDER.TABLENAME_GENERATOR, tablename)\
                                .replace(CONSTANT.SERVICE.PLACEHOLDER.CONSTRAINT_NAME, obj[CONSTANT.TABLE.KEY_NAME])\
                                .replace(CONSTANT.SERVICE.PLACEHOLDER.COLUMN_NAME,obj[CONSTANT.TABLE.COLUMN_NAME])\
                             .replace(CONSTANT.SERVICE.PLACEHOLDER.REFERENCE_TABLE_NAME, obj[CONSTANT.TABLE.REFERENCE_TABLE_NAME])\
                             .replace(CONSTANT.SERVICE.PLACEHOLDER.REFERENCE_COLUMN_NAME,obj[CONSTANT.TABLE.REFERENCE_COLUMN_NAME])
                    arr.append((drop, add))
            else:
                pass

        return arr

    def getDropTableNames(self,sql_database):
        drop =  [table[CONSTANT.TABLE.TABLE_NAME] for table in self.drop if table[CONSTANT.SERVICE.ACTION.CAN_PERFORM_ACTION] == CONSTANT.SERVICE.STATE.TRUE]

        validated_drop_array = self.validateDropTablesNamesFromDatabase(drop,sql_database)

        return validated_drop_array

    def generateDropQueries(self,con,action,drop_tablenames,sql_database):
        drop_query = self.getQuery(con,STATEMENT_TYPE.DROP_TABLE_STATEMENT,sql_database)

        dp = ''
        for query in drop_query:
            dp += query

        drop_tables_queries = []

        for tablename in drop_tablenames:

            if action == CONSTANT.SERVICE.ACTION.DROP:
                drop_tables_queries.append(dp.replace(CONSTANT.SERVICE.PLACEHOLDER.TABLENAME_GENERATOR, tablename))
            else:
                if self.common[tablename][CONSTANT.SERVICE.ACTION.REPLACE][CONSTANT.SERVICE.ACTION.CAN_PERFORM_ACTION] == CONSTANT.SERVICE.STATE.TRUE:
                    drop_tables_queries.append(dp.replace(CONSTANT.SERVICE.PLACEHOLDER.TABLENAME_GENERATOR, tablename))

        return drop_tables_queries

    def generateDropQuery(self,con,tablename,sql_database):
        drop_query = self.getQuery(con,STATEMENT_TYPE.DROP_TABLE_STATEMENT,sql_database)

        if len(drop_query) == 1:
            dp = drop_query[0]

            drop_query = dp.replace(CONSTANT.SERVICE.PLACEHOLDER.TABLENAME_GENERATOR,tablename)
            return drop_query
        return

    def performSpecificAction(self,con,common_tables,sql_database):
        queries = []

        common_tablenames = common_tables

        common_tables = self.common

        for tablename in common_tablenames:

            table = common_tables[tablename]
            action = table[CONSTANT.TABLE.ACTION]
            canPerformAction = table[CONSTANT.SERVICE.ACTION.CAN_PERFORM_ACTION]

            if canPerformAction == CONSTANT.SERVICE.STATE.TRUE:
                table = table[action]
                if action == CONSTANT.SERVICE.ACTION.REPLACE:
                    drop_queries = self.generateDropQuery(con,tablename,sql_database)
                    queries.append(drop_queries)

                    self.createIndividualTableCreateQueryPart(con,table,sql_database,queries)

                    foreign_keys_for_replace_table = self.createForeignKeyForTable(con,table,sql_database)
                    unique_keys_for_replace_table = self.createUniqueKeyForTable(con, table, sql_database)
                    indexes_for_replace_table = self.createIndexesForTable(con, table, sql_database)

                    queries = queries + foreign_keys_for_replace_table + unique_keys_for_replace_table

                    # add_indexes_for_replace_table = []
                    for drop  in indexes_for_replace_table[1]:
                        queries.append(drop)

                    for add  in indexes_for_replace_table[0]:
                        queries.append(add)

                    # print(queries)
                elif action == CONSTANT.SERVICE.ACTION.UPDATE:
                    self.get_update_table_queries(con, table, queries, sql_database)

        return queries

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

            dynamicPrinter('Database: Postgres \nStatus: Connected')
            return con
        except Exception as e:
            dynamicPrinter(f'Exception: {e}')
            return

    def getQuery(self,con,statement_type,sql_database):
        statement_type_str = str(statement_type)
        if db_queries.get(statement_type_str):
           return db_queries[statement_type_str]
        else:
            bean  = DatabaseEngineGeneratorBean()
            query = bean.fetchingQuery(con,statement_type_str,sql_database)

            db_queries[statement_type_str] = query
            db_queries.update({
                f'{statement_type_str}': query
            })
            return db_queries[statement_type_str]

    def validateDropTablesNamesFromDatabase(self,tablenames,sql_database):
        arr = []
        # print(tablenames)
        bean = DatabaseEngineBean(self.cred.sql_database)
        bean.fetchingTable(self.con,self.cred.database,arr,sql_database)

        validated_arr = [table[CONSTANT.TABLE.TABLE_NAME] for table in arr if table[CONSTANT.TABLE.TABLE_NAME]  in tablenames ]

        return validated_arr

    def validateAddTablesNamesFromDatabase(self,tablenames):

        arr = []

        bean = DatabaseEngineBean(self.cred.sql_database)
        bean.fetchingTable(self.con, self.cred.database, arr, self.cred.sql_database)

        validated_arr = []
        is_present = False

        for tablename in tablenames:
            for present_table in arr:
                if tablename[CONSTANT.TABLE.TABLE_NAME] == present_table[CONSTANT.TABLE.TABLE_NAME]:

                    is_present = True
                    continue
                else:

                    is_present = False

            if is_present == False:
                validated_arr.append(tablename)
            is_present = False

        return validated_arr

    def validateCommonTablesNamesFromDatabase(self,tablenames):
        arr = []

        bean = DatabaseEngineBean(self.cred.sql_database)
        bean.fetchingTable(self.con,
                           self.cred.database,
                           arr,
                           self.cred.sql_database)

        validated_arr = []
        is_present = False

        for tablename in tablenames:
            for present_table in arr:
                if tablename in present_table[CONSTANT.TABLE.TABLE_NAME]:

                    is_present = True
                    break

            if is_present == True:
                validated_arr.append(tablename)
            is_present = False
        return validated_arr

    def getDatatypeInfo(self,database,datatype):

        if datatypes.get(datatype):
            return datatypes[datatype]
        else:
            mapper = DatabaseEngineGeneratorBean()
            datatypeInfo = mapper.getDatatypeInfo(database, datatype)

            datatypes[datatype] = datatypeInfo
            datatypes.update({
                f'{datatype}':datatypeInfo
            })

            return datatypes[datatype]

    def executeQueries(self, con, query):
        if len(query) != 0:
            try:
                cur = con.cursor()
                cur.execute(query)
                cur.close()
                con.commit()
                dynamicPrinter(f'Query Executed: {query}')

            except Exception as e:

                cur = con.cursor()
                cur.execute(CONSTANT.SERVICE.KEYWORD.ROLL_BACK)

                cur.close()
                con.commit()
