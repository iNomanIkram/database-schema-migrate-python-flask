from flask_restful import Resource, Api
from flask_restful import reqparse
from constants.CONSTANT import CONSTANT, SQLDatabase
from models.DatabaseCredential import DatabaseCredential
from models.Mapper.Mapper import Mapper
from models.Response.response import ResponseModel
from models.SQLDatabases.Comparison.Comparison import Comparison
from models.SQLDatabases.DatabaseEngine.DatabaseEngine import DatabaseEngine
from models.SQLDatabases.DatabaseEngine.DatabaseQueryGenerator import DatabaseQueryGenerator
import json as js
import traceback

# from models.SQLDatabases.DatabaseEngine.DatabaseQueryGenerator import DatabaseQueryGenerator2
from modules.DynamicPrinter import dynamicPrinter, visible


class DatabaseEngineGenerateTablesService(Resource):
    def post(self):
        # global variable to make it aviable globally
        global instance, response, con

        #   parsing arguments
        data = self.parse_arguments()

        # getting data from databaseInfo,tableInfo key to Dict form
        databaseInfo = eval(data.get(CONSTANT.SERVICE.PARAMETERS.DATABASE_INFO))
        tableInfo =    data.get(CONSTANT.SERVICE.PARAMETERS.TABLE_INFO)

        # tableInfo parameter can not be empty
        if tableInfo == None or tableInfo == '':
            response = ResponseModel(404, 'TABLE_INFO_CANNOT_BE_EMPTY', '')
            return {CONSTANT.DATATYPE.JSON: response.toJSON()}, response.status

        # cred object declared and initialized with database credentials
        cred = DatabaseCredential(databaseInfo[CONSTANT.SERVICE.CREDIONTIAL.USERNAME],
                                  databaseInfo[CONSTANT.SERVICE.CREDIONTIAL.PASSWORD],
                                  databaseInfo[CONSTANT.SERVICE.CREDIONTIAL.HOSTNAME],
                                  databaseInfo[CONSTANT.SERVICE.CREDIONTIAL.DATABASE_NAME],
                                  databaseInfo[CONSTANT.SERVICE.CREDIONTIAL.DATABASE_ID],
                                  databaseInfo[CONSTANT.SERVICE.CREDIONTIAL.PORT_NUMBER])

        json = {
            CONSTANT.SERVICE.PARAMETERS.DATABASE_INFO: databaseInfo,
            CONSTANT.SERVICE.PARAMETERS.TABLE_INFO: js.loads(tableInfo)
        }

        instance = None
        response = None

        try:

            # instance of  DatabaseEngine initialized with database credentials
            # fetching tables and table schemas
            instance = DatabaseEngine(cred)

            if instance.con == None:
                raise ConnectionError()

            # getting connection object
            con = instance.getConnection()

        except:
            response = ResponseModel(404, CONSTANT.SERVICE.EXCEPTION.CONNECTION_FAILED, '')
            return {CONSTANT.DATATYPE.JSON: response.toJSON()}, response.status
        try:

            generator = DatabaseQueryGenerator()
            generator.generateTables(con,json,cred.sql_database)

            response = ResponseModel(200, CONSTANT.SERVICE.RESPONSE.SUCCESSFULLY_GENERATED, '')
            return {CONSTANT.DATATYPE.JSON: response.toJSON()}, response.status
        except:
            # traceback.print_exc()
            response = ResponseModel(404, CONSTANT.SERVICE.EXCEPTION.FAILED_DURING_QUERY_GENERATION, '')
            return {CONSTANT.DATATYPE.JSON: response.toJSON()}, response.status

    def parse_arguments(self):
        parser = reqparse.RequestParser()
        parser.add_argument(CONSTANT.SERVICE.PARAMETERS.DATABASE_INFO,

                            required=True,
                            help=CONSTANT.SERVICE.EXCEPTION.DATABASE_PARM_CAN_NOT_BE_EMPTY)
        parser.add_argument(CONSTANT.SERVICE.PARAMETERS.TABLE_INFO,

                            required=True,
                            help=CONSTANT.SERVICE.EXCEPTION.COMPARISON_PARM_NOT_SPECIFIED)
        data = parser.parse_args()
        return data

    def performMappingSpecificToDatabase(self, cred, mapper):

        mapper.datatype_mapping(cred.sql_database)