from flask_restful import Resource,Api
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


class DatabaseEngineCheckService(Resource):
    def post(self):
        # global variable to make it aviable globally
        global instance, response, con

        #   parsing arguments
        data = self.parse_arguments()

        # getting data from databaseInfo,tableInfo key to Dict form
        databaseInfo = eval(data.get(CONSTANT.SERVICE.PARAMETERS.DATABASE_INFO))
        tableInfo = data.get(CONSTANT.SERVICE.PARAMETERS.TABLE_INFO)
        # action = databaseInfo['action']
        
        # tableInfo parameter can not be empty
        if tableInfo == None or tableInfo == '':
            response = ResponseModel(404,CONSTANT.TABLE_INFO_CANNOT_BE_EMPTY, '')
            return {CONSTANT.DATATYPE.JSON: response.toJSON()}, response.status

        # saving in variable in dict form
        json = {
            CONSTANT.SERVICE.PARAMETERS.DATABASE_INFO: databaseInfo,
            CONSTANT.SERVICE.PARAMETERS.TABLE_INFO: js.loads(tableInfo)
        }

        # cred object declared and initialized with database credentials
        cred = DatabaseCredential(databaseInfo[CONSTANT.SERVICE.CREDIONTIAL.USERNAME], databaseInfo[CONSTANT.SERVICE.CREDIONTIAL.PASSWORD], databaseInfo[CONSTANT.SERVICE.CREDIONTIAL.HOSTNAME], databaseInfo[CONSTANT.SERVICE.CREDIONTIAL.DATABASE_NAME], databaseInfo[CONSTANT.SERVICE.CREDIONTIAL.DATABASE_ID], databaseInfo[CONSTANT.SERVICE.CREDIONTIAL.PORT_NUMBER])

        instance= None
        response = None

        try:

            # instance of  DatabaseEngine initialized with database credentials
            # fetching tables and table schemas
            instance = DatabaseEngine(cred)

            if instance.con == None:
                raise ConnectionError()

            # getting connection object
            con = instance.getConnection()

            dynamicPrinter("***********************************\n" +
                            "JSON FETCH FROM CONNECTED DATABASE \n" +
                            f"{instance.getJSON()}\n" +
                            "***********************************\n")

        except:
            response = ResponseModel(404, CONSTANT.SERVICE.EXCEPTION.CONNECTION_FAILED, '')
            return {CONSTANT.DATATYPE.JSON: response.toJSON()}, response.status

        # if the json is not empty
        if instance.getJSON() != None:

            # Mapper class is responsible for mapping datatypes
            mapper = Mapper(json)

            # In databaseInfo database is defined to map the tableInfo received to similar datatype
            self.performMappingSpecificToDatabase(cred, mapper)

            # Comparison class is responsible for the comparison of the two database schemas
            # where json[CONSTANT.SERVER.PARAMETERS.TABLE_INFO] is tables schemas provided by user
            # and instance.getJSON() is table schemas fetching by connecting with the database
            print(instance.getJSON())
            comparison = Comparison(json[CONSTANT.SERVICE.PARAMETERS.TABLE_INFO], instance.getJSON())
            dynamicPrinter("***********************************\n" +
                           "COMPARISON OF TWO DATABASES IN THE FORM OF JSON \n" +
                           f"{comparison.response}\n" +
                           "***********************************\n")

            # if the comparison response is not empty
            if comparison.response != None:
                traceback.print_exc()
                response = ResponseModel(200, CONSTANT.SERVICE.RESPONSE.SUCCESSFUL_COMPARED, comparison.response)
                return {CONSTANT.DATATYPE.JSON: response.toJSON()}, response.status
            else:
                # Response
                response = ResponseModel(200, CONSTANT.SERVICE.EXCEPTION.NOTHING_TO_COMPARED, '')
                return {CONSTANT.DATATYPE.JSON: response.toJSON()}, response.status
        else:
            # Response
            response = ResponseModel(200, CONSTANT.SERVICE.EXCEPTION.SCHEMA_IS_EMPTY, '')
            return {CONSTANT.DATATYPE.JSON: response.toJSON()}, response.status

    def parse_arguments(self):
        parser = reqparse.RequestParser()
        parser.add_argument(CONSTANT.SERVICE.PARAMETERS.DATABASE_INFO,

                            required=True,
                            help=CONSTANT.SERVICE.EXCEPTION.DATABASE_PARM_CAN_NOT_BE_EMPTY)
        parser.add_argument(CONSTANT.SERVICE.PARAMETERS.TABLE_INFO,
                            required=True,
                            help=CONSTANT.SERVICE.EXCEPTION.TABLEINFO_PARM_CAN_NOT_BE_EMPTY)
        data = parser.parse_args()
        return data

    def performMappingSpecificToDatabase(self, cred, mapper):

        mapper.datatype_mapping(cred.sql_database)