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


class DatabaseEngineService(Resource):
    def post(self):
        # global variable to make it aviable globally
        global instance, response, con

        #   parsing arguments
        data = self.parse_arguments()

        
        # getting data from databaseInfo,tableInfo key to Dict form
        databaseInfo = eval(data.get(CONSTANT.SERVICE.PARAMETERS.DATABASE_INFO))
        comparison = data.get(CONSTANT.SERVICE.PARAMETERS.COMPARISON)
        action = databaseInfo[CONSTANT.SERVICE.PARAMETERS.ACTION]

        if action == None:
            response = ResponseModel(404, CONSTANT.SERVICE.PARAMETERS.ACTION_NOT_SPECIFIED, '')
            return {CONSTANT.DATATYPE.JSON: response.toJSON()}, response.status
        if comparison == None or comparison == '':
            return {CONSTANT.DATATYPE.JSON: response.toJSON()}, response.status


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
            # if con is None
            response = ResponseModel(404, CONSTANT.SERVICE.EXCEPTION.CONNECTION_FAILED, '')
            return {CONSTANT.DATATYPE.JSON: response.toJSON()}, response.status
        try:

            actions = [CONSTANT.SERVICE.ACTION.DROP, CONSTANT.SERVICE.ACTION.ADD, CONSTANT.SERVICE.ACTION.UPDATE, CONSTANT.SERVICE.ACTION.REPLACE, CONSTANT.SERVICE.ACTION.CUSTOM]


            ##########################




            ##########################
            if action not in actions:
                response = ResponseModel(404, CONSTANT.SERVICE.EXCEPTION.INVALID_ACTION_PROVIDED, '')
                return {CONSTANT.DATATYPE.JSON: response.toJSON()}, response.status

            generator = DatabaseQueryGenerator()
            generator.getInfo(instance.getConnection(),action, cred, comparison, cred.sql_database)

            if action == CONSTANT.SERVICE.ACTION.DROP or action == CONSTANT.SERVICE.ACTION.CUSTOM:

                generator.fetchDropQueries(cred.sql_database)

                generator.executeRemoveConstraintBeforeDropping()
                generator.executeDropQueries()
                # pass
            if action == CONSTANT.SERVICE.ACTION.ADD or action == CONSTANT.SERVICE.ACTION.CUSTOM:
                generator.fetchingAddQueries(cred.sql_database)
                generator.executeAddQueries()
                # pass
            if action == CONSTANT.SERVICE.ACTION.REPLACE or action == CONSTANT.SERVICE.ACTION.UPDATE or action == CONSTANT.SERVICE.ACTION.CUSTOM:

                generator.fetchingCommonQueries(action,cred.sql_database)
                generator.executePairOfConstraintForCommonTable()

                if   action == CONSTANT.SERVICE.ACTION.REPLACE :
                    # generator.fet
                    generator.executeDropTableBeforeReplace()
                    generator.executeReplaceAllTableQueries()
                    # pass
                elif action == CONSTANT.SERVICE.ACTION.UPDATE:
                    generator.executeUpdateAllQueries()
                    # pass
                elif action == CONSTANT.SERVICE.ACTION.CUSTOM:
                    generator.executeCustomQueries()
                    # pass
                generator.executeCreateConstraintAgain()

            response = ResponseModel(200,CONSTANT.SERVICE.RESPONSE.SUCCESSFULLY_GENERATED, '')
            return {CONSTANT.DATATYPE.JSON: response.toJSON()}, response.status
        except:
            traceback.print_exc()
            response = ResponseModel(404, CONSTANT.SERVICE.EXCEPTION.FAILED_DURING_QUERY_GENERATION, '')
            return {CONSTANT.DATATYPE.JSON: response.toJSON()}, response.status


    def parse_arguments(self):
        parser = reqparse.RequestParser()
        parser.add_argument(CONSTANT.SERVICE.PARAMETERS.DATABASE_INFO,

                            required=True,
                            help=CONSTANT.SERVICE.EXCEPTION.DATABASE_PARM_CAN_NOT_BE_EMPTY)
        parser.add_argument(CONSTANT.SERVICE.PARAMETERS.COMPARISON,

                            required=True,
                            help=CONSTANT.SERVICE.EXCEPTION.COMPARISON_PARM_NOT_SPECIFIED)
        data = parser.parse_args()
        return data

    def performMappingSpecificToDatabase(self, cred, mapper):

        mapper.datatype_mapping(cred.sql_database)