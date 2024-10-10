from flask_restful import Resource
from flask_restful import reqparse
from constants.CONSTANT import CONSTANT
from models.DatabaseCredential import DatabaseCredential
from models.Response.response import ResponseModel
from models.SQLDatabases.DatabaseEngine.DatabaseEngine import DatabaseEngine

# from models.SQLDatabases.DatabaseEngine.DatabaseQueryGenerator import DatabaseQueryGenerator2
from modules.DynamicPrinter import dynamicPrinter, visible

class DatabaseStructureService(Resource):
    def post(self):
        # global variable to make it aviable globally
        global instance, response, con

        # parsing arguments
        data = self.parse_arguments()

        # getting data from databaseInfo,tableInfo key to Dict form
        databaseInfo = eval(data.get(CONSTANT.SERVICE.PARAMETERS.DATABASE_INFO))

        # cred object declared and initialized with database credentials
        cred = DatabaseCredential(databaseInfo[CONSTANT.SERVICE.CREDIONTIAL.USERNAME],
                                  databaseInfo[CONSTANT.SERVICE.CREDIONTIAL.PASSWORD],
                                  databaseInfo[CONSTANT.SERVICE.CREDIONTIAL.HOSTNAME],
                                  databaseInfo[CONSTANT.SERVICE.CREDIONTIAL.DATABASE_NAME],
                                  databaseInfo[CONSTANT.SERVICE.CREDIONTIAL.DATABASE_ID],
                                  databaseInfo[CONSTANT.SERVICE.CREDIONTIAL.PORT_NUMBER])

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

            dynamicPrinter("***********************************\n" +
                           "JSON FETCH FROM CONNECTED DATABASE \n" +
                           f"{instance.getJSON()}\n" +
                           "***********************************\n")
            response = ResponseModel(200, CONSTANT.SERVICE.RESPONSE.SUCCESSFULLY_GENERATED, instance.getJSON())
            return {CONSTANT.DATATYPE.JSON: response.toJSON()}, response.status

        except:
            response = ResponseModel(404, CONSTANT.SERVICE.EXCEPTION.CONNECTION_FAILED, '')
            return {CONSTANT.DATATYPE.JSON: response.toJSON()}, response.status

    def parse_arguments(self):
        parser = reqparse.RequestParser()
        parser.add_argument(CONSTANT.SERVICE.PARAMETERS.DATABASE_INFO,
                            required=True,
                            help=CONSTANT.SERVICE.EXCEPTION.DATABASE_PARM_CAN_NOT_BE_EMPTY)

        data = parser.parse_args()
        return data

