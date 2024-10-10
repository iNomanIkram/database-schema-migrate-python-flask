from flask import Flask
from flask_restful import Api
from flask_cors import CORS
from constants.CONSTANT import CONSTANT
from services.DatabaseStructureService import DatabaseStructureService
from services.DatabaseEngineCheckService import DatabaseEngineCheckService
from services.DatabaseEngineService import DatabaseEngineService
from services.DatabaseEngineGenerateTablesService import DatabaseEngineGenerateTablesService

app = Flask(__name__)
cors = CORS(app, resources=CONSTANT.RESOURCE)
api = Api(app)

api.add_resource(DatabaseEngineCheckService,'/compareschema')#check
api.add_resource(DatabaseEngineService,'/alterdatabase')#database
api.add_resource(DatabaseStructureService,'/fetchschema') # structure
# api.add_resource(DatabaseEngineGenerateTablesService,'/generatedatabase')#generate

app.run(host='0.0.0.0',port=4401)