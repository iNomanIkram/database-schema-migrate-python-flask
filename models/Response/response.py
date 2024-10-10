
import json

class ResponseModel:
    status = None
    message = None
    data = None

    def __init__(self,status,message,data) :
        self.status = status
        self.message = message
        self.data = data

    def toJSON(self):
        dumps = self.toJSONString()
        return json.loads(dumps)

    def toJSONString(self):
        return json.dumps(self.__dict__)