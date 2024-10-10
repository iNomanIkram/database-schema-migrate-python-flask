import json as js

from constants.CONSTANT import CONSTANT


class DefaultValueComparison:

    def __init__(self,a,b):
        self.key_a = a
        self.key_b = b
        self.response = self.compare()
        # print(f'response: {self.response}')

    def compare(self):

        json = {}

        if self.key_a[CONSTANT.TABLE.CONSTRAINT_NAME] != self.key_b[CONSTANT.TABLE.CONSTRAINT_NAME]:
            json.update({CONSTANT.TABLE.CONSTRAINT_NAME: self.key_a[CONSTANT.TABLE.CONSTRAINT_NAME]})
            json.update({CONSTANT.TABLE.DEFAULT_VALUE: self.key_a[CONSTANT.TABLE.DEFAULT_VALUE]})

        if self.key_a[CONSTANT.TABLE.DEFAULT_VALUE] != self.key_b[CONSTANT.TABLE.DEFAULT_VALUE]:
            json.update({CONSTANT.TABLE.DEFAULT_VALUE: self.key_a[CONSTANT.TABLE.DEFAULT_VALUE]})
            json.update({CONSTANT.TABLE.CONSTRAINT_NAME: self.key_a[CONSTANT.TABLE.CONSTRAINT_NAME]})

        # if there are no changes in the defaultValue then return the same
        if json == {}:
            return None
        else:
            return json
