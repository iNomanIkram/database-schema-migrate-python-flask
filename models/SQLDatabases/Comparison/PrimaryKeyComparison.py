from constants.CONSTANT import CONSTANT


class PrimaryKeyComparison:

    def __init__(self,col_a,col_b):

        self.col_a = col_a # new constraint key
        self.col_b = col_b # old constraint key
        self.response  = self.compare() # comparing both keys

    def compare(self):
        changes = []

        for obj_1 in self.col_a:

            for obj_2 in self.col_b:
                json_for_old = {}
                json_for_new = {}

                # if the column name is same and values is updated, then execute statements
                # keep the record for old and new to perform update and drop operation
                if obj_1[CONSTANT.TABLE.COLUMN_NAME] == obj_2[CONSTANT.TABLE.COLUMN_NAME]:
                    if obj_1[CONSTANT.TABLE.CONSTRAINT_NAME] != obj_2[CONSTANT.TABLE.CONSTRAINT_NAME]:
                        json_for_new.update({CONSTANT.TABLE.CONSTRAINT_NAME:obj_1[CONSTANT.TABLE.CONSTRAINT_NAME]})
                        json_for_new.update({CONSTANT.TABLE.COLUMN_NAME: obj_1[CONSTANT.TABLE.COLUMN_NAME]})
                        json_for_new.update({CONSTANT.TABLE.ACTION:CONSTANT.SERVICE.ACTION.ADD})

                        json_for_old.update({CONSTANT.TABLE.CONSTRAINT_NAME: obj_2[CONSTANT.TABLE.CONSTRAINT_NAME]})
                        json_for_old.update({CONSTANT.TABLE.COLUMN_NAME: obj_2[CONSTANT.TABLE.COLUMN_NAME]})
                        json_for_old.update({CONSTANT.TABLE.ACTION: CONSTANT.SERVICE.ACTION.DROP})

                        changes.append(json_for_old)
                        changes.append(json_for_new)

        return changes