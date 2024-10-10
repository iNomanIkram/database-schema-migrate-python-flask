from constants.CONSTANT import CONSTANT


class ForeignKeyComparison:

    def __init__(self,col_a,col_b):
        self.col_a = col_a
        self.col_b = col_b

        self.response = self.compare()
        # print(self.response)

    # not tested, copied from primary
    def compare(self):

        changes = []

        for obj_1 in self.col_a:

            for obj_2 in self.col_b:

                json_for_old = {}
                json_for_new = {}

                # if the constraint name is same and column is not same, then execute statements
                # keep the record for old and new to perform update and drop operation
                if obj_1[CONSTANT.TABLE.KEY_NAME] == obj_2[CONSTANT.TABLE.KEY_NAME]:
                    if obj_1[CONSTANT.TABLE.COLUMN_NAME] != obj_2[CONSTANT.TABLE.COLUMN_NAME]:
                        json_for_old.update(obj_2)
                        json_for_old.update({CONSTANT.TABLE.ACTION: CONSTANT.SERVICE.ACTION.DROP})
                        changes.append(json_for_old)

                        json_for_new.update(obj_1)
                        json_for_new.update({CONSTANT.TABLE.ACTION: CONSTANT.SERVICE.ACTION.ADD})
                        changes.append(json_for_new)

        return changes

