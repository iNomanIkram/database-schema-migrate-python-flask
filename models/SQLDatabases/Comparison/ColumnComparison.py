from constants.CONSTANT import CONSTANT
from models.SQLDatabases.Comparison.DefaultValueComparison import DefaultValueComparison
# from constants.CONSTANT import CONSTANT

class ColumnComparison:

    def __init__(self,column_a,column_b):
        self.col_a = column_a # containing list of columns provided in json schema
        self.col_b = column_b # containing list of columns in present in database
        self.responseJSON = self.compare()
        
    def compare(self):

        ca = []
        cb = []

        for a in self.col_a:
            if a not in ca:
                ca.append(a[CONSTANT.TABLE.COLUMN_NAME])
        for b in self.col_b:
            if b not in cb:
                cb.append(b[CONSTANT.TABLE.COLUMN_NAME])

        ca = set(ca)
        cb = set(cb)

        ad = list(ca.difference(cb)) # add
        dr = list(cb.difference(ca)) # drop
        cm = list(ca.intersection(cb)) # common

        changes = []

       # Compare which field to add ,delete and update
        for new_col in self.col_a:

            for old_col in self.col_b:
                json_for_old = {}
                json_for_new = {}

                # if the name of column in drop list then drop
                if old_col[CONSTANT.TABLE.COLUMN_NAME] in dr:

                    json_for_old.update({CONSTANT.TABLE.COLUMN_NAME:old_col[CONSTANT.TABLE.COLUMN_NAME]})
                    json_for_old.update({CONSTANT.TABLE.ACTION: CONSTANT.SERVICE.ACTION.DROP})

                    dr.remove(old_col[CONSTANT.TABLE.COLUMN_NAME])
                    changes.append(json_for_old)

                # if the name of column is common in both table
                #
                if new_col[CONSTANT.TABLE.COLUMN_NAME] in cm and new_col[CONSTANT.TABLE.COLUMN_NAME] == old_col[CONSTANT.TABLE.COLUMN_NAME]:

                    json_for_new.update({CONSTANT.TABLE.COLUMN_NAME:new_col[CONSTANT.TABLE.COLUMN_NAME]})
                    json_for_new.update({CONSTANT.TABLE.ACTION:CONSTANT.SERVICE.ACTION.UPDATE})
                    json_for_new.update({CONSTANT.TABLE.DATA_TYPE: new_col[CONSTANT.TABLE.DATA_TYPE]})#########

                    if new_col[CONSTANT.TABLE.DATA_TYPE] != old_col[CONSTANT.TABLE.DATA_TYPE]:
                        json_for_new.update({CONSTANT.TABLE.DATA_TYPE:new_col[CONSTANT.TABLE.DATA_TYPE]})
                        json_for_new.update({CONSTANT.TABLE.LENGTH: new_col[CONSTANT.TABLE.LENGTH]})

                    if new_col[CONSTANT.TABLE.LENGTH] != old_col[CONSTANT.TABLE.LENGTH]:
                        json_for_new.update({CONSTANT.TABLE.LENGTH:new_col[CONSTANT.TABLE.LENGTH]})
                        json_for_new.update({CONSTANT.TABLE.DATA_TYPE: new_col[CONSTANT.TABLE.DATA_TYPE]})

                    if new_col[CONSTANT.TABLE.NULL_CONSTRAINT] != old_col[CONSTANT.TABLE.NULL_CONSTRAINT]:
                        json_for_new.update({CONSTANT.TABLE.DATA_TYPE: new_col[CONSTANT.TABLE.DATA_TYPE]})
                        json_for_new.update({CONSTANT.TABLE.NULL_CONSTRAINT:new_col[CONSTANT.TABLE.NULL_CONSTRAINT]})

                    if new_col[CONSTANT.TABLE.AUTO_INCREMENT] != old_col[CONSTANT.TABLE.AUTO_INCREMENT]:
                        json_for_new.update({CONSTANT.TABLE.AUTO_INCREMENT:new_col[CONSTANT.TABLE.AUTO_INCREMENT]})


                    defaultValue = DefaultValueComparison(new_col[CONSTANT.TABLE.DEFAULT_VALUE], old_col[CONSTANT.TABLE.DEFAULT_VALUE])
                    json_for_new.update( {CONSTANT.TABLE.DEFAULT_VALUE: defaultValue.response })

                    cm.remove(new_col[CONSTANT.TABLE.COLUMN_NAME])
                    changes.append(json_for_new)


                if new_col[CONSTANT.TABLE.COLUMN_NAME] in ad:
                    json_for_new.update(new_col)
                    json_for_new.update({CONSTANT.TABLE.ACTION: CONSTANT.SERVICE.ACTION.ADD})
                    ad.remove(new_col[CONSTANT.TABLE.COLUMN_NAME])
                    # break
                    changes.append(json_for_new)

        return changes



