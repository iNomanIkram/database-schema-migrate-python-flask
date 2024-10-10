from constants.CONSTANT import CONSTANT


class IndexComparison:

    # additionally i need tablename in parms
    def __init__(self,column_a,column_b,tablename):
        self.column_a = column_a
        self.column_b = column_b
        self.tablename = tablename
        self.responseJSON = self.compare()


    def compare(self):

        new_index = []
        old_index = []

        for a in self.column_a:
            if a not in new_index:
                new_index.append(a[CONSTANT.TABLE.INDEX_NAME])
        for b in self.column_b:
            if b not in old_index:
                old_index.append(b[CONSTANT.TABLE.INDEX_NAME])

        new_index = set(new_index)
        old_index = set(old_index)

        ad = list(new_index.difference(old_index))  # add
        dr = list(old_index.difference(new_index))  # drop
        cm = list(new_index.intersection(old_index))  # common

        changes = []
        # iterating through indexes of column of table A
        for new_index in self.column_a:

            # iterating through indexes of column of table B
            for old_index in self.column_b:
                # print(old_index)
                json_for_old = {}
                json_for_new = {}

                # if index is in drop list then drop it
                if old_index[CONSTANT.TABLE.INDEX_NAME] in dr:
                    json_for_old.update(new_index)
                    json_for_old.update({CONSTANT.TABLE.ACTION:CONSTANT.SERVICE.ACTION.DROP})
                    json_for_old.update({CONSTANT.TABLE.TABLE_NAME:self.tablename})
                    dr.remove(old_index[CONSTANT.TABLE.INDEX_NAME])
                    changes.append(json_for_old)

                # if index is in common list then mention changes
                if new_index[CONSTANT.TABLE.INDEX_NAME] in cm and new_index[CONSTANT.TABLE.INDEX_NAME] == new_index[CONSTANT.TABLE.INDEX_NAME]:
                    json_for_new.update(new_index)
                    json_for_new.update({CONSTANT.TABLE.ACTION: CONSTANT.SERVICE.ACTION.UPDATE})
                    json_for_new.update({CONSTANT.TABLE.TABLE_NAME: self.tablename})
                    cm.remove(new_index[CONSTANT.TABLE.INDEX_NAME])
                    changes.append(json_for_new)

                # if index is in add list then add index info
                if new_index[CONSTANT.TABLE.INDEX_NAME] in ad:
                    json_for_new.update(new_index)
                    json_for_new.update({CONSTANT.TABLE.ACTION: CONSTANT.SERVICE.ACTION.ADD})
                    json_for_new.update({CONSTANT.TABLE.TABLE_NAME: self.tablename})
                    ad.remove(new_index[CONSTANT.TABLE.INDEX_NAME])
                    changes.append(json_for_new)

        return changes