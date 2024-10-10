from constants.CONSTANT import CONSTANT
from models.SQLDatabases.Comparison.ColumnComparison import ColumnComparison
from models.SQLDatabases.Comparison.ConstraintComparison import ConstraintComparison
from models.SQLDatabases.Comparison.IndexComparison import IndexComparison
from models.Schema.Schema import Schema
import json, traceback


class Comparison:

    def __init__(self,tbl_a,tbl_b):
        self.tbl_a = None
        self.tbl_b = None
        self.response = None
        try:
            self.tbl_a = Schema(tbl_a).tableInfo
            self.tbl_b = Schema(tbl_b).tableInfo
            self.response = self.compare()
        except Exception as e:
            print(f'Comparison File\n{e}')
            traceback.print_exc()

    def compare(self):

        common = []
        drop   = []
        add    = []

        # for getting common and new tables
        for a in self.tbl_a:
            global found
            found = False

            modification = None
            for b in self.tbl_b:

                if a.tablename == b.tablename:



                    modification = {
                        # 'new': a
                        CONSTANT.SERVICE.ACTION.REPLACE: a
                        ,
                        CONSTANT.SERVICE.STATE.OLD: b
                    }
                    found = True
                else:#otherwise table is unqiue and new table present in
                    pass

            if found == True:

                common.append(modification)
                modification = None
            else:
                a.action = CONSTANT.SERVICE.ACTION.ADD
                a.canPerformAction = CONSTANT.SERVICE.STATE.TRUE
                add.append(a)

        # for figuring out dropped table
        # if table is not present in add list or common list then is left for drop list
        for tbl in self.tbl_b:
            isPresent = False
            for addTable in json.loads(self.convertSchemaObjectStructureToDict(add))[CONSTANT.SERVICE.SAMPLE]:
                if tbl.tablename == addTable[CONSTANT.TABLE.TABLE_NAME]:
                    isPresent = True
                    break

            if isPresent != True:
                for commonTable in json.loads(self.convertSchemaObjectStructureToDict(common))[CONSTANT.SERVICE.SAMPLE]:
                    # if tbl.tablename == commonTable['new'][CONSTANT.TABLE.TABLE_NAME]:
                    if tbl.tablename == commonTable[CONSTANT.SERVICE.ACTION.REPLACE][CONSTANT.TABLE.TABLE_NAME]:
                        isPresent = True
                        break

            if isPresent == False:
                tbl.action = CONSTANT.SERVICE.ACTION.DROP
                tbl.canPerformAction = CONSTANT.SERVICE.STATE.FALSE

                drop.append(tbl)

        # json includes data of tables to be added, common tables or tables to be dropped
        jsons = json.dumps({
            CONSTANT.SERVICE.ACTION.ADD: add,
            CONSTANT.SERVICE.ACTION.COMMON: common,
            CONSTANT.SERVICE.ACTION.DROP : drop
        }, default=lambda x: x.__dict__)

        ###########################
        # Comparison of JSON Schema
        ###########################

        temp = json.loads(jsons)
        # get change key value from compareColumn
        change = json.dumps(self.compareColumns(jsons)) # Changed from json.loads to dump on 25th sept , 6:00
        if change == 'null':
            change ='{}'
        # print(f'Majo {change}'+str(change))
        add = temp[CONSTANT.SERVICE.ACTION.ADD]
        common = temp[CONSTANT.SERVICE.ACTION.COMMON]
        drop = temp[CONSTANT.SERVICE.ACTION.DROP]


        for index in common:
            changes = self.compareColumnsForSingleTable(index)
            if len(changes) == 1:
                # index['changes'] = changes[0]
                index[CONSTANT.SERVICE.ACTION.UPDATE] = changes[0] # CHANGE 'changes' key in common to CONSTANT.SERVICE.ACTION.UPDATE

                changes[0][CONSTANT.TABLE.ACTION] = CONSTANT.SERVICE.ACTION.UPDATE
                changes[0][CONSTANT.SERVICE.ACTION.CAN_PERFORM_ACTION] = CONSTANT.SERVICE.STATE.TRUE


        dict_for_commons = {}
        for obj in common:
            # tablename = obj['new'][CONSTANT.TABLE.TABLE_NAME]
            replace = obj[CONSTANT.SERVICE.ACTION.REPLACE]
            replace[CONSTANT.TABLE.ACTION] = CONSTANT.SERVICE.ACTION.REPLACE
            replace[CONSTANT.SERVICE.ACTION.CAN_PERFORM_ACTION] = CONSTANT.SERVICE.STATE.TRUE
            tablename = replace[CONSTANT.TABLE.TABLE_NAME]
            del obj[CONSTANT.SERVICE.STATE.OLD]
            dict_for_commons.update({tablename:obj})


            # to add the choice to any of the table either to replace or update
            obj[CONSTANT.TABLE.ACTION] = CONSTANT.SERVICE.ACTION.UPDATE
            obj [CONSTANT.SERVICE.ACTION.CAN_PERFORM_ACTION] = CONSTANT.SERVICE.STATE.TRUE

        # print(dict_for_commons)

        jsonWithChange = {
            CONSTANT.SERVICE.ACTION.ADD: add,
            CONSTANT.SERVICE.ACTION.COMMON: dict_for_commons,#CONSTANT.SERVICE.ACTION.COMMON: common,
            CONSTANT.SERVICE.ACTION.DROP: drop
            # ,'change':json.loads(change)
        }

        return json.dumps(jsonWithChange)

    # Common key contains copy of new and old schema
    # new and old table schemas are compared to produce schema for changes (Update Schema)
    def compareColumns(self,jsons):

        # common key stored in common variable
        common = json.loads(jsons)[CONSTANT.SERVICE.ACTION.COMMON]

        arr = []
        # iterating over tables one by one
        for individual_table in common:

            # new and old copy of same table
            # new = individual_table['new']
            new = individual_table[CONSTANT.SERVICE.ACTION.REPLACE]
            old = individual_table[CONSTANT.SERVICE.STATE.OLD]

            changes = {}

            # adding tablename to the object
            # after that compare and watch if its changed then it is added to the object dict
            changes.update({CONSTANT.TABLE.TABLE_NAME:new[CONSTANT.TABLE.TABLE_NAME]})
            if new[CONSTANT.TABLE.STORAGE_ENGINE] != old[CONSTANT.TABLE.STORAGE_ENGINE]:
                changes.update({CONSTANT.TABLE.STORAGE_ENGINE: new[CONSTANT.TABLE.STORAGE_ENGINE]})

            if new[CONSTANT.TABLE.CHARACTER_SET] != old[CONSTANT.TABLE.CHARACTER_SET]:
                changes.update({CONSTANT.TABLE.CHARACTER_SET: new[CONSTANT.TABLE.CHARACTER_SET]})

            if new[CONSTANT.TABLE.COLLATION] != old[CONSTANT.TABLE.COLLATION]:
                changes.update({CONSTANT.TABLE.COLLATION: new[CONSTANT.TABLE.COLLATION]})

            if new[CONSTANT.TABLE.OWNER] != old[CONSTANT.TABLE.OWNER]:
                changes.update({CONSTANT.TABLE.OWNER: new[CONSTANT.TABLE.OWNER]})

            if new[CONSTANT.TABLE.SCHEMA] != old[CONSTANT.TABLE.SCHEMA]:
                changes.update({CONSTANT.TABLE.SCHEMA: new[CONSTANT.TABLE.SCHEMA]})

            # Comparison of Columns of new and old column of same schema
            columns = ColumnComparison(new[CONSTANT.TABLE.COLUMNS], old[CONSTANT.TABLE.COLUMNS])
            changes.update({CONSTANT.TABLE.COLUMNS: columns.responseJSON})

            # Comparison of Index of new and old column of same schema
            index = IndexComparison(new[CONSTANT.TABLE.INDEXES],old[CONSTANT.TABLE.INDEXES],new[CONSTANT.TABLE.TABLE_NAME])
            changes.update({CONSTANT.TABLE.INDEXES: index.responseJSON})

            # Comparison of Constraints of new and old column of same schema
            constraints = ConstraintComparison(new[CONSTANT.TABLE.CONSTRAINTS], old[CONSTANT.TABLE.CONSTRAINTS])
            changes.update({CONSTANT.TABLE.CONSTRAINTS: constraints.responseJSON})

            arr.append(changes)

            return arr

    def compareColumnsForSingleTable(self, index_of_table):

        # common key stored in common variable
        # common = json.loads(jsons)[CONSTANT.SERVICE.ACTION.COMMON]
        #
        arr = []
        # # iterating over tables one by one
        # for individual_table in common:

        # new and old copy of same table
        # new = index_of_table['new']
        new = index_of_table[CONSTANT.SERVICE.ACTION.REPLACE]
        old = index_of_table[CONSTANT.SERVICE.STATE.OLD]

        changes = {}

        # adding tablename to the object
        # after that compare and watch if its changed then it is added to the object dict
        changes.update({CONSTANT.TABLE.TABLE_NAME: new[CONSTANT.TABLE.TABLE_NAME]})
        if new[CONSTANT.TABLE.STORAGE_ENGINE] != old[CONSTANT.TABLE.STORAGE_ENGINE]:
            changes.update({CONSTANT.TABLE.STORAGE_ENGINE: new[CONSTANT.TABLE.STORAGE_ENGINE]})

        if new[CONSTANT.TABLE.CHARACTER_SET] != old[CONSTANT.TABLE.CHARACTER_SET]:
            changes.update({CONSTANT.TABLE.CHARACTER_SET: new[CONSTANT.TABLE.CHARACTER_SET]})

        if new[CONSTANT.TABLE.COLLATION] != old[CONSTANT.TABLE.COLLATION]:
            changes.update({CONSTANT.TABLE.COLLATION: new[CONSTANT.TABLE.COLLATION]})

        if new[CONSTANT.TABLE.OWNER] != old[CONSTANT.TABLE.OWNER]:
            changes.update({CONSTANT.TABLE.OWNER: new[CONSTANT.TABLE.OWNER]})

        if new[CONSTANT.TABLE.SCHEMA] != old[CONSTANT.TABLE.SCHEMA]:
            changes.update({CONSTANT.TABLE.SCHEMA: new[CONSTANT.TABLE.SCHEMA]})

        # Comparison of Columns of new and old column of same schema
        columns = ColumnComparison(new[CONSTANT.TABLE.COLUMNS], old[CONSTANT.TABLE.COLUMNS])
        changes.update({CONSTANT.TABLE.COLUMNS: columns.responseJSON})

        # Comparison of Index of new and old column of same schema
        index = IndexComparison(new[CONSTANT.TABLE.INDEXES], old[CONSTANT.TABLE.INDEXES], new[CONSTANT.TABLE.TABLE_NAME])
        changes.update({CONSTANT.TABLE.INDEXES: index.responseJSON})

        # Comparison of Constraints of new and old column of same schema
        constraints = ConstraintComparison(new[CONSTANT.TABLE.CONSTRAINTS], old[CONSTANT.TABLE.CONSTRAINTS])
        changes.update({CONSTANT.TABLE.CONSTRAINTS: constraints.responseJSON})

        arr.append(changes)

        return arr

    # to convert schema structure to dict
    def convertSchemaObjectStructureToDict(self,obj):
        return json.dumps({
            CONSTANT.SERVICE.SAMPLE: obj
        }, default=lambda x: x.__dict__)