from constants.CONSTANT import CONSTANT
# class TABLE:
#     INDEX_TYPE = 'indexType'
#     INDEX_NAME = 'indexName'
#     REFERENCE_COLUMN_NAME = 'ReferenceColumnName'
#     REFERENCE_TABLE_NAME = 'ReferenceTableName'
#     KEY_NAME = 'keyName'
#     NAME = 'columnName'
#     CONSTRAINT_NAME = 'constraintName'
#     UNQIUE_KEY = 'unqiueKey'
#     FOREIGN_KEY = 'foreignKey'
#     PRIMARY_KEY = 'primaryKey'
#     LENGTH = 'length'
#     DEFAULT_VALUE = 'defaultValue'
#     AUTO_INCREMENT = 'autoIncrement'
#     NULL_CONSTRAINT = 'nullConstraint'
#     DATA_TYPE = 'dataType'
#     COLUMN_NAME = 'columnName'
#     INDEXES = 'indexes'
#     CONSTRAINTS = 'constraints'
#     COLUMNS = 'columns'
#     SCHEMA = 'schema'
#     OWNER = 'owner'
#     COLLATION = 'collation'
#     CHARACTER_SET = 'characterSet'
#     STORAGE_ENGINE = 'storageEngine'
#     COMMENTS = 'comments'
#     ACTION = 'action'
#     TABLE_NAME = 'tableName'


class Schema:

    def __init__(self,json):
        arr = []
        for table in json:
            t = Table(table)
            arr.append(t)
        self.tableInfo = arr

class DatabaseInfo:

    def __init__(self,json):
        self.hostname = json[CONSTANT.SERVICE.CREDIONTIAL.HOSTNAME]
        self.port = json[CONSTANT.SERVICE.CREDIONTIAL.PORT_NUMBER]
        self.username = json[CONSTANT.SERVICE.CREDIONTIAL.USERNAME]
        self.password = json[CONSTANT.SERVICE.CREDIONTIAL.PASSWORD]
        self.databaseName = json[CONSTANT.SERVICE.CREDIONTIAL.DATABASE_NAME]
        self.databaseId = json[CONSTANT.SERVICE.CREDIONTIAL.DATABASE_ID]

class Table:

    def __init__(self,json):
        self.tablename = json[CONSTANT.TABLE.TABLE_NAME]
        self.action = json[CONSTANT.TABLE.ACTION]
        self.canPerformAction = ''
        self.comments = json[CONSTANT.TABLE.COMMENTS]
        self.storageEngine = json[CONSTANT.TABLE.STORAGE_ENGINE]
        self.characterSet = json[CONSTANT.TABLE.CHARACTER_SET]
        self.collation = json[CONSTANT.TABLE.COLLATION]
        self.owner = json[CONSTANT.TABLE.OWNER]
        self.schema = json[CONSTANT.TABLE.SCHEMA]

        columns = json[CONSTANT.TABLE.COLUMNS]
        arr = []
        for column in columns:
            col = Column(column)
            arr.append(col)
        self.columns = arr

        constraints = json[CONSTANT.TABLE.CONSTRAINTS]

        self.constraints = constraints

        indexes = json[CONSTANT.TABLE.INDEXES]
        arr = []
        for index in indexes:
            indx = Index(index)
            arr.append(indx)
        self.indexes = arr

class Column:

    def __init__(self,json):
        self.columnName = json[CONSTANT.TABLE.COLUMN_NAME]
        self.datatype = json[CONSTANT.TABLE.DATA_TYPE]
        self.nullConstraint = json[CONSTANT.TABLE.NULL_CONSTRAINT]
        self.autoIncrement = json[CONSTANT.TABLE.AUTO_INCREMENT]
        self.defaultValue = DefaultValue(json[CONSTANT.TABLE.DEFAULT_VALUE])
        self.length = json[CONSTANT.TABLE.LENGTH]

class DefaultValue:

    def __init__(self,json):
        self.constraintName = json[CONSTANT.TABLE.CONSTRAINT_NAME]
        self.defaultValue = json[CONSTANT.TABLE.DEFAULT_VALUE]

class Constraint:

    def __init__(self,json):
        primaries = json[CONSTANT.TABLE.PRIMARY_KEY]
        pri_arr = []
        for primary in primaries:
            p = PrimaryKey(primary)
            pri_arr.append(p)

        foreigns = json[CONSTANT.TABLE.FOREIGN_KEY]
        for_arr = []
        for foreign in foreigns:
            p = ForeignKey(foreign)
            for_arr.append(p)

        uniques = json[CONSTANT.TABLE.UNQIUE_KEY]
        uni_arr = []
        for unique in uniques:
            p = UniqueKey(unique)
            uni_arr.append(p)


        self.primaryKey = pri_arr
        self.foreignKey = for_arr
        self.unqiueKey = uni_arr


class PrimaryKey:
    def __init__(self,json):
        self.constraintName = json[CONSTANT.TABLE.CONSTRAINT_NAME]
        self.columnName = json[CONSTANT.TABLE.COLUMN_NAME]

class ForeignKey:
    def __init__(self,json):
        self.keyName = json[CONSTANT.TABLE.KEY_NAME]
        self.columnName = json[CONSTANT.TABLE.COLUMN_NAME]
        self.ReferenceTableName = json[CONSTANT.TABLE.REFERENCE_TABLE_NAME]
        self.ReferenceColumnName = json[CONSTANT.TABLE.REFERENCE_COLUMN_NAME]

class UniqueKey:
    def __init__(self,json):
        self.constraintName = json[CONSTANT.TABLE.CONSTRAINT_NAME]
        self.columnName = json[CONSTANT.TABLE.COLUMN_NAME]

class Index:
    def __init__(self,json):
        self.action = json[CONSTANT.TABLE.ACTION]
        self.indexName = json[CONSTANT.TABLE.INDEX_NAME]
        self.indexType = json[CONSTANT.TABLE.INDEX_TYPE]
        self.columnName = json[CONSTANT.TABLE.COLUMN_NAME]