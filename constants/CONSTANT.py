
# class SQLDatabase:
#     POSTGRES = 1    #'postgres'
#     ORACLE = 2      #'oracle'
#     MSSQLSERVER = 3 #'mssqlserver'
#     MYSQL = 4       #'mysql'

class SQLDatabase:
    POSTGRES = 1
    MYSQL     = 2
    MSSQLSERVER = 3
    ORACLE = 4



class VIEW_QUERY_TYPE:
    FETCH_ALL_TABLENAMES = 1
    FETCH_TABLE_SCHEMA = 2
    FETCH_PRIMARY_KEY = 3
    FETCH_FOREIGN_KEYS = 4
    FETCH_UNIQUE_KEYS = 5
    FETCH_INDEXES = 6

class CONSTANT:
    RESOURCE = {r"*": {"origins": "*"}}
    EXCEPTION = 'exception'
    UTF8 = 'utf8'
    UTF8_GENERAL_CI ='utf8_general_ci'

    class SCHEMA_KEY:
        TABLENAMES = 'tablenames'
        PRIMARY = 'primary'
        FOREIGN = 'foreign'
        UNIQUE = 'unique'
        INDEX = 'index'

    class QUERY_DATABASE_CRED:
        USERNAME  = 'postgres'
        PASSWORD = '.'
        HOST =  'localhost'
        DATABASE =  'integration_db'
        PORT = 1433

    # class QUERY_DATABASE_CRED:
    #     USERNAME = 'dev_user'
    #     PASSWORD = 'devuser'
    #     HOST = '192.168.2.25'
    #     DATABASE = 'integration_db'
    #     PORT = 5432

    class CONNECTION_MESSAGE:
        SQL_SERVER = 'Database: SQL SERVER \nStatus: Connected'
        ORACLE = 'Database: ORACLE \nStatus: Connected'
        MYSQL = 'Database: MYSQL \nStatus: Connected'
        POSTGRES = 'Database: POSTGRES \nStatus: Connected'

    class SQL_SERVER:
        DRIVER_STRING = '{ODBC Driver 17 for SQL Server}'
        DRIVER = 'DRIVER='
        SERVER = ';SERVER='
        PORT = ';PORT='
        DATABASE = ';DATABASE='
        UID = ';UID='
        PASSWORD = ';PWD='

    class ORACLE:
        DEFAULT_PORT = '1521'

    class MAPPER:
        class DATATYPE:
            CHARACTER_VARYING =  'CHARACTER VARYING'
            VARCHAR='VARCHAR'
            CHARACTER = 'CHARACTER'
            CHAR = 'CHAR'

    class DATATYPE:
        JSON = 'json'

    class TABLE:
        INDEX_TYPE = 'indexType'
        INDEX_NAME = 'indexName'
        REFERENCE_COLUMN_NAME = 'ReferenceColumnName'
        REFERENCE_TABLE_NAME = 'ReferenceTableName'
        KEY_NAME = 'keyName'
        CONSTRAINT_NAME = 'constraintName'
        UNIQUE_KEY = 'uniqueKey'
        FOREIGN_KEY = 'foreignKey'
        PRIMARY_KEY = 'primaryKey'
        LENGTH = 'length'
        DEFAULT_VALUE = 'defaultValue'
        AUTO_INCREMENT = 'autoIncrement'
        NULL_CONSTRAINT = 'nullConstraint'
        DATA_TYPE = 'datatype'
        COLUMN_NAME = 'columnName'
        INDEXES = 'indexes'
        CONSTRAINTS = 'constraints'
        COLUMNS = 'columns'
        SCHEMA = 'schema'
        OWNER = 'owner'
        COLLATION = 'collation'
        CHARACTER_SET = 'characterSet'
        STORAGE_ENGINE = 'storageEngine'
        COMMENTS = 'comments'
        ACTION = 'action'
        TABLE_NAME = 'tablename'

    class SERVICE:
        SAMPLE = 'sample'

        class MAPPER:
            FAMILY = 'family'
            HAS_LENGTH = 'haslength'
            PRIORITY = 'priority'

            class DATATYPE:
                TEXT = 'text'

        class PARAMETERS:
            DATABASE_INFO = 'databaseInfo'
            TABLE_INFO    = 'tableInfo'
            COMPARISON = 'comparison'
            ACTION = 'action'
            PREDEFINED_SCHEMA = 'predefinedSchema'

        class RESPONSE:
            SUCCESSFUL_COMPARED = 'Successful Compared'
            SUCCESSFULLY_GENERATED = 'successfully generated'

        class ACTION:
            ADD = 'add'
            DROP = 'drop'
            REPLACE = 'replace'
            UPDATE = 'update'
            CUSTOM ='custom'
            COMMON = 'common'

            # ADD = 'add'
            # DROP = 'drop'
            # REPLACE = 'commonreplace'
            # UPDATE = 'commonalter'
            # CUSTOM = 'custom'
            # COMMON = 'common'

            NEXT_VAL = 'nextval'
            CAN_PERFORM_ACTION = 'canPerformAction'

        class STATE:
            OLD = 'old'
            TRUE = 'true'
            FALSE = 'false'
            N = 'N'
            NO = 'NO'
            YES = 'yes'
            NO_LOWERCASE = 'no'
            ONE = '1'

        class KEYWORD:
            NOT_NULL = 'NOT NULL'
            NULL = 'NULL'
            PRIMARY = 'PRIMARY'
            AUTO_INCREMENT = 'AUTO_INCREMENT'
            ROLL_BACK = 'ROLLBACK'
            SERIAL = 'SERIAL'
            AUTO_INCREMENT_LOWERCASE = "auto_increment"

        class EXCEPTION:
            CONNECTION_FAILED  = 'Connection Failed'
            NOTHING_TO_COMPARE ='Nothing to Compare'
            SCHEMA_IS_EMPTY    = 'Schema is empty'
            NOTHING_TO_COMPARED = 'Nothing to Compare'
            SCHEMA_IS_EMPTY = 'Schema is empty'
            DATABASE_PARM_CAN_NOT_BE_EMPTY = 'databaseInfo parm can not be empty'
            TABLEINFO_PARM_CAN_NOT_BE_EMPTY = 'tableInfo parm can not be empty'
            TABLE_INFO_CANNOT_BE_EMPTY = 'tableInfo can not be empty'
            ACTION_NOT_SPECIFIED = 'action not specified'
            INVALID_ACTION_PROVIDED = 'invalid action provided'
            FAILED_DURING_QUERY_GENERATION = 'failure during query generation'
            COMPARISON_PARM_NOT_SPECIFIED = 'comparison parm can not be empty'

        class CREDIONTIAL:
            USERNAME = 'dbUserName'
            PASSWORD = 'dbPassword'
            HOSTNAME = 'hostname'
            DATABASE_NAME = 'dbName'
            DATABASE_ID = 'databaseId'
            PORT_NUMBER = 'portNumber'

        class PLACEHOLDER:
            TABLENAME_SCHEMA = '##tablename@@##'
            TABLENAME_GENERATOR = '##tableName@@##'
            SCHEMA = '##schema@@##'
            DATABASE = '##database@@##'
            CONSTRAINT_NAME = '##constraintName@@##'
            COLUMN_NAME = '##columnName@@##'
            REFERENCE_TABLE_NAME = '##referenceTableName@@##'
            REFERENCE_COLUMN_NAME = '##referenceColumnName@@##'
            INDEX_NAME = '##indexName@@##'
            # INDEX_TYPE
            DEFAULT_VALUE =  "##defaultValue@@##"
            LENGTH  =  '##length@@##'
            DATATYPE = '##datatype@@##'
            KEYNAME = '##keyName@@##'
            NULL_CONSTRAINT = '##nullConstraint@@##'
            AUTO_INCREMENT = '##autoIncrement@@##'

class STATEMENT_PART_TYPE:

    CREATE_TABLE = 1
    ADD_COLUMN = 2
    ADD_NULL_CONSTRAINT = 3
    ADD_AUTO_INCREMENT = 4
    ADD_DEFAULT_VALUE = 5
    ADD_UNIQUE_KEY = 6
    ADD_PRIMARY_KEY = 7
    ADD_FOREIGN_KEY = 8
    DROP_TABLE = 9

    ADD_COLUMN_STATEMENT = 10
    DROP_COLUMN_STATEMENT = 11

    CHANGING_DATATYPE = 12

    ADD_UNIQUE_KEY_CONSTRAINT_STATEMENT = 13
    DROP_UNIQUE_KEY_CONSTRAINT_STATEMENT = 14

    ADD_DEFAULT_VALUE_STATEMENT = 15
    DROP_DEFAULT_VALUE_STATEMENT = 16

    ADD_PRIMARY_KEY_STATEMENT = 17
    DROP_PRIMARY_KEY_STATEMENT = 18

    ADD_FOREIGN_KEY_STATEMENT = 19
    DROP_FOREIGN_KEY_STATEMENT = 20

    CREATE_INDEX_STATEMENT = 21
    DROP_INDEX_STATEMENT = 22

    ADD_NULL_CONSTRAINT_STATEMENT = 23
    DROP_NULL_CONSTRAINT_STATEMENT = 24

class STATEMENT_TYPE:
    CREATE_TABLE_STATEMENT = 1
    DROP_TABLE_STATEMENT = 2

    ADD_COLUMN_STATEMENT= 3
    DROP_COLUMN_STATEMENT = 4

    CHANGING_DATATYPE = 5

    ADD_UNIQUE_KEY_CONSTRAINT_STATEMENT = 6
    DROP_UNIQUE_KEY_CONSTRAINT_STATEMENT = 7

    ADD_DEFAULY_VALUE_STATEMENT = 8
    DROP_DEFAULT_VALUE_STATEMENT = 9

    ADD_PRIMARY_KEY_CONSTRAINT_STATEMENT = 10
    DROP_PRIMARY_KEY_CONSTRAINT_STATEMENT = 11

    ADD_FOREIGN_KEY_CONSTRAINT_STATEMENT = 12
    DROP_FOREIGN_KEY_CONSTRAINT_STATEMENT = 13

    CREATE_INDEX_STATEMENT = 14
    DROP_INDEX_STATEMENT = 15

    ADD_NULL_STATEMENT = 16
    DROP_NULL_STATEMENT = 17


