
class DatabaseCredential:

    def __init__(self, username, password, hostname, database_name, sql_database, port=None):
        self.username = username
        self.password = password
        self.hostname = hostname
        self.port = port
        self.database = database_name
        self.sql_database = int(sql_database)




