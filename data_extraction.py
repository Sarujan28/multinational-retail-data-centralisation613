from database_utils import DatabaseConnecter
import pandas as pd

class DataExtractor:
    def read_rds_table(self, table_name, database_connection):
        database_connection = DatabaseConnecter()
        self.database_connection = database_connection
        database_connection.init_db_engine()
        database_connection.list_db_tables()
        table = pd.read_sql(table_name, database_connection.engine)
        self.table = table
        print(table)