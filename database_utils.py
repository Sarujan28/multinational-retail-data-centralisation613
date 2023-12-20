import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect

class DatabaseConnecter:
    def read_db_creds(self):
        with open('db_creds.yaml', 'r') as file:
            credentials = yaml.safe_load(file)
        self.credentials = credentials
        
        return credentials

    def init_db_engine(self):
        self.read_db_creds()
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        RDS_HOST = self.credentials['RDS_HOST']
        RDS_PASSWORD = self.credentials['RDS_PASSWORD']
        RDS_USER = self.credentials['RDS_USER']
        RDS_DATABASE = self.credentials['RDS_DATABASE']
        RDS_PORT = self.credentials['RDS_PORT']
        engine = create_engine(f'{DATABASE_TYPE}+{DBAPI}://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{RDS_DATABASE}')
        self.engine = engine

    def list_db_tables(self):
        inspector = inspect(self.engine)
        table_names = inspector.get_table_names()
        self.table_names = table_names
        print(table_names)

    def upload_to_db(self, clean_dataframe, sql_table,):
        from data_cleaning import DataCleaning
        cleaning = DataCleaning()
        self.cleaning = cleaning
        self.clean_dataframe = clean_dataframe
        table_upload = clean_dataframe
        self.table_upload = table_upload
        table_upload.to_csv('final_user.csv')
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = 'localhost'
        USER = 'postgres'
        PASSWORD = 'dragon'
        DATABASE = 'sales_data'
        PORT = 5432
        engine_2 = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        self.engine_2 = engine_2
        print(table_upload)
        table_upload.to_sql(sql_table, self.engine_2, if_exists = 'replace', index = False)