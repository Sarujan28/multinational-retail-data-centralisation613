import yaml
from sqlalchemy import create_engine

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