from database_utils import DatabaseConnecter
import pandas as pd
import tabula
from tabula.io import read_pdf
import requests

class DataExtractor:
    def read_rds_table(self, table_name, database_connection):
        database_connection = DatabaseConnecter()
        self.database_connection = database_connection
        database_connection.init_db_engine()
        database_connection.list_db_tables()
        table = pd.read_sql(table_name, database_connection.engine)
        self.table = table
        print(table)

    def retrive_pdf_data(self, link):
        page_dataframes = tabula.io.read_pdf(link, pages = 'all')
        dataframe = pd.concat(page_dataframes, ignore_index = True)
        self.dataframe = dataframe
        print(dataframe)
        return dataframe

    def list_number_of_stores(self, endpoint, headers):
        response = requests.get(endpoint, headers = headers)
        number_of_stores = response.json()
        print(number_of_stores)