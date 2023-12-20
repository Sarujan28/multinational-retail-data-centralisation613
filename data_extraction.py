from database_utils import DatabaseConnecter
import pandas as pd
import tabula
from tabula.io import read_pdf
import requests
import boto3

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

    def retrive_stores_data(self, retrive_endpoint, headers):
        store_number = 0
        list_of_store_data = []
        self.retrive_endpoint = retrive_endpoint
        self.headers = headers
        while store_number in range(0,451):
            response = requests.get(retrive_endpoint + f'{store_number}', headers = headers)
            store_data = response.json()
            list_of_store_data.append(store_data)
            store_number += 1
        stores_data = pd.DataFrame(list_of_store_data)
        stores_data.set_index('index', inplace= True)
        self.stores_data = stores_data
        print(stores_data)

    def extract_from_s3(self, address):
        self.address = address
        bucket_name = ''
        object_key = ''
        if 'https' in address:
            components = address.split('/')
            region = components[2]
            region_components = region.split('.')
            bucket_name = region_components[0]
            object_key = components[3]
        else:
            components = address.split('/')
            bucket_name = components[2]
            object_key = components[3]
        print(bucket_name)
        print(object_key)
        s3 = boto3.client('s3')
        s3.download_file(bucket_name, object_key, f'/Users/saruj/Downloads/multinational-retail-data-centralisation613/{object_key}')
        dataframe = pd.DataFrame([])
        if '.csv' in object_key:
            dataframe = pd.read_csv(f'/Users/saruj/Downloads/multinational-retail-data-centralisation613/{object_key}', index_col = 0)
        elif '.json' in object_key:
            dataframe = pd.read_json(f'/Users/saruj/Downloads/multinational-retail-data-centralisation613/{object_key}')
        self.dataframe = dataframe
        dataframe.info()
        print(dataframe)
        return dataframe