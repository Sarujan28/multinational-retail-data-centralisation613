from data_extraction import DataExtractor
import pandas as pd
import numpy as np
from dateutil.parser import parse
import re

class DataCleaning:
    def clean_user_data(self, table_name, dataframe):
        dataframe = DataExtractor()
        self.dataframe = dataframe
        self.table_name = table_name
        dataframe.read_rds_table(table_name, dataframe)
        clean_table = dataframe.table
        self.clean_table = clean_table
        clean_table.info()
        clean_table.isna()

        clean_table.set_index('index', inplace= True)

        clean_table['phone_number'] = clean_table['phone_number'].str.strip()
        
        regex_expression = '^(?:(?:\(?(?:0(?:0|11)\)?[\s-]?\(?|\+)44\)?[\s-]?(?:\(?0\)?[\s-]?)?)|(?:\(?0))(?:(?:\d{5}\)?[\s-]?\d{4,5})|(?:\d{4}\)?[\s-]?(?:\d{5}|\d{3}[\s-]?\d{3}))|(?:\d{3}\)?[\s-]?\d{3}[\s-]?\d{3,4})|(?:\d{2}\)?[\s-]?\d{4}[\s-]?\d{4}))(?:[\s-]?(?:x|ext\.?|\#)\d{3,4})?$'
        clean_table.loc[~clean_table['phone_number'].str.match(regex_expression), 'phone_number'] = np.nan

        clean_table['first_name'] = clean_table['first_name'].replace('NULL', np.nan, regex = True)
        clean_table['last_name'] = clean_table['last_name'].replace('NULL', np.nan, regex = True)
        clean_table['date_of_birth'] = clean_table['date_of_birth'].replace('NULL', np.nan, regex = True)
        clean_table['company'] = clean_table['company'].replace('NULL', np.nan, regex = True)
        clean_table['email_address'] = clean_table['email_address'].replace('NULL', np.nan, regex = True)
        clean_table['address'] = clean_table['address'].replace('NULL', np.nan, regex = True)
        clean_table['country'] = clean_table['country'].replace('NULL', np.nan, regex = True)
        clean_table['country_code'] = clean_table['country_code'].replace('NULL', np.nan, regex = True)
        clean_table['phone_number'] = clean_table['phone_number'].replace('NULL', np.nan, regex = True)
        clean_table['join_date'] = clean_table['join_date'].replace('NULL', np.nan, regex = True)
        clean_table['user_uuid'] = clean_table['user_uuid'].replace('NULL', np.nan, regex = True)
        
        regex_expression_2 = '^[A-Z0-9]{10}$'
        clean_table['first_name'] = clean_table['first_name'].replace(regex_expression_2, np.nan, regex = True)
        clean_table['last_name'] = clean_table['last_name'].replace(regex_expression_2, np.nan, regex = True)
        clean_table['date_of_birth'] = clean_table['date_of_birth'].replace(regex_expression_2, np.nan, regex = True)
        clean_table['company'] = clean_table['company'].replace(regex_expression_2, np.nan, regex = True)
        clean_table['email_address'] = clean_table['email_address'].replace(regex_expression_2, np.nan, regex = True)
        clean_table['address'] = clean_table['address'].replace(regex_expression_2, np.nan, regex = True)
        clean_table['country'] = clean_table['country'].replace(regex_expression_2, np.nan, regex = True)
        clean_table['country_code'] = clean_table['country_code'].replace(regex_expression_2, np.nan, regex = True)
        clean_table['phone_number'] = clean_table['phone_number'].replace(regex_expression_2, np.nan, regex = True)
        clean_table['join_date'] = clean_table['join_date'].replace(regex_expression_2, np.nan, regex = True)
        clean_table['user_uuid'] = clean_table['user_uuid'].replace(regex_expression_2, np.nan, regex = True)

        clean_table['date_of_birth'] = clean_table['date_of_birth'].replace(np.nan, '1800-01-01', regex = True)
        clean_table['join_date'] = clean_table['join_date'].replace(np.nan, '1800-01-01', regex = True)
        clean_table['date_of_birth'] = clean_table['date_of_birth'].apply(parse)
        clean_table['date_of_birth'] = pd.to_datetime(clean_table['date_of_birth'], infer_datetime_format=True, errors='coerce')
        clean_table['join_date'] = clean_table['join_date'].apply(parse)
        clean_table['join_date'] = pd.to_datetime(clean_table['join_date'], infer_datetime_format=True, errors='coerce')
        clean_table.loc[clean_table['date_of_birth'] == '1800-01-01 00:00:00', 'date_of_birth'] = pd.NaT
        clean_table.loc[clean_table['join_date'] == '1800-01-01 00:00:00', 'join_date'] = pd.NaT
        clean_table = clean_table.dropna(axis = 0, how = 'all')

        return clean_table

    def clean_card_details(self, dataframe):
        self.extract = dataframe
        clean_table = dataframe
        clean_table.info()
        clean_table.isna()

        regex_expression_2 = '^[A-Z0-9]{10}$'
        clean_table['card_number'] = clean_table['card_number'].replace(regex_expression_2, np.nan, regex = True)
        clean_table['expiry_date'] = clean_table['expiry_date'].replace(regex_expression_2, np.nan, regex = True)
        clean_table['card_provider'] = clean_table['card_provider'].replace(regex_expression_2, np.nan, regex = True)
        clean_table['date_payment_confirmed'] = clean_table['date_payment_confirmed'].replace(regex_expression_2, np.nan, regex = True)

        clean_table['card_number'] = clean_table['card_number'].replace('NULL', np.nan, regex = True)
        clean_table['expiry_date'] = clean_table['expiry_date'].replace('NULL', np.nan, regex = True)
        clean_table['card_provider'] = clean_table['card_provider'].replace('NULL', np.nan, regex = True)
        clean_table['date_payment_confirmed'] = clean_table['date_payment_confirmed'].replace('NULL', np.nan, regex = True)
        clean_table = clean_table.dropna(axis = 0, how = 'all')
      
        clean_table['date_payment_confirmed'] = clean_table['date_payment_confirmed'].apply(parse)
        clean_table['date_payment_confirmed'] = pd.to_datetime(clean_table['date_payment_confirmed'], infer_datetime_format=True, errors='coerce')

        clean_table['card_number'] = clean_table['card_number'].astype(str).str.replace(r'\D+','', regex = True)

        print(clean_table)
        return clean_table
    
    def called_clean_store_data(self, retrive_endpoint, headers):
        retrive_stores = DataExtractor()
        self.retrive_endpoint = retrive_endpoint
        self.headers = headers
        retrive_stores.retrive_stores_data(retrive_endpoint, headers)
        clean_table = retrive_stores.stores_data
        self.clean_table = clean_table
        clean_table.info()
        clean_table.isna()

        clean_table.drop(columns = ['lat'], inplace = True)

        regex_expression_2 = '^[A-Z0-9]{10}$'
        clean_table['address'] = clean_table['address'].replace(regex_expression_2, np.nan, regex = True)
        clean_table['longitude'] = clean_table['longitude'].replace(regex_expression_2, np.nan, regex = True)
        clean_table['locality'] = clean_table['locality'].replace(regex_expression_2, np.nan, regex = True)
        clean_table['store_code'] = clean_table['store_code'].replace(regex_expression_2, np.nan, regex = True)
        clean_table['staff_numbers'] = clean_table['staff_numbers'].replace(regex_expression_2, np.nan, regex = True)
        clean_table['opening_date'] = clean_table['opening_date'].replace(regex_expression_2, np.nan, regex = True)
        clean_table['store_type'] = clean_table['store_type'].replace(regex_expression_2, np.nan, regex = True)
        clean_table['latitude'] = clean_table['latitude'].replace(regex_expression_2, np.nan, regex = True)
        clean_table['country_code'] = clean_table['country_code'].replace(regex_expression_2, np.nan, regex = True)
        clean_table['continent'] = clean_table['continent'].replace(regex_expression_2, np.nan, regex = True)
       
        clean_table['address'] = clean_table['address'].replace('N/A', np.nan, regex = True)
        clean_table['longitude'] = clean_table['longitude'].replace('N/A', np.nan, regex = True)
        clean_table['locality'] = clean_table['locality'].replace('N/A', np.nan, regex = True)

        clean_table['address'] = clean_table['address'].replace('NULL', np.nan, regex = True)
        clean_table['longitude'] = clean_table['longitude'].replace('NULL', np.nan, regex = True)
        clean_table['locality'] = clean_table['locality'].replace('NULL', np.nan, regex = True)
        clean_table['store_code'] = clean_table['store_code'].replace('NULL', np.nan, regex = True)
        clean_table['staff_numbers'] = clean_table['staff_numbers'].replace('NULL', np.nan, regex = True)
        clean_table['opening_date'] = clean_table['opening_date'].replace('NULL', np.nan, regex = True)
        clean_table['store_type'] = clean_table['store_type'].replace('NULL', np.nan, regex = True)
        clean_table['latitude'] = clean_table['latitude'].replace('NULL', np.nan, regex = True)
        clean_table['country_code'] = clean_table['country_code'].replace('NULL', np.nan, regex = True)
        clean_table['continent'] = clean_table['continent'].replace('NULL', np.nan, regex = True)
        clean_table = clean_table.dropna(axis = 0, how = 'all')

        clean_table['opening_date'] = clean_table['opening_date'].replace(np.nan, '1800-01-01', regex = True)
        clean_table['opening_date'] = clean_table['opening_date'].apply(parse)
        clean_table['opening_date'] = pd.to_datetime(clean_table['opening_date'], infer_datetime_format=True, errors='coerce')
        clean_table.loc[clean_table['opening_date'] == '1800-01-01 00:00:00', 'opening_date'] = pd.NaT

        clean_table['continent'] = clean_table['continent'].str.replace('ee', '', regex = True)

        clean_table['staff_numbers'] = clean_table['staff_numbers'].str.replace(r'\D', '', regex = True)

        return clean_table

    def convert_product_weight(self, convert_dataframe):
        self.convert_dataframe = convert_dataframe
        clean_table = convert_dataframe
        clean_table.info()
        clean_table.isna()

        clean_table['weight'] = clean_table['weight'].astype(str).str.replace(r'.$', '', regex = True)

        def convert(weight):
            if 'x' in weight:
                weight = float(re.findall('\d+', weight)[0]) * float(re.findall('\d+', weight)[1])
                weight = weight/1000
                return weight
            elif 'kg' in weight:
                weight = weight.replace('g','')
                weight = weight.replace('k','')
                return float(weight)
            elif 'g' in weight:
                weight = weight.replace('g','')
                weight = float(weight)
                weight = weight/1000
                return weight
            elif 'ml' in weight:
                weight = weight.replace('l','')
                weight = weight.replace('m','')
                weight = float(weight)
                weight = weight/1000
                return weight
            elif 'oz' in weight:
                weight = weight.replace('z','')
                weight = weight.replace('o','')
                weight = float(weight)
                weight = weight/35.274
                return weight
            else:
                return weight
        
        clean_table['weight'] = clean_table['weight'].astype(str).apply(convert)

        print(clean_table)
        return clean_table