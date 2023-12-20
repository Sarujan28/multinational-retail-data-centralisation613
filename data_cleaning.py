from data_extraction import DataExtractor
import pandas as pd
import numpy as np
from dateutil.parser import parse

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