from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from database_utils import DatabaseConnecter

extract = DataExtractor()
endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
headers = {'x-api-key':'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
extract.list_number_of_stores(endpoint, headers)
retrive_endpoint = f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'
cleaning = DataCleaning()
cleaner = cleaning.called_clean_store_data(retrive_endpoint, headers)
db =DatabaseConnecter()
db.upload_to_db(cleaner, 'dim_store_details')
