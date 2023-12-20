from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from database_utils import DatabaseConnecter

address = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
extractor = DataExtractor()
extract = extractor.extract_from_s3(address)
cleaning = DataCleaning()
cleaner = cleaning.clean_date_times(extract)
db = DatabaseConnecter()
db.upload_to_db(cleaner, 'dim_date_times')