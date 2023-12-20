from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from database_utils import DatabaseConnecter

link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'

extractor = DataExtractor()
extract = extractor.retrive_pdf_data(link)
cleaning = DataCleaning()
cleaner = cleaning.clean_card_details(extract)
db = DatabaseConnecter()
db.upload_to_db(cleaner,'dim_card_details')
