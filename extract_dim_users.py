from database_utils import DatabaseConnecter
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

db = DatabaseConnecter()
extract_db = DataExtractor()
cleaning_table = DataCleaning()
cleaner = cleaning_table.clean_user_data('legacy_users', cleaning_table)
db.upload_to_db(cleaner, 'dim_users')