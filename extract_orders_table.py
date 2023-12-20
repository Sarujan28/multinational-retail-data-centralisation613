from database_utils import DatabaseConnecter
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

db = DatabaseConnecter()
extract_db = DataExtractor()
cleaning_table = DataCleaning()
cleaner = cleaning_table.clean_orders_data('orders_table', cleaning_table)
db.upload_to_db(cleaner, 'orders_table')