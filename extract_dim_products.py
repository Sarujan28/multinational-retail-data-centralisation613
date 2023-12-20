from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from database_utils import DatabaseConnecter

address = 's3://data-handling-public/products.csv'
extractor = DataExtractor()
df = extractor.extract_from_s3(address)
cleaning = DataCleaning()
convert = cleaning.convert_product_weight(df)
cleaner = cleaning.clean_products_data(convert)
db = DatabaseConnecter()
db.upload_to_db(cleaner,'dim_products')