# Multinational Retail Data Centralisation

## Table of Contents
1. [Introduction](#introduction)
2. [DatabaseConnector](#databaseconnector)
    - [read_db_creds](#read_db_creds)
    - [init_db_engine](#init_db_engine)
    - [list_db_tables](#list_db_tables)
    - [upload_to_db](#upload_to_db)
3. [DataExtractor](#dataextractor)
    - [read_rds_table](#read_rds_table)
    - [retrive_pdf_data](#retrive_pdf_data)
    - [list_number_of_stores](#list_number_of_stores)
    - [retrive_stores_data](#retrive_stores_data)
    - [extract_from_s3](#extract_from_s3)
4. [DataCleaning](#datacleaning)
    - [clean_user_data](#clean_user_data)
    - [clean_card_details](#clean_card_details)
    - [called_clean_store_data](#called_clean_store_data)
    - [convert_product_weight](#convert_product_weight)
    - [clean_products_data](#clean_products_data)
    - [clean_orders_data](#clean_orders_data)
    - [clean_date_times](#clean_date_times)
5. [Extraxtion Instructions](#extraction-instructions)
    - [Extract Orders Table](#extract-orders-table)
    - [Extract Dim Users](#extract-dim-users)
    - [Extract Dim Store Details](#extract-dim-store-details)
    - [Extract Dim Products](#extract-dim-products)
    - [Extract Dim Card Details](#extract-dim-card-details)
    - [Extract Dim Date Times](#extract-dim-date-times)
6. [Altering Table][#altering-table]
    - [Altering Columns](#altering-columns)
    - [Assigning Primary and Foreign Keys][#assigning-primary-and-foreign-keys]
7. [Queries][#queries]
8. [File structure of project](#file-structure-of-project)
9. [License Information](#license-information)

## Introduction

Multinational retail data centralisation is a project that seeks to produce a system where all the data from a hypothetical company is stored within one database. This data is accessiable from one centralised location. Using this database, queries will be run to give up-to-date metrics from the data to give the company a better understanding of their sales.

## DatabaseConnector

### read_db_creds

This function reads the credentials that would connect to an AWS database in the cloud and returns these credentials.

### init_db_engine

This function creats an sqlalchemy engine using the AWS database credentials that contains information about the AWS database and a connections to it.

### list_db_tables

This function inspects the information about the AWS database and retrives the names of the tables within the AWS database.

### upload_to_db

This function takes in a dataframe that had been cleaned and a name that will be given to the table when uploaded to the local host. Another sqlalchemy engine is created in this function; however, this time it connects to a postgresql database, sales_data, on the local host. The engine is used to upload the dataframe passed in this function on to sales_data.

## DataExtractor

### read_rds_table

This function takes in an instance of the DataConnector class so that the init_db_engine and list_db_tables can be run within the function. It also takes in a table name retrived from the AWS database. The engine created by the init_db_engine to read an sql table from the AWS database to return a dataframe.

### retrive_pdf_data

This function takes in a link to a pdf that will be read to return multiple dataframes for each page of the pdf. The dataframes will be merged together using pd.concat to return a singular dataframe.

### list_number_of_stores

This function takes the endpoint for an API that returns the number of stores and an api key-value pair. As such, the number of stores are retrived.

### retrive_stores_data

This function takes the endpoint for an API that retrives the data for a store and an api key-value pair. A while loop is used to retrive the data of all stores within the API using a range of 0 to the number of stores retrived. The endpoint takes in the store number defined at the start of the function as 0 that is increased by one after each loop. Each retrival of a store data is appended to an empty list that is returned as a dataframe after the loop is completed.

### extract_from_s3

This function takes in an address to an s3 bucket. This address is split up into multiple components from which a bucket name and object key are retrived. The file is downloaded on to the current folder using the object key and bucket name. This file is converted into a dataframe.

## DataCleaning

### clean_user_data

This function takes in the legacy_users table name and an instance of the DataExtractor class. The read_rds_table function is run using the leagcy_user table name to use a dataframe extracted from the AWS database. The dataframe is cleaned by: removing phone numbers not in a correct format, removing incorrect entries that are a mix of uppercase letters and numbers, rows where all values are null are removed and date columns are changed into the datetime format.

### clean_card_details

This function takes in the dataframe retrived from the pdf with card details. The dataframe is cleaned by: removing incorrect entries that are a mix of uppercase letters and numbers, rows where all values are null are removed, date columns are changed into the datetime format and question marks are removed from the start of some values in the card number column.

### called_clean_store_data

This function takes in the endpoint for an API that retrives the data for each store and an api key-value pair. In this function, the retrive_stores_data function is run to return a dataframe. The dataframe is cleaned by: removing the lat column, removing incorrect entries that are a mix of uppercase letters and numbers, rows where all values are null are removed, date columns are changed into the datetime format, ee is removed from the start of some values in the country code column and non-numeric are removed from some values in the staff numbers column.

### covert_product_weight

This function takes in a dataframe returned from the s3 bucket. A nested function checks each value of the weight column, removes the unit of measurment and converts it to a kilogram value. This nested function is applied to the weight column for the nested function to run.

This function failed to convert most of the measurements into kilograms such as the gram values without multiple amounts. I had tried this method on a pandas dataframe created from a list of dictionaries in which it did work. As such, I assume I have not properly converted the datatype of the columns or the existence of non-float values in the column cause the function to not work properly. However, I was unable to figure out how to fix this issue.

### clean_products_data

This function takes in the dataframe returned from the convert_product_weight function. The dataframe is cleaned by: removing incorrect entries that are a mix of uppercase letters and numbers and rows where all values are null are removed.

Due to the failure of the convert_product_function, if the unit had multiple characters, then the first letter of the unit had to be removed in this function.

### clean_orders_table

This function takes in an instance of the DataExtractor class. The read_rds_table is run within the function to return a dataframe. The dataframe is cleaned by removing the unnecessary columns: level_0, first_name, last_name and 1.

### clean_date_times

This function takes in a dataframe returned from an s3 bucket. The dataframe is cleaned by: removing incorrect entries that are a mix of uppercase letters and numbers and rows where all values are null are removed.

## Extraxtion Instructions

To extract data for all of these, instances of all three classes have to be created.

### Extract Orders Table

Using the instance of the DatabaseConnector, run init_db_engine and list_db_table to recieve the names of the tables from the AWS database. Using the instance of the DataCleaning, assign clean_orders_data to a variable that takes in an instance of DataCleaning as it returns a cleaned dataframe and the name of the table, orders_table. Then use upload_to_db with the instance of the DatabaseConnector taking in the variable from the last step and name the uploaded table, orders_table

from database_utils import DatabaseConnecter
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

db = DatabaseConnecter()
extract_db = DataExtractor()
cleaning_table = DataCleaning()
cleaner = cleaning_table.clean_orders_data('orders_table', cleaning_table)
db.upload_to_db(cleaner, 'orders_table')

### Extract Dim Users

Using the instance of the DatabaseConnector, run init_db_engine and list_db_table to recieve the names of the tables from the AWS database. Using the instance of the DataCleaning, assign clean_user_data to a variable that takes in an instance of DataCleaning as it returns a cleaned dataframe and the name of the table, legacy_users. Then use upload_to_db with the instance of the DatabaseConnector taking in the variable from the last step and name the uploaded table dim_users.

from database_utils import DatabaseConnecter
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

db = DatabaseConnecter()
extract_db = DataExtractor()
cleaning_table = DataCleaning()
cleaner = cleaning_table.clean_user_data('legacy_users', cleaning_table)
db.upload_to_db(cleaner, 'dim_users')

### Extract Dim Store Details

Using the instance of the DataExtractor, run list_number_of_stores to retrive the number of stores that take in the API endpoint and key-value pair. Use the number of stores returned to change the range of the while loop in the retrive_stores_data. Using an instance of DataCleaning, assign called_clean_store_data to a variable that takes in the API endpoint for retriving the store data and key-pair value. Then use upload_to_db with the instance of the DatabaseConnector taking in the variable from the last step and name the uploaded table dim_store_details.

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


### Extract Dim Products

Using the instance of the DataExtractor, assign extract_from_s3 that takes in an address to an s3 bucket to a variable. Using the instance of the DataCleaning, assign to a variable the convert_product_weight that takes in the variable from the last step and then pass this variable through clean_products_data that will be assigned another variable. Then use upload_to_db with the instance of the DatabaseConnector taking in the variable from the last step and name the uploaded table dim_products.

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

### Extract Dim Card Details

Using the instance of the DataExtractor, assing to a variable the retrive_pdf_data that takes in a link of a pdf. Using the instance of DataCleaning, pass the last variable through clean_card_details and assign it to a vairable. Then use upload_to_db with the instance of the DatabaseConnector taking in the variable from the last step and name the uploaded table dim_card_details.

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

### Extract Dim Date Times

Using the instance of the DataExtractor, assign extract_from_s3 that takes in an address to an s3 bucket to a variable. Using the instance of the DataCleaning, assign to a variable the clean_date_times that takes in the variable from the last step. Then use upload_to_db with the instance of the DatabaseConnector taking in the variable from the last step and name the uploaded table dim_date_times.

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

## Altering Table

### Altering Columns

Using psycopg2, you can open a connection to the local host and execute sql queries to alter a columns data type.

Example:

import psycopg2
HOST = 'localhost'
USER = 'postgres'
PASSWORD = 'dragon'
DATABASE = 'sales_data'
PORT = 5432

with psycopg2.connect(host=HOST, user=USER, password=PASSWORD, dbname=DATABASE, port=PORT) as conn:
    with conn.cursor() as cur:
        cur.execute('''ALTER TABLE dim_users
                           ALTER COLUMN first_name TYPE VARCHAR(255);
                       ALTER TABLE dim_users
                           ALTER COLUMN last_name TYPE VARCHAR(255);
                       ALTER TABLE dim_users
                           ALTER COLUMN date_of_birth TYPE DATE;
                       ALTER TABLE dim_users
                           ALTER COLUMN country_code TYPE VARCHAR(5);
                       ALTER TABLE dim_users
                           ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid;
                       ALTER TABLE dim_users
                           ALTER COLUMN join_date TYPE DATE;''')
conn.commit()
conn.close()

There was issues with altering the dim_products table, since the weight class colum created from weight ranges would be wrong as the weight column was not converted properly as well as changing the data type to boolean for the removed column that was renamed still_available lead to mostly null values

### Assigning Primary and Foreign Keys

Simiarly, primary and foreign keys are assgined executung queries through the same method.

import psycopg2
HOST = 'localhost'
USER = 'postgres'
PASSWORD = 'dragon'
DATABASE = 'sales_data'
PORT = 5432

with psycopg2.connect(host=HOST, user=USER, password=PASSWORD, dbname=DATABASE, port=PORT) as conn:
    with conn.cursor() as cur:
        cur.execute('''ALTER TABLE dim_users
                           ADD PRIMARY KEY (user_uuid);
                       ALTER TABLE dim_store_details
                           ADD PRIMARY KEY (store_code);
                       ALTER TABLE dim_date_times
                           ADD PRIMARY KEY (date_uuid);
                       ALTER TABLE dim_card_details
                           ADD PRIMARY KEY (card_number);
                       ALTER TABLE dim_products
                           ADD PRIMARY KEY (product_code);
                       ALTER TABLE orders_table
                           ADD CONSTRAINT FK_dates_orders 
                           FOREIGN KEY (date_uuid) REFERENCES dim_date_times(date_uuid);
                       ALTER TABLE orders_table
                           ADD CONSTRAINT FK_users_orders 
                           FOREIGN KEY (user_uuid) REFERENCES dim_users(user_uuid);
                       ALTER TABLE orders_table
                           ADD CONSTRAINT FK_store_orders 
                           FOREIGN KEY (store_code) REFERENCES dim_store_details(store_code);
                       ALTER TABLE orders_table
                           ADD CONSTRAINT FK_card_orders 
                           FOREIGN KEY (card_number) REFERENCES dim_card_details(card_number);
                       ALTER TABLE orders_table
                           ADD CONSTRAINT FK_product_orders 
                           FOREIGN KEY (product_code) REFERENCES dim_products(product_code);''')
conn.commit()
conn.close()

### Queries

To run queries give up-to-date metrics from the data to give the company a better understanding of their sales, an sqlalchemy engine is created from the local host database. Then a conncetion is made using the engine with execution option to auto-commit, to execute queries afterwards. Due to it being auto-commit, the connection will be closed after going through the with block.

Example:
from sqlalchemy import create_engine
import pandas as pd
DATABASE_TYPE = 'postgresql'
DBAPI = 'psycopg2'
HOST = 'localhost'
USER = 'postgres'
PASSWORD = 'dragon'
DATABASE = 'sales_data'
PORT = 5432
engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")

with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
        print('Number of stores within each country:')
        stores_per_country = pd.read_sql_query('''SELECT country_code AS country,
                                                         COUNT(store_code) AS total_no_stores
                                                  FROM
                                                      dim_store_details
                                                  GROUP BY
                                                     country_code
                                                  ORDER BY
                                                      COUNT(store_code) DESC;''', conn)
        print(stores_per_country)

The percentage of sale per store type query seems to raise the error:
TypeError: sqlalchemy.cyextension.immutabledict.immutabledict is not a sequence

The total amount of sales for each store type from Germany the total sales column retruns values in this format 1.983736e+05.

## File structure of project

![file_structure_of_project](file_structure.png)

## License Information

MIT License

Copyright (c) [2023] [Sarujan Sothilingham]

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.