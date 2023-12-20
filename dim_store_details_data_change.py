import psycopg2
HOST = 'localhost'
USER = 'postgres'
PASSWORD = 'dragon'
DATABASE = 'sales_data'
PORT = 5432

with psycopg2.connect(host=HOST, user=USER, password=PASSWORD, dbname=DATABASE, port=PORT) as conn:
    with conn.cursor() as cur:
        cur.execute('''ALTER TABLE dim_store_details
                           ALTER COLUMN longitude TYPE REAL USING longitude::real;
                       ALTER TABLE dim_store_details
                           ALTER COLUMN locality TYPE VARCHAR(255);
                       ALTER TABLE dim_store_details
                           ALTER COLUMN store_code TYPE VARCHAR(25);
                       ALTER TABLE dim_store_details
                           ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::smallint;
                       ALTER TABLE dim_store_details
                           ALTER COLUMN store_type TYPE VARCHAR(255);
                       ALTER TABLE dim_store_details
                           ALTER COLUMN store_type DROP NOT NULL;
                       ALTER TABLE dim_store_details
                           ALTER COLUMN latitude TYPE REAL USING latitude::real;
                       ALTER TABLE dim_store_details
                           ALTER COLUMN country_code TYPE VARCHAR(25);
                       ALTER TABLE dim_store_details
                           ALTER COLUMN continent TYPE VARCHAR(255);''')
conn.commit()
conn.close()