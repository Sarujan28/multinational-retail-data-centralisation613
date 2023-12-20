import psycopg2
HOST = 'localhost'
USER = 'postgres'
PASSWORD = 'dragon'
DATABASE = 'sales_data'
PORT = 5432

with psycopg2.connect(host=HOST, user=USER, password=PASSWORD, dbname=DATABASE, port=PORT) as conn:
    with conn.cursor() as cur:
        cur.execute('''UPDATE dim_products 
                           SET product_price = REPLACE(product_price,'£','');
                       ALTER TABLE dim_products
                           ADD weight_class VARCHAR(25);
                       UPDATE dim_products   
                       SET weight_class = CASE
                                              WHEN weight >= 0 AND weight < 2 THEN 'Light'
                                              WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
                                              WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
                                              WHEN weight >= 140 THEN 'Truck-Required'
                                          END;
                       ALTER TABLE dim_products
                           ALTER COLUMN product_price TYPE REAL USING product_price::real;
                       ALTER TABLE dim_products
                           ALTER COLUMN weight TYPE DOUBLE PRECISION USING weight::double precision;
                       ALTER TABLE dim_products
                           ALTER COLUMN "EAN" TYPE VARCHAR(25);
                       ALTER TABLE dim_products
                           ALTER COLUMN product_code TYPE VARCHAR(25);
                       ALTER TABLE dim_products
                           ALTER COLUMN date_added TYPE DATE USING date_added::date;
                       ALTER TABLE dim_products
                           ALTER COLUMN uuid TYPE UUID USING uuid::uuid;
                       ALTER TABLE dim_products
                           RENAME removed TO still_available;
                       UPDATE dim_products
                       SET still_available = CASE
                                                 WHEN still_available = 'Still_available' THEN 'TRUE'
                                                 WHEN still_available = 'Removed' THEN 'FALSE'
                                             END;
                       ALTER TABLE dim_products
                           ALTER COLUMN still_available DROP DEFAULT;
                       ALTER TABLE dim_products
                           ALTER COLUMN still_available TYPE BOOL USING still_available::boolean;''')
conn.commit()
conn.close()