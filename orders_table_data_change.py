import psycopg2
HOST = 'localhost'
USER = 'postgres'
PASSWORD = 'dragon'
DATABASE = 'sales_data'
PORT = 5432

with psycopg2.connect(host=HOST, user=USER, password=PASSWORD, dbname=DATABASE, port=PORT) as conn:
    with conn.cursor() as cur:
        cur.execute('''ALTER TABLE orders_table
                           ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid;
                       ALTER TABLE orders_table
                           ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid;
                       ALTER TABLE orders_table
                           ALTER COLUMN card_number TYPE VARCHAR(50);
                       ALTER TABLE orders_table
                           ALTER COLUMN store_code TYPE VARCHAR(25);
                       ALTER TABLE orders_table
                           ALTER COLUMN product_code TYPE VARCHAR(25);
                       ALTER TABLE orders_table
                           ALTER COLUMN product_quantity TYPE SMALLINT;''')
conn.commit()
conn.close()