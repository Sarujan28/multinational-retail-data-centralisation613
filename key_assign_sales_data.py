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