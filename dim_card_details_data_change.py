import psycopg2
HOST = 'localhost'
USER = 'postgres'
PASSWORD = 'dragon'
DATABASE = 'sales_data'
PORT = 5432

with psycopg2.connect(host=HOST, user=USER, password=PASSWORD, dbname=DATABASE, port=PORT) as conn:
    with conn.cursor() as cur:
        cur.execute('''ALTER TABLE dim_card_details
                           ALTER COLUMN card_number TYPE VARCHAR(25);
                       ALTER TABLE dim_card_details
                           ALTER COLUMN expiry_date TYPE VARCHAR(5);
                       ALTER TABLE dim_card_details
                           ALTER COLUMN date_payment_confirmed TYPE DATE;''')
conn.commit()
conn.close()