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