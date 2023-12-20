import psycopg2
HOST = 'localhost'
USER = 'postgres'
PASSWORD = 'dragon'
DATABASE = 'sales_data'
PORT = 5432

with psycopg2.connect(host=HOST, user=USER, password=PASSWORD, dbname=DATABASE, port=PORT) as conn:
    with conn.cursor() as cur:
        cur.execute('''ALTER TABLE dim_date_times
                           ALTER COLUMN "month" TYPE VARCHAR(2);
                       ALTER TABLE dim_date_times
                           ALTER COLUMN "year" TYPE VARCHAR(4);
                       ALTER TABLE dim_date_times
                           ALTER COLUMN "day" TYPE VARCHAR(2);
                       ALTER TABLE dim_date_times
                           ALTER COLUMN time_period TYPE VARCHAR(25);
                       ALTER TABLE dim_date_times
                           ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid;''')
conn.commit()
conn.close()