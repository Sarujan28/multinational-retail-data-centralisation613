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
        print('Number of stores within each locality:')
        locality_per_country = pd.read_sql_query('''SELECT locality,
                                                           COUNT(store_code) AS total_no_stores
                                                    FROM
                                                        dim_store_details
                                                    GROUP BY
                                                        locality
                                                    ORDER BY
                                                        COUNT(store_code) DESC;''', conn)
        print(locality_per_country)
        print('Total amount of sales in a month:')
        sales_per_month = pd.read_sql_query('''SELECT "month",
                                                      SUM(product_price * product_quantity) AS total_sales
                                               FROM
                                                   orders_table
                                               INNER JOIN
                                                   dim_products ON orders_table.product_code = dim_products.product_code
                                               INNER JOIN
                                                   dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
                                               GROUP BY
                                                   "month"
                                               ORDER BY
                                                   SUM(product_price * product_quantity) DESC;''',conn)
        print(sales_per_month)
        print('Number of sales and product quantity from online and offline:')
        online = pd.read_sql_query('''SELECT COUNT(product_quantity) AS number_of_sales,
                                             SUM(product_quantity) AS product_quantity_count,
                                             CASE
                                                 WHEN store_type = 'Web Portal' THEN 'Web'
		                                         ELSE 'Offline'
		                                     END AS "location"
                                      FROM
                                          orders_table
                                      INNER JOIN
                                          dim_store_details ON orders_table.store_code = dim_store_details.store_code
                                      GROUP BY
                                          CASE
                                              WHEN store_type = 'Web Portal' THEN 'Web'
		                                      ELSE 'Offline'
                                          END
                                      ORDER BY
                                          SUM(product_quantity) ASC;''', conn)
        print(online)
        print('Percentage of sales per store type:')
        percentage = pd.read_sql_query('''SELECT store_type,
                                                 SUM(product_price * product_quantity) AS total_sales,
	                                             SUM(product_price * product_quantity)/(SELECT SUM(product_price * product_quantity)
										                                                 FROM
											                                                 orders_table
										                                                 INNER JOIN
                                                                                             dim_products ON orders_table.product_code = dim_products.product_code) AS "percentage_total(%)"
                                          FROM
                                              orders_table
                                          INNER JOIN
                                              dim_products ON orders_table.product_code = dim_products.product_code
                                          INNER JOIN
                                              dim_store_details ON orders_table.store_code = dim_store_details.store_code
                                          GROUP BY
                                              store_type
                                          ORDER BY
                                              SUM(product_price * product_quantity) Desc;''', conn)
        print(percentage)
        print('Total amount of sales in each month of each year:')
        month_year = pd.read_sql_query('''SELECT SUM(product_price * product_quantity) AS total_sales,
                                                 "year"
                                                 "month"
                                          FROM
                                              orders_table
                                          INNER JOIN
                                              dim_products ON orders_table.product_code = dim_products.product_code
                                          INNER JOIN
                                              dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
                                          GROUP BY
                                              "month", "year"
                                          ORDER BY
                                              SUM(product_price * product_quantity) DESC;''',conn)
        print(month_year)
        print('Total number of staff from each country:')
        staff = pd.read_sql_query('''SELECT SUM(staff_numbers) AS total_staff_numbers,
                                            country_code
                                     FROM
                                         dim_store_details
                                     GROUP BY
                                         country_code
                                     ORDER BY
                                         SUM(staff_numbers) DESC;''',conn)
        print(staff)
        print('Total amount of sale for each store type in Germany:')
        german = pd.read_sql_query('''SELECT SUM(product_price * product_quantity) AS total_sales,
                                             store_type,
                                             country_code
                                      FROM
                                          orders_table
                                      INNER JOIN
                                          dim_products ON orders_table.product_code = dim_products.product_code
                                      INNER JOIN
                                          dim_store_details ON orders_table.store_code = dim_store_details.store_code
                                      WHERE
                                          country_code = 'DE'
                                      GROUP BY
                                          store_type, country_code
                                      ORDER BY
                                          SUM(product_price * product_quantity) ASC;''', conn)
        print(german)