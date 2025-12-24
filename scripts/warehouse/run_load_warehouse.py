import psycopg2
from load_warehouse import (
    build_dim_customers,
    build_dim_products,
    build_dim_date,
    build_fact_sales
)

connection = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres",
    password="Chaituram@1918"
)

print("Loading dim_customers...")
print("Records:", build_dim_customers(connection))

print("Loading dim_products...")
print("Records:", build_dim_products(connection))

print("Loading dim_date...")
print("Records:", build_dim_date("2020-01-01", "2025-12-31", connection))

print("Loading fact_sales...")
print("Records:", build_fact_sales(connection))

connection.close()

print("WAREHOUSE LOAD COMPLETE")
