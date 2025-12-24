import psycopg2
from build_aggregates import (
    build_agg_daily_sales,
    build_agg_product_performance,
    build_agg_customer_metrics
)

conn = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres",
    password="YOUR_PASSWORD"
)

build_agg_daily_sales(conn)
build_agg_product_performance(conn)
build_agg_customer_metrics(conn)

conn.close()
print("AGGREGATES BUILT SUCCESSFULLY")
