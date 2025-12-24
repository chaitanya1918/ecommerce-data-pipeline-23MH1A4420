import psycopg2

def build_agg_daily_sales(connection):
    cur = connection.cursor()
    cur.execute("TRUNCATE TABLE warehouse.agg_daily_sales;")
    cur.execute("""
        INSERT INTO warehouse.agg_daily_sales
        SELECT
            d.calendar_date,
            SUM(f.line_total) AS total_revenue,
            SUM(f.quantity) AS total_quantity,
            COUNT(DISTINCT f.date_sk) AS total_orders
        FROM warehouse.fact_sales f
        JOIN warehouse.dim_date d ON f.date_sk = d.date_sk
        GROUP BY d.calendar_date;
    """)
    connection.commit()

def build_agg_product_performance(connection):
    cur = connection.cursor()
    cur.execute("TRUNCATE TABLE warehouse.agg_product_performance;")
    cur.execute("""
        INSERT INTO warehouse.agg_product_performance
        SELECT
            product_sk,
            SUM(line_total) AS total_revenue,
            SUM(quantity) AS total_quantity
        FROM warehouse.fact_sales
        GROUP BY product_sk;
    """)
    connection.commit()

def build_agg_customer_metrics(connection):
    cur = connection.cursor()
    cur.execute("TRUNCATE TABLE warehouse.agg_customer_metrics;")
    cur.execute("""
        INSERT INTO warehouse.agg_customer_metrics
        SELECT
            customer_sk,
            SUM(line_total) AS total_revenue,
            COUNT(*) AS total_orders
        FROM warehouse.fact_sales
        GROUP BY customer_sk;
    """)
    connection.commit()
