import psycopg2

# =========================
# DIM CUSTOMERS
# =========================
def build_dim_customers(connection) -> int:
    cursor = connection.cursor()

    cursor.execute("TRUNCATE TABLE warehouse.dim_customers RESTART IDENTITY;")

    cursor.execute("""
        INSERT INTO warehouse.dim_customers (customer_name, email, phone)
        SELECT
            CONCAT(first_name, ' ', last_name) AS customer_name,
            email,
            phone
        FROM production.customers;
    """)

    connection.commit()

    cursor.execute("SELECT COUNT(*) FROM warehouse.dim_customers;")
    return cursor.fetchone()[0]


# =========================
# DIM PRODUCTS
# =========================
def build_dim_products(connection) -> int:
    cursor = connection.cursor()

    cursor.execute("TRUNCATE TABLE warehouse.dim_products RESTART IDENTITY;")

    cursor.execute("""
        INSERT INTO warehouse.dim_products (product_name, price)
        SELECT product_name, price
        FROM production.products;
    """)

    connection.commit()

    cursor.execute("SELECT COUNT(*) FROM warehouse.dim_products;")
    return cursor.fetchone()[0]


# =========================
# DIM DATE (FIXED FOR YOUR TABLE)
# =========================
def build_dim_date(start_date: str, end_date: str, connection) -> int:
    cursor = connection.cursor()

    cursor.execute("TRUNCATE TABLE warehouse.dim_date RESTART IDENTITY;")

    cursor.execute(f"""
        WITH dates AS (
            SELECT generate_series(
                '{start_date}'::DATE,
                '{end_date}'::DATE,
                INTERVAL '1 day'
            )::DATE AS d
        )
        INSERT INTO warehouse.dim_date
            (calendar_date, year, month, day, quarter)
        SELECT
            d,
            EXTRACT(YEAR FROM d)::INT,
            EXTRACT(MONTH FROM d)::INT,
            EXTRACT(DAY FROM d)::INT,
            EXTRACT(QUARTER FROM d)::INT
        FROM dates;
    """)

    connection.commit()

    cursor.execute("SELECT COUNT(*) FROM warehouse.dim_date;")
    return cursor.fetchone()[0]


# =========================
# FACT SALES
# =========================
def build_fact_sales(connection) -> int:
    cursor = connection.cursor()

    cursor.execute("TRUNCATE TABLE warehouse.fact_sales RESTART IDENTITY;")

    cursor.execute("""
        INSERT INTO warehouse.fact_sales
            (customer_sk, product_sk, date_sk, quantity, line_total)
        SELECT
            dc.customer_sk,
            dp.product_sk,
            dd.date_sk,
            ti.quantity,
            ti.line_total
        FROM production.transaction_items ti
        JOIN production.transactions t
            ON ti.transaction_id = t.transaction_id
        JOIN warehouse.dim_customers dc
            ON t.customer_id = dc.customer_id
        JOIN warehouse.dim_products dp
            ON ti.product_id = dp.product_id
        JOIN warehouse.dim_date dd
            ON t.transaction_date = dd.calendar_date;
    """)

    connection.commit()

    cursor.execute("SELECT COUNT(*) FROM warehouse.fact_sales;")
    return cursor.fetchone()[0]
