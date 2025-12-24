import pandas as pd
import psycopg2

def cleanse_customer_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    
    # Lowercase emails
    df["email"] = df["email"].str.lower()

    # Remove spaces from phone numbers
    df["phone"] = df["phone"].str.replace(" ", "")

    # Title case names
    df["customer_name"] = df["customer_name"].str.title()

    return df


def cleanse_product_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Capitalize product names
    df["product_name"] = df["product_name"].str.title()

    # Replace negative prices
    df.loc[df["price"] < 0, "price"] = abs(df["price"])

    return df


def apply_business_rules(df: pd.DataFrame, rule_type: str) -> pd.DataFrame:
    df = df.copy()

    if rule_type == "profit_margin":
        df["profit_margin"] = df["price"] * 0.25  # 25% margin

    if rule_type == "price_category":
        df["price_category"] = pd.cut(
            df["price"],
            bins=[0, 50, 150, 500, 5000],
            labels=["Low", "Medium", "High", "Premium"],
            include_lowest=True
        )

    return df


def load_to_production(df: pd.DataFrame, table_name: str, connection, strategy: str) -> dict:
    cursor = connection.cursor()

    if strategy == "full":
        cursor.execute(f"TRUNCATE TABLE production.{table_name} RESTART IDENTITY;")
    
    buffer = []
    for _, row in df.iterrows():
        values = tuple(row)
        buffer.append(values)

    columns = ", ".join(df.columns)
    placeholders = ", ".join(["%s"] * len(df.columns))

    insert_query = f"""
        INSERT INTO production.{table_name} ({columns})
        VALUES ({placeholders})
    """

    cursor.executemany(insert_query, buffer)
    connection.commit()

    return {
        "status": "success",
        "records_loaded": len(df),
        "table": table_name
    }
