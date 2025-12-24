from faker import Faker
import pandas as pd
import random

fake = Faker()

def generate_customers(num_customers: int) -> pd.DataFrame:
    data = []
    for i in range(num_customers):
        data.append({
            "customer_id": f"CUST{i+1}",
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.email(),
            "phone": fake.phone_number(),
            "city": fake.city()
        })
    return pd.DataFrame(data)

def generate_products(num_products: int) -> pd.DataFrame:
    categories = ["Electronics", "Clothing", "Sports", "Toys"]
    data = []
    for i in range(num_products):
        data.append({
            "product_id": f"PROD{i+1}",
            "product_name": fake.word(),
            "category": random.choice(categories),
            "price": round(random.uniform(5, 200), 2)
        })
    return pd.DataFrame(data)

def generate_transactions(num_transactions: int, customers_df: pd.DataFrame) -> pd.DataFrame:
    data = []
    for i in range(num_transactions):
        data.append({
            "transaction_id": f"TXN{i+1}",
            "customer_id": random.choice(customers_df["customer_id"]),
            "transaction_date": fake.date_time_this_year(),
            "payment_method": random.choice(["Credit Card", "Debit Card", "Cash"]),
            "total_amount": round(random.uniform(10, 500), 2)
        })
    return pd.DataFrame(data)

def generate_transaction_items(transactions_df: pd.DataFrame, products_df: pd.DataFrame) -> pd.DataFrame:
    data = []
    for _, row in transactions_df.iterrows():
        for j in range(random.randint(1, 3)):
            product = products_df.sample(n=1).iloc[0]
            qty = random.randint(1, 5)
            data.append({
                "item_id": fake.uuid4(),
                "transaction_id": row["transaction_id"],
                "product_id": product["product_id"],
                "quantity": qty,
                "line_total": qty * product["price"]
            })
    return pd.DataFrame(data)
if __name__ == "__main__":
    customers = generate_customers(1000)
    products = generate_products(500)
    transactions = generate_transactions(10000, customers)
    items = generate_transaction_items(transactions, products)

    customers.to_csv("data/raw/customers.csv", index=False)
    products.to_csv("data/raw/products.csv", index=False)
    transactions.to_csv("data/raw/transactions.csv", index=False)
    items.to_csv("data/raw/transaction_items.csv", index=False)

    print("CSV files created!")
