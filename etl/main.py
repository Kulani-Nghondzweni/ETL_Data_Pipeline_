import pandas as pd
import sqlite3
import os

# My File paths
DATA_FOLDER = "./data"
ORDERS_FILE = os.path.join(DATA_FOLDER, "orders.csv")
USERS_FILE = os.path.join(DATA_FOLDER, "users.csv")
DB_FILE = "etl_database.db"

# MY EXTRACT FUNCTION
def extract_orders(path=ORDERS_FILE):
    df = pd.read_csv(path, sep=";")
    print("Orders Data Sample:")
    print(df.head())
    print(f"Orders Data Shape: {df.shape}")
    return df

def extract_users(path=USERS_FILE):
    df = pd.read_csv(path)
    print("\nUsers Data Sample:")
    print(df.head())
    print(f"Users Data Shape: {df.shape}")
    return df

# MY TRANSFORM FUNCTION
def transform_orders(df):
    df['product_list'] = df['product_list'].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)
    df['date_purchased'] = pd.to_datetime(df['date_purchased'])
    df['user_id'] = df['user_id'].astype(int)
    df['order_no'] = df['order_no'].astype(int)
    print("\nTransformed Orders Data Sample:")
    print(df.head())
    return df

def transform_users(df):
    df['date_joined'] = pd.to_datetime(df['date_joined'])
    df['name'] = df['name'].str.title()
    df['address'] = df['address'].str.strip()
    print("\nTransformed Users Data Sample:")
    print(df.head())
    return df

# MY LOAD FUNCTION 
def load_to_sqlite(orders_df, users_df, db_path=DB_FILE):
    conn = sqlite3.connect(db_path)
    orders_df.to_sql('orders', conn, if_exists='replace', index=False)
    users_df.to_sql('users', conn, if_exists='replace', index=False)
    tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table';", conn)
    print("\nTables in SQLite DB:")
    print(tables)
    conn.close()

# MY MAIN PIPELINE
def main():
    orders_df = extract_orders()
    users_df = extract_users()

    orders_df = transform_orders(orders_df)
    users_df = transform_users(users_df)

    load_to_sqlite(orders_df, users_df)

    print("\nETL pipeline completed successfully!")

if __name__ == "__main__":
    main()
