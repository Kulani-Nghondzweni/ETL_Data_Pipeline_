import unittest
import pandas as pd
import os
from etl.main import extract_orders, extract_users, transform_orders, transform_users, load_to_sqlite, DB_FILE
import sqlite3

class TestETLPipeline(unittest.TestCase):

    def setUp(self):
        """Runs before each test."""
        self.orders_df = extract_orders()
        self.users_df = extract_users()

    def test_extract_orders(self):
        self.assertFalse(self.orders_df.empty)
        self.assertIn('order_no', self.orders_df.columns)
        self.assertIn('product_list', self.orders_df.columns)

    def test_extract_users(self):
        self.assertFalse(self.users_df.empty)
        self.assertIn('user_id', self.users_df.columns)
        self.assertIn('name', self.users_df.columns)

    def test_transform_orders(self):
        transformed = transform_orders(self.orders_df)
        self.assertIsInstance(transformed['product_list'][0], str)
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(transformed['date_purchased']))

    def test_transform_users(self):
        transformed = transform_users(self.users_df)
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(transformed['date_joined']))
        self.assertTrue(all(transformed['name'].str.istitle()))

    def test_load_to_sqlite(self):
        transformed_orders = transform_orders(self.orders_df)
        transformed_users = transform_users(self.users_df)
        load_to_sqlite(transformed_orders, transformed_users)

        conn = sqlite3.connect(DB_FILE)
        tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table';", conn)
        self.assertIn('orders', tables['name'].values)
        self.assertIn('users', tables['name'].values)
        conn.close()

if __name__ == '__main__':
    unittest.main()
