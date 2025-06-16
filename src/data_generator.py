import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os

def generate_products(n=10):
    # Generate product data
    products = {
        'product_id': range(1, n+1),
        'product_name': [f'Product_{i}' for i in range(1, n+1)],
        'category': random.choices(['Electronics', 'Clothing', 'Food'], k=n),
        'price': [round(random.uniform(10, 500), 2) for _ in range(n)]
    }
    return pd.DataFrame(products)

def generate_customers(n=20):
    # Generate customer data with SCD attributes
    customers = {
        'customer_id': range(1, n+1),
        'customer_name': [f'Customer_{i}' for i in range(1, n+1)],
        'email': [f'customer{i}@example.com' for i in range(1, n+1)],
        'address': [f'{random.randint(100, 999)} Main St' for _ in range(n)],
        'start_date': [datetime(2024, 1, 1) for _ in range(n)],
        'end_date': [pd.NaT for _ in range(n)],
        'is_active': [True for _ in range(n)]
    }
    return pd.DataFrame(customers)

def generate_time(start_date='2024-01-01', end_date='2024-12-31'):
    # Generate time dimension
    dates = pd.date_range(start=start_date, end=end_date, freq='D')
    time_data = {
        'date_id': range(1, len(dates)+1),
        'date': dates,
        'day': [d.day for d in dates],
        'month': [d.month for d in dates],
        'year': [d.year for d in dates],
        'quarter': [d.quarter for d in dates],
        'day_of_week': [d.day_name() for d in dates]
    }
    return pd.DataFrame(time_data)

def generate_sales(products, customers, time, n=100):
    # Generate sales fact data
    sales = {
        'sale_id': range(1, n+1),
        'product_id': random.choices(products['product_id'], k=n),
        'customer_id': random.choices(customers['customer_id'], k=n),
        'date_id': random.choices(time['date_id'], k=n),
        'quantity': [random.randint(1, 10) for _ in range(n)]
    }
    sales_df = pd.DataFrame(sales)
    # Calculate total_amount
    sales_df = sales_df.merge(products[['product_id', 'price']], on='product_id')
    sales_df['total_amount'] = sales_df['quantity'] * sales_df['price']
    return sales_df[['sale_id', 'product_id', 'customer_id', 'date_id', 'quantity', 'total_amount']]

def save_dataframes(products, customers, time, sales, output_dir='data/processed'):
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    # Save to CSV
    products.to_csv(f'{output_dir}/products.csv', index=False)
    customers.to_csv(f'{output_dir}/customers.csv', index=False)
    time.to_csv(f'{output_dir}/time.csv', index=False)
    sales.to_csv(f'{output_dir}/sales.csv', index=False)
    print(f"Data saved to {output_dir}")

if __name__ == "__main__":
    # Generate mock data
    products = generate_products(10)
    customers = generate_customers(20)
    time = generate_time()
    sales = generate_sales(products, customers, time, 100)
    save_dataframes(products, customers, time, sales)