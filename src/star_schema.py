import pandas as pd
from datetime import datetime
import os

def apply_scd_type1(customers, customer_id, new_address):
    # SCD Type 1: Overwrite address for customer
    customers.loc[customers['customer_id'] == customer_id, 'address'] = new_address
    return customers

def apply_scd_type2(customers, customer_id, new_address, effective_date):
    # SCD Type 2: Add new version of customer record
    customer_row = customers[(customers['customer_id'] == customer_id) & (customers['is_active'] == True)].copy()
    if not customer_row.empty:
        # Mark current record as inactive
        customers.loc[(customers['customer_id'] == customer_id) & (customers['is_active'] == True), 'end_date'] = effective_date
        customers.loc[(customers['customer_id'] == customer_id) & (customers['is_active'] == True), 'is_active'] = False
        # Create new record
        new_row = customer_row.copy()
        new_row['address'] = new_address
        new_row['start_date'] = effective_date
        new_row['end_date'] = pd.NaT
        new_row['is_active'] = True
        customers = pd.concat([customers, new_row], ignore_index=True)
    return customers

def create_star_schema():
    # Load data
    products = pd.read_csv('data/processed/products.csv')
    customers = pd.read_csv('data/processed/customers.csv')
    time = pd.read_csv('data/processed/time.csv')
    sales = pd.read_csv('data/processed/sales.csv')

    # Apply SCD Type 1 example
    customers = apply_scd_type1(customers, customer_id=1, new_address='123 New St')

    # Apply SCD Type 2 example
    customers = apply_scd_type2(customers, customer_id=2, new_address='456 Updated Rd', effective_date=datetime(2024, 6, 1))

    # Save updated data
    os.makedirs('data/processed/star', exist_ok=True)
    products.to_csv('data/processed/star/dim_products.csv', index=False)
    customers.to_csv('data/processed/star/dim_customers.csv', index=False)
    time.to_csv('data/processed/star/dim_time.csv', index=False)
    sales.to_csv('data/processed/star/fact_sales.csv', index=False)
    print("Star schema data saved to data/processed/star")

if __name__ == "__main__":
    create_star_schema()