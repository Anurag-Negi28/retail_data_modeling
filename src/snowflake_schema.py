import pandas as pd
import os

def create_snowflake_schema():
    # Load data
    products = pd.read_csv('data/processed/products.csv')
    customers = pd.read_csv('data/processed/customers.csv')
    time = pd.read_csv('data/processed/time.csv')
    sales = pd.read_csv('data/processed/sales.csv')

    # Normalize Product into Product and Category
    categories = pd.DataFrame({
        'category_id': range(1, len(products['category'].unique()) + 1),
        'category_name': products['category'].unique()
    })
    products = products.merge(categories, left_on='category', right_on='category_name')
    products = products[['product_id', 'product_name', 'price', 'category_id']]
    
    # Normalize Time into Date and Month
    months = pd.DataFrame({
        'month_id': range(1, len(time[['month', 'year']].drop_duplicates()) + 1),
        'month': time[['month', 'year']].drop_duplicates()['month'],
        'year': time[['month', 'year']].drop_duplicates()['year']
    })
    time = time.merge(months, on=['month', 'year'])
    time = time[['date_id', 'date', 'day', 'quarter', 'day_of_week', 'month_id']]

    # Save Snowflake schema tables
    os.makedirs('data/processed/snowflake', exist_ok=True)
    products.to_csv('data/processed/snowflake/dim_products.csv', index=False)
    categories.to_csv('data/processed/snowflake/dim_categories.csv', index=False)
    customers.to_csv('data/processed/snowflake/dim_customers.csv', index=False)
    time.to_csv('data/processed/snowflake/dim_time.csv', index=False)
    months.to_csv('data/processed/snowflake/dim_months.csv', index=False)
    sales.to_csv('data/processed/snowflake/fact_sales.csv', index=False)
    print("Snowflake schema data saved to data/processed/snowflake")

if __name__ == "__main__":
    create_snowflake_schema()