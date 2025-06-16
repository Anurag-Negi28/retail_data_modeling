import sys
import os

# Add the project root directory to sys.path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(project_root)

from src.data_generator import generate_products, generate_customers, generate_time, generate_sales, save_dataframes
from src.star_schema import create_star_schema
from src.snowflake_schema import create_snowflake_schema

def run_pipeline():
    print("Generating mock data...")
    products = generate_products(10)
    customers = generate_customers(20)
    time = generate_time()
    sales = generate_sales(products, customers, time, 100)
    save_dataframes(products, customers, time, sales)
    
    print("Creating Star Schema...")
    create_star_schema()
    
    print("Creating Snowflake Schema...")
    create_snowflake_schema()
    print("Pipeline completed.")

if __name__ == "__main__":
    run_pipeline()