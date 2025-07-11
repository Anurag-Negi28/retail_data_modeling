Retail Data Modeling Project
This project models a small retail business scenario using Star and Snowflake schemas, implements Slowly Changing Dimensions (SCD) Type 1 and Type 2, and generates ER diagrams.
Project Structure

data/: Stores raw, processed, and schema data.
src/: Python scripts for data generation and schema creation.
scripts/: Pipeline execution script.
notebooks/: Jupyter notebook for analysis and visualization.
docs/er_diagrams/: ER diagram definitions for Star and Snowflake schemas.
diagrams/: Additional visualizations.

Setup

Install dependencies:pip install pandas numpy matplotlib

Ensure Python 3.8+ is installed.

Running the Pipeline

Run the pipeline to generate data and create schemas:python scripts/run_pipeline.py

Open notebooks/retail_modeling.ipynb in Jupyter to explore data and visualizations:jupyter notebook notebooks/retail_modeling.ipynb

ER Diagrams

Use dbdiagram.io to import docs/er_diagrams/star_schema.dbd and docs/er_diagrams/snowflake_schema.dbd.
Export diagrams as PNG/PDF to docs/er_diagrams/.

SCD Implementation

Type 1: Overwrites customer address (e.g., customer_id=1).
Type 2: Tracks historical customer data with start_date, end_date, and is_active (e.g., customer_id=2).
#   r e t a i l _ d a t a _ m o d e l i n g  
 