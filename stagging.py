from connector import cs
import csv


cs.execute("USE SCHEMA DWH_STG")

# List of local file paths
file_paths = [
    r'C:\Users\binod.dhamala\Desktop\ETL\country.csv',
    r'C:\Users\binod.dhamala\Desktop\ETL\customer.csv',
    r'C:\Users\binod.dhamala\Desktop\ETL\product.csv',
    r'C:\Users\binod.dhamala\Desktop\ETL\region.csv',
    r'C:\Users\binod.dhamala\Desktop\ETL\sales.csv',
    r'C:\Users\binod.dhamala\Desktop\ETL\store.csv',
    r'C:\Users\binod.dhamala\Desktop\ETL\subcategory.csv',
    r'C:\Users\binod.dhamala\Desktop\ETL\category.csv'
]

# Snowflake stage name
stage_name = 'MY_STAGE'

# Loop through each file path
for local_file_path in file_paths:
    # Use PUT command to upload the file to the stage
    put_sql = f"""
    PUT file://{local_file_path} @{stage_name}
    """
    # Execute the PUT command
    cs.execute(put_sql)