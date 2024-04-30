from connector import cs
import csv

cs.execute("USE SCHEMA DWH_STG")

# Snowflake stage name
stage_name = 'DIRECT_LOAD'

# List of tables to load from the stage into the DWH_STG schema
tables_to_load = ['CATEGORY', 'COUNTRY', 'CUSTOMER', 'PRODUCT', 'REGION', 'STORE', 'SUBCATEGORY', 'SALES']

# List of corresponding tables in the DWH_STG schema
load_tables = ['STG_D_CATEGORY', 'STG_D_CNTRY', 'STG_D_CUST', 'STG_D_PRD', 'STG_D_REGN', 'STG_D_STR', 'STG_D_SUBCATEGORY', 'STG_F_SLS_T']

# Truncate each table in the load_tables list
for dwh_table in load_tables:
    truncate_STG = f"TRUNCATE TABLE {dwh_table}"
    cs.execute(truncate_STG)

# Iterate over each pair of tables
for stage_table, dwh_table in zip(tables_to_load, load_tables):
    # Define the SQL command for the current pair of tables
    copy_into_stg = f"""
    COPY INTO {dwh_table}
    FROM @{stage_name}/{stage_table}
    """
    # Execute the COPY INTO command for the current pair of tables
    cs.execute(copy_into_stg)