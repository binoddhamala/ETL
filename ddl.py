# Create schema for STG, TMP and TGT (The stg schema will contain all the staging tables and so on)

from connector import cs

#using the database and schema from BHATBHATENI_DBs
# creating the staging schema
cs.execute("CREATE OR REPLACE SCHEMA DWH_STG")

# creating staging table of catagory

cs.execute("""CREATE OR REPLACE TABLE STG_D_CATEGORY (
    id NUMBER,
    category_desc VARCHAR(50),
    PRIMARY KEY(id)
)""")

# creating staging table of subcatagory
cs.execute("""
CREATE or replace  TABLE STG_D_SUBCATEGORY(
    id NUMBER,
    category_id NUMBER,
    subcategory_desc VARCHAR(55),
    PRIMARY KEY(id),
    FOREIGN KEY(category_id) REFERENCES STG_D_CATEGORY(id)
)
""")
# creating staging table of product

cs.execute("""
CREATE or replace  TABLE STG_D_PRD (
    id NUMBER,
    subcategory_id NUMBER,
    product_desc VARCHAR(55),
    PRIMARY KEY(id),
    FOREIGN KEY(subcategory_id) REFERENCES STG_D_SUBCATEGORY(id)
)

""")

# creating staging table of country
cs.execute("""create or replace table STG_D_CNTRY(
    id NUMBER,
    country_desc VARCHAR(256),
    PRIMARY KEY (id)
)
""")
# creating staging table of region
cs.execute("""create or replace  table STG_D_REGN
(
    id NUMBER,
    country_id NUMBER,
    region_desc VARCHAR(256),
    PRIMARY KEY (id),
    FOREIGN KEY (country_id) references STG_D_CNTRY(id) 
)
""")

# creating staging table of store

cs.execute("""create or replace table STG_D_STR
(
    id NUMBER,
    region_id NUMBER,
    store_desc VARCHAR(256),
    PRIMARY KEY (id),
    FOREIGN KEY (region_id) references STG_D_REGN(id) 
)

""")
# creating staging table of customer
cs.execute("""create or replace   table STG_D_CUST
(
    id NUMBER,
    customer_first_name VARCHAR(256),
    customer_middle_name VARCHAR(256),
    customer_last_name VARCHAR(256),
    customer_address VARCHAR(256) ,
    primary key (id)
)
""")

# creating staging table of salaes

cs.execute("""create or replace  table STG_F_SLS_T
(
    id NUMBER,
    store_id NUMBER NOT NULL,
    product_id NUMBER NOT NULL,
    customer_id NUMBER,
    transaction_time TIMESTAMP_NTZ,
    quantity NUMBER,
    amount NUMBER(20,2),
    discount NUMBER(20,2),
    primary key (id),
    FOREIGN KEY (store_id) references STG_D_STR(id),
    FOREIGN KEY (product_id) references STG_D_PRD(id),
    FOREIGN KEY (customer_id) references STG_D_CUST(id)
)
""")
######################################################################
#creating the schema for temp table
cs.execute("CREATE OR REPLACE SCHEMA DWH_TEMP")

# creating temp table of catagory

cs.execute("""CREATE OR REPLACE TABLE TEMP_D_CATEGORY (
    id NUMBER,
    category_key NUMBER NOT NULL AUTOINCREMENT,
    category_desc VARCHAR(50),
    PRIMARY KEY(category_key)
)""")

# creating temp table of subcatagory

cs.execute("""
CREATE or replace  TABLE TEMP_D_SUBCATEGORY (
    id NUMBER,
    subcategory_key NUMBER NOT NULL AUTOINCREMENT,
    category_key NUMBER NOT NULL,
    subcategory_desc VARCHAR(55),
    PRIMARY KEY(subcategory_key),
    FOREIGN KEY(category_key) REFERENCES TEMP_D_CATEGORY(category_key)
)
""")

# creating temp table of product

cs.execute("""
CREATE or replace  TABLE TEMP_D_PRD (
    id NUMBER,
    product_key NUMBER NOT NULL AUTOINCREMENT,
    subcategory_key NUMBER NOT NULL,
    product_desc VARCHAR(55),
    PRIMARY KEY(product_key),
    FOREIGN KEY(subcategory_key) REFERENCES TEMP_D_SUBCATEGORY(subcategory_key)
)

""")

# creating temp table of country

cs.execute("""create or replace table TEMP_D_CNTRY(
    id NUMBER,
    country_key NUMBER NOT NULL AUTOINCREMENT,
    country_desc VARCHAR(256),
    PRIMARY KEY (country_key)
)
""")

# creating temp table of region

cs.execute("""create or replace  table TEMP_D_REGN
(
    id NUMBER,
    region_key NUMBER NOT NULL AUTOINCREMENT,
    country_key NUMBER NOT NULL,
    region_desc VARCHAR(256),
    PRIMARY KEY (region_key),
    FOREIGN KEY (country_key) references TEMP_D_CNTRY(country_key) 
)
""")

# creating temp table of store

cs.execute("""create or replace table TEMP_D_STR
(
    id NUMBER,
    store_key NUMBER NOT NULL AUTOINCREMENT,
    region_key NUMBER NOT NULL,
    store_desc VARCHAR(256),
    PRIMARY KEY (store_key),
    FOREIGN KEY (region_key) references TEMP_D_REGN(region_key) 
)

""")

# creating temp table of customer

cs.execute("""create or replace table TEMP_D_CUST
(
    id NUMBER,
    customer_key NUMBER NOT NULL AUTOINCREMENT,
    customer_first_name VARCHAR(256),
    customer_middle_name VARCHAR(256),
    customer_last_name VARCHAR(256),
    customer_address VARCHAR(256) ,
    primary key (customer_key)
)
""")

# creating temp table of salaes

cs.execute("""create or replace  table TEMP_F_SLS_T
(
    id NUMBER,
    sales_key NUMBER NOT NULL AUTOINCREMENT,
    store_key NUMBER,
    product_key NUMBER,
    customer_key NUMBER,
    transaction_time TIMESTAMP_NTZ,
    quantity NUMBER,
    amount NUMBER(20,2),
    discount NUMBER(20,2),
    primary key (id),
    FOREIGN KEY (store_key) references TEMP_D_STR(store_key),
    FOREIGN KEY (product_key) references TEMP_D_PRD(product_key),
    FOREIGN KEY (customer_key) references TEMP_D_CUST(customer_key)
)
""")

#######################################################################

#Creating Schema for Target
cs.execute("CREATE OR REPLACE SCHEMA DWH_TGT")

#Creating tgt table for category

cs.execute("""CREATE OR REPLACE TABLE DWH_D_CATEGORY_LU (
    id NUMBER,
    category_desc VARCHAR(50),
    category_key NUMBER NOT NULL,
    OPEN_CLOSE_CD VARCHAR(1),
    ROW_INSRT_TMS TIMESTAMP_NTZ,
    ROW_UPDT_TMS TIMESTAMP_NTZ,
    PRIMARY KEY(category_key)
)""")

# creating tgt table of subcatagory

cs.execute("""
CREATE or replace  TABLE DWH_D_SUBCATEGORY_LU (
    id NUMBER,
    category_key NUMBER NOT NULL,
    subcategory_desc VARCHAR(55),
    subcategory_key NUMBER,
    OPEN_CLOSE_CD VARCHAR(1),
    ROW_INSRT_TMS TIMESTAMP_NTZ,
    ROW_UPDT_TMS TIMESTAMP_NTZ,
    PRIMARY KEY(subcategory_key),
    FOREIGN KEY(category_key) REFERENCES DWH_D_CATEGORY_LU(category_key)
)
""")

# creating tgt table of product

cs.execute("""
CREATE or replace  TABLE DWH_D_PRD_LU (
    id NUMBER,
    subcategory_key NUMBER,
    product_desc VARCHAR(55),
    product_key NUMBER NOT NULL,
    OPEN_CLOSE_CD VARCHAR(1),
    ROW_INSRT_TMS TIMESTAMP_NTZ,
    ROW_UPDT_TMS TIMESTAMP_NTZ,
    PRIMARY KEY(product_key),
    FOREIGN KEY(subcategory_key) REFERENCES DWH_D_SUBCATEGORY_LU(subcategory_key)
)

""")

# creating tgt table of country

cs.execute("""create or replace table DWH_D_CNTRY_LU(
    id NUMBER,
    country_desc VARCHAR(256),
    country_key NUMBER NOT NULL,
    OPEN_CLOSE_CD VARCHAR(1),
    ROW_INSRT_TMS TIMESTAMP_NTZ,
    ROW_UPDT_TMS TIMESTAMP_NTZ,
    PRIMARY KEY (country_key)
)
""")
# creating tgt table of region
cs.execute("""create or replace  table DWH_D_REGN_LU
(
    id NUMBER,
    country_key NUMBER,
    region_desc VARCHAR(256),
    region_key NUMBER NOT NULL,
    OPEN_CLOSE_CD VARCHAR(1),
    ROW_INSRT_TMS TIMESTAMP_NTZ,
    ROW_UPDT_TMS TIMESTAMP_NTZ,
    PRIMARY KEY (region_key),
    FOREIGN KEY (country_key) references DWH_D_CNTRY_LU(country_key) 
)
""")

# creating tgt table of store

cs.execute("""create or replace table DWH_D_STR_LU
(
    id NUMBER,
    region_key NUMBER,
    store_desc VARCHAR(256),
    store_key NUMBER NOT NULL,
    OPEN_CLOSE_CD VARCHAR(1),
    ROW_INSRT_TMS TIMESTAMP_NTZ,
    ROW_UPDT_TMS TIMESTAMP_NTZ,
    PRIMARY KEY (store_key),
    FOREIGN KEY (region_key) references DWH_D_REGN_LU(region_key) 
)

""")

# creating tgt table of customer

cs.execute("""create or replace table DWH_D_CUST_LU
(
    id NUMBER,
    customer_key NUMBER NOT NULL,
    customer_first_name VARCHAR(256),
    customer_middle_name VARCHAR(256),
    customer_last_name VARCHAR(256),
    customer_address VARCHAR(256) ,
    OPEN_CLOSE_CD VARCHAR(1),
    ROW_INSRT_TMS TIMESTAMP_NTZ,
    ROW_UPDT_TMS TIMESTAMP_NTZ,
    primary key (customer_key)
)
""")
# creating tgt table of salaes

cs.execute("""create or replace  table DWH_F_SLS_T
(
    id NUMBER,
    sales_key NUMBER NOT NULL,
    store_key NUMBER,
    product_key NUMBER,
    customer_key NUMBER,
    transaction_time TIMESTAMP_NTZ,
    ROW_INSRT_TMS TIMESTAMP_NTZ,
    ROW_UPDT_TMS TIMESTAMP_NTZ,
    quantity NUMBER,
    amount NUMBER(20,2),
    discount NUMBER(20,2),
    primary key (sales_key),
    FOREIGN KEY (store_key) references DWH_D_STR_LU(store_key),
    FOREIGN KEY (product_key) references DWH_D_PRD_LU(product_key),
    FOREIGN KEY (customer_key) references DWH_D_CUST_LU(customer_key)
)
""")
