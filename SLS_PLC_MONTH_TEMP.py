from connector import cs
from datetime import datetime

# Using the schema from temp
cs.execute("USE SCHEMA DWH_TEMP")
current_time = datetime.now()

# create table for sls_plc_month_temp

cs.execute("""
CREATE OR REPLACE TABLE DWH_F_BHATBHATENI_AGG_SLS_PLC_MONTH(

                product_key NUMBER NOT NULL REFERENCES DWH_TEMP.TEMP_D_PRD(product_key),
                store_key NUMBER NOT NULL REFERENCES DWH_TEMP.TEMP_D_STR(store_key),
                category_key NUMBER NOT NULL REFERENCES DWH_TEMP.TEMP_D_CATEGORY(category_key),
                month_key NUMBER NOT NULL,
                quantity NUMBER,
                amount NUMBER(10,2),
                discount NUMBER(10,2)    

                )
""")

cs.execute("TRUNCATE DWH_F_BHATBHATENI_AGG_SLS_PLC_MONTH ")

insert = f"""
       INSERT INTO DWH_F_BHATBHATENI_AGG_SLS_PLC_MONTH(product_key,store_key,category_key,month_key,
       quantity,amount,discount)
        (SELECT p.product_key, l.store_key, c.category_key, month(TRANSACTION_TIME) AS month_key,
         SUM(s.quantity) AS TOTAL_QTY, SUM(s.amount) AS TOTAL_AMT, SUM(s.discount) AS TOTAL_DSCNT
                FROM DWH_TEMP.TEMP_F_SLS_T s
                JOIN DWH_TEMP.TEMP_D_STR l
                ON s.STORE_KEY = l.STORE_KEY
                join DWH_TEMP.TEMP_D_PRD p on
                s.product_key = p.product_key
                JOIN DWH_TEMP.TEMP_D_SUBCATEGORY sc
                ON p.SUBCATEGORY_KEY = sc.SUBCATEGORY_KEY
                JOIN DWH_TEMP.TEMP_D_CATEGORY c
                ON sc.CATEGORY_KEY = c.CATEGORY_KEY
                GROUP BY p.PRODUCT_KEY, l.STORE_KEY, c.CATEGORY_KEY, MONTH_KEY
                ORDER BY MONTH_KEY

                );
"""
cs.execute(insert)

