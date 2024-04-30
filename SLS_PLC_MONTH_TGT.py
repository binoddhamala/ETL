from connector import cs
from datetime import datetime

current_time = datetime.now()
#Using the sales target schema
cs.execute("USE SCHEMA DWH_TGT")

cs.execute("""
CREATE OR REPLACE TABLE DWH_F_BHATBHATENI_AGG_SLS_PLC_MONTH_T(

                product_key NUMBER NOT NULL REFERENCES DWH_TEMP.TEMP_D_PRD(product_key),
                store_key NUMBER NOT NULL REFERENCES DWH_TEMP.TEMP_D_STR(store_key),
                category_key NUMBER NOT NULL REFERENCES DWH_TEMP.TEMP_D_CATEGORY(category_key),
                month_key NUMBER NOT NULL,
                ROW_INSRT_TMS TIMESTAMP_NTZ,
                ROW_UPDT_TMS TIMESTAMP_NTZ,
                quantity NUMBER,
                amount NUMBER(10,2),
                discount NUMBER(10,2)    

                )
""")

insert = f"""
        INSERT INTO DWH_TGT.DWH_F_BHATBHATENI_AGG_SLS_PLC_MONTH_T
        (product_key, store_key, category_key, month_key, quantity, amount, discount, ROW_INSRT_TMS, ROW_UPDT_TMS)
            (SELECT tmp.product_key, tmp.store_key, tmp.category_key, tmp.month_key, tmp.quantity, tmp.amount,
             tmp.discount, '{current_time}', '{current_time}'
            FROM DWH_TEMP.DWH_F_BHATBHATENI_AGG_SLS_PLC_MONTH tmp)
            """

cs.execute(insert)