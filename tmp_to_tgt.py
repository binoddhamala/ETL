from connector import cs
from datetime import datetime

#############################################################################################

# now loading data from temp table to tgt table of country
# Use the schema of target table
cs.execute("USE SCHEMA DWH_TGT")

current_time = datetime.now()
update = f"""
                 UPDATE DWH_TGT.DWH_D_CNTRY_LU tgt
                 SET
                 tgt.ID = tmp.ID,
                 tgt.country_DESC = tmp.country_DESC,
                 tgt.ROW_INSRT_TMS = '{current_time}',
                 tgt.ROW_UPDT_TMS = '{current_time}',
                 tgt.OPEN_CLOSE_CD = 'N'
                 FROM DWH_TEMP.TEMP_D_CNTRY tmp
                 WHERE tgt.country_key = tmp.country_key
                 """
cs.execute(update)

insert = f"""
               INSERT INTO DWH_TGT.DWH_D_CNTRY_LU(ID,COUNTRY_KEY, COUNTRY_DESC, OPEN_CLOSE_CD, ROW_INSRT_TMS,ROW_UPDT_TMS)
               SELECT tmp.COUNTRY_KEY,ID, tmp.COUNTRY_DESC,'N', '{current_time}', '{current_time}'
               FROM DWH_TEMP.TEMP_D_CNTRY tmp
               WHERE COUNTRY_KEY NOT IN (SELECT COUNTRY_KEY FROM DWH_TGT.DWH_D_CNTRY_LU);
               """

cs.execute(insert)





# now loading data from temp table to tgt table of region

update1 = f"""
                UPDATE DWH_TGT.DWH_D_REGN_LU tgt
                 SET
                 tgt.ID = tmp.ID,
                 tgt.region_DESC = tmp.region_DESC,
                 tgt.ROW_INSRT_TMS = '{current_time}',
                 tgt.ROW_UPDT_TMS = '{current_time}',
                 tgt.OPEN_CLOSE_CD = 'N'
                 FROM DWH_TEMP.TEMP_D_REGN tmp
                 WHERE tgt.region_key = tmp.region_key
                 """
cs.execute(update1)

insert1 = f"""
               INSERT INTO DWH_TGT.DWH_D_REGN_LU(ID,REGION_KEY,country_key, REGION_DESC, OPEN_CLOSE_CD, ROW_INSRT_TMS,ROW_UPDT_TMS)
               SELECT tmp.REGION_KEY,tmp.ID,tmp.country_key, tmp.REGION_DESC,'N', '{current_time}', '{current_time}'
               FROM DWH_TEMP.TEMP_D_REGN tmp
               WHERE REGION_KEY NOT IN (SELECT REGION_KEY FROM DWH_TGT.DWH_D_REGN_LU);
               """

cs.execute(insert1)

# now loading data from temp table to tgt table of store

update2 = f"""
                UPDATE DWH_TGT.DWH_D_STR_LU tgt
                 SET
                 tgt.ID = tmp.ID,
                 tgt.store_DESC = tmp.store_DESC,
                 tgt.ROW_INSRT_TMS = '{current_time}',
                 tgt.ROW_UPDT_TMS = '{current_time}',
                 tgt.OPEN_CLOSE_CD = 'N'
                 FROM DWH_TEMP.TEMP_D_STR tmp
                 WHERE tgt.store_key = tmp.store_key
                 """
cs.execute(update2)

insert2 = f"""
               INSERT INTO DWH_TGT.DWH_D_STR_LU(ID,STORE_KEY,region_key, STORE_DESC, OPEN_CLOSE_CD, ROW_INSRT_TMS,ROW_UPDT_TMS)
               SELECT tmp.STORE_KEY,tmp.ID,tmp.region_key, tmp.STORE_DESC,'N', '{current_time}', '{current_time}'
               FROM DWH_TEMP.TEMP_D_STR tmp
               WHERE STORE_KEY NOT IN (SELECT STORE_KEY FROM DWH_TGT.DWH_D_STR_LU);
               """

cs.execute(insert2)

# now loading data from temp table to tgt table of category

update3 = f"""
            UPDATE DWH_TGT.DWH_D_CUST_LU tgt
            SET
            tgt.ID = tmp.ID,
            tgt.CUSTOMER_FIRST_NAME = tmp.CUSTOMER_FIRST_NAME,
            tgt.CUSTOMER_MIDDLE_NAME = tmp.CUSTOMER_MIDDLE_NAME,
            tgt.CUSTOMER_LAST_NAME = tmp.CUSTOMER_LAST_NAME,
            tgt.CUSTOMER_ADDRESS = tmp.CUSTOMER_ADDRESS,
            tgt.ROW_INSRT_TMS = '{current_time}',
            tgt.ROW_UPDT_TMS = '{current_time}',
            tgt.OPEN_CLOSE_CD = 'N'
            FROM DWH_TEMP.TEMP_D_CUST tmp
            WHERE tgt.CUSTOMER_KEY = tmp.CUSTOMER_KEY
            """
cs.execute(update3)

insert3 = f"""
            INSERT INTO DWH_TGT.DWH_D_CUST_LU(ID,CUSTOMER_KEY, CUSTOMER_FIRST_NAME, CUSTOMER_MIDDLE_NAME,
            CUSTOMER_LAST_NAME, CUSTOMER_ADDRESS, OPEN_CLOSE_CD, ROW_INSRT_TMS, ROW_UPDT_TMS)
            SELECT tmp.CUSTOMER_KEY, tmp.ID, tmp.CUSTOMER_FIRST_NAME, tmp.CUSTOMER_MIDDLE_NAME, tmp.CUSTOMER_LAST_NAME,
            tmp.CUSTOMER_ADDRESS, 'N', '{current_time}', '{current_time}'
            FROM DWH_TEMP.TEMP_D_CUST tmp
            WHERE tmp.CUSTOMER_KEY NOT IN (SELECT CUSTOMER_KEY FROM DWH_TGT.DWH_D_CUST_LU);
            """

cs.execute(insert3)

# now loading data from temp table to tgt table of category

update4 = f"""
                UPDATE DWH_TGT.DWH_D_CATEGORY_LU tgt
                 SET
                 tgt.ID = tmp.ID,
                 tgt.CATEGORY_DESC = tmp.category_DESC,
                 tgt.ROW_INSRT_TMS = '{current_time}',
                 tgt.ROW_UPDT_TMS = '{current_time}',
                 tgt.OPEN_CLOSE_CD = 'N'
                 FROM DWH_TEMP.TEMP_D_CATEGORY tmp
                 WHERE tgt.category_key = tmp.category_key
                 """
cs.execute(update4)

insert4 = f"""
               INSERT INTO DWH_TGT.DWH_D_CATEGORY_LU(ID,CATEGORY_KEY, CATEGORY_DESC, OPEN_CLOSE_CD, ROW_INSRT_TMS,ROW_UPDT_TMS)
               SELECT tmp.CATEGORY_KEY,tmp.ID, tmp.CATEGORY_DESC,'N', '{current_time}', '{current_time}'
               FROM DWH_TEMP.TEMP_D_CATEGORY tmp
               WHERE CATEGORY_KEY NOT IN (SELECT CATEGORY_KEY FROM DWH_TGT.DWH_D_CATEGORY_LU);
               """

cs.execute(insert4)

# now loading data from temp table to tgt table of subcategory

update5 = f"""
                UPDATE DWH_TGT.DWH_D_SUBCATEGORY_LU tgt
                 SET
                 tgt.ID = tmp.ID,
                 tgt.SUBCATEGORY_DESC = tmp.subcategory_DESC,
                 tgt.ROW_INSRT_TMS = '{current_time}',
                 tgt.ROW_UPDT_TMS = '{current_time}',
                 tgt.OPEN_CLOSE_CD = 'N'
                 FROM DWH_TEMP.TEMP_D_SUBCATEGORY tmp
                 WHERE tgt.subcategory_key = tmp.subcategory_key
                 """
cs.execute(update5)

insert5 = f"""
               INSERT INTO DWH_TGT.DWH_D_SUBCATEGORY_LU(ID,SUBCATEGORY_KEY,CATEGORY_KEY, SUBCATEGORY_DESC, OPEN_CLOSE_CD, ROW_INSRT_TMS,ROW_UPDT_TMS)
               SELECT tmp.SUBCATEGORY_KEY,tmp.ID,tmp.CATEGORY_KEY, tmp.SUBCATEGORY_DESC,'N', '{current_time}', '{current_time}'
               FROM DWH_TEMP.TEMP_D_SUBCATEGORY tmp
               WHERE SUBCATEGORY_KEY NOT IN (SELECT SUBCATEGORY_KEY FROM DWH_TGT.DWH_D_SUBCATEGORY_LU);
               """

cs.execute(insert5)

# now loading data from temp table to tgt table of product

update6 = f"""
                UPDATE DWH_TGT.DWH_D_PRD_LU tgt
                 SET
                 tgt.ID = tmp.ID,
                 tgt.PRODUCT_DESC = tmp.product_DESC,
                 tgt.ROW_INSRT_TMS = '{current_time}',
                 tgt.ROW_UPDT_TMS = '{current_time}',
                 tgt.OPEN_CLOSE_CD = 'N'
                 FROM DWH_TEMP.TEMP_D_PRD tmp
                 WHERE tgt.product_key = tmp.product_key
                 """
cs.execute(update6)

insert6 = f"""
               INSERT INTO DWH_TGT.DWH_D_PRD_LU(ID,SUBCATEGORY_KEY,PRODUCT_KEY, PRODUCT_DESC, OPEN_CLOSE_CD, ROW_INSRT_TMS,ROW_UPDT_TMS)
               SELECT tmp.PRODUCT_KEY,tmp.ID,tmp.SUBCATEGORY_KEY, tmp.PRODUCT_DESC,'N', '{current_time}', '{current_time}'
               FROM DWH_TEMP.TEMP_D_PRD tmp
               WHERE PRODUCT_KEY NOT IN (SELECT PRODUCT_KEY FROM DWH_TGT.DWH_D_PRD_LU);
               """

cs.execute(insert6)

# now loading data from temp table to tgt table of sales

update7 = f"""
                UPDATE DWH_TGT.DWH_F_SLS_T tgt
                 SET
                 tgt.ID = tmp.ID,
                 tgt.quantity = tmp.quantity,
                 tgt.amount = tmp.amount,
                 tgt.discount = tmp.discount,
                 tgt.transaction_time = tmp.transaction_time,
                 tgt.ROW_INSRT_TMS = '{current_time}',
                 tgt.ROW_UPDT_TMS = '{current_time}'
                 FROM DWH_TEMP.TEMP_F_SLS_T tmp
                 WHERE tgt.sales_key = tmp.sales_key
                 """
cs.execute(update7)

insert7 = f"""
               INSERT INTO DWH_TGT.DWH_F_SLS_T(ID,SALES_KEY,PRODUCT_KEY,CUSTOMER_KEY,STORE_KEY,QUANTITY,AMOUNT,DISCOUNT,
               transaction_time, ROW_INSRT_TMS,ROW_UPDT_TMS)
               SELECT tmp.ID,tmp.SALES_KEY,tmp.PRODUCT_KEY,tmp.CUSTOMER_KEY,tmp.STORE_KEY,tmp.quantity,tmp.amount,
               tmp.discount,tmp.transaction_time, '{current_time}', '{current_time}'
               FROM DWH_TEMP.TEMP_F_SLS_T tmp
               WHERE SALES_KEY NOT IN (SELECT SALES_KEY FROM DWH_TGT.DWH_F_SLS_T);
               """

cs.execute(insert7)