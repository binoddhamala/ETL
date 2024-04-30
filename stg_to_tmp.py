from connector import cs

# using the schema of temp

cs.execute("USE SCHEMA DWH_TEMP")
cs.execute("TRUNCATE TABLE TEMP_D_CNTRY")

# loading the file from stg to temp of country
query = """ INSERT INTO TEMP_D_CNTRY(id,country_desc)
SELECT DWH_STG.STG_D_CNTRY.id as id,
DWH_STG.STG_D_CNTRY.country_desc as country_desc
FROM DWH_STG.STG_D_CNTRY
"""
cs.execute(query)

# loading the file from stg to temp of region
cs.execute("TRUNCATE TABLE TEMP_D_REGN")

query1 = """ INSERT INTO TEMP_D_REGN(ID,COUNTRY_KEY,REGION_DESC )
     SELECT STG_D_REGN.id as id,TEMP_D_CNTRY.COUNTRY_KEY as COUNTRY_KEY, STG_D_REGN.REGION_DESC as REGION_DESC
     FROM DWH_STG.STG_D_REGN join TEMP_D_CNTRY on
     STG_D_REGN.ID = TEMP_D_CNTRY.ID

    """
cs.execute(query1)
# loading the file from stg to temp of store
cs.execute("TRUNCATE TABLE TEMP_D_STR")

query2 = """ INSERT INTO TEMP_D_STR(ID,REGION_KEY,STORE_DESC )
        SELECT STG_D_STR.id as id,TEMP_D_REGN.REGION_KEY as region_key,STG_D_STR.STORE_DESC as STORE_DESC
        FROM DWH_STG.STG_D_STR join TEMP_D_REGN on
        STG_D_STR.id = TEMP_D_REGN.id
        """
cs.execute(query2)

# loading the file from stg to temp of category
cs.execute("TRUNCATE TABLE TEMP_D_CATEGORY")

query3 = """ INSERT INTO TEMP_D_CATEGORY(ID,CATEGORY_DESC)
SELECT STG_D_CATEGORY.id as id,STG_D_CATEGORY.CATEGORY_DESC as CATEGORY_DESC FROM DWH_STG.STG_D_CATEGORY

"""
cs.execute(query3)

# loading the file from stg to temp of subcategory
cs.execute("TRUNCATE TABLE TEMP_D_SUBCATEGORY")

query4 = """ INSERT INTO TEMP_D_SUBCATEGORY(ID,CATEGORY_KEY,SUBCATEGORY_DESC)
      SELECT STG_D_SUBCATEGORY.id as id,TEMP_D_CATEGORY.CATEGORY_KEY as CATEGORY_KEY,
      STG_D_SUBCATEGORY.SUBCATEGORY_DESC as SUBCATEGORY_DESC
      FROM DWH_STG.STG_D_SUBCATEGORY join DWH_TEMP.TEMP_D_CATEGORY on
      STG_D_SUBCATEGORY.id = TEMP_D_CATEGORY.id


    """
cs.execute(query4)

# loading the file from stg to temp of product
cs.execute("TRUNCATE TABLE TEMP_D_PRD")

query5 = """ INSERT INTO TEMP_D_PRD(ID,SUBCATEGORY_KEY,PRODUCT_DESC)
        SELECT STG_D_PRD.id as id,TEMP_D_SUBCATEGORY.SUBCATEGORY_KEY as SUBCATEGORY_KEY,
        STG_D_PRD.PRODUCT_DESC as PRODUCT_DESC
         FROM DWH_STG.STG_D_PRD join TEMP_D_SUBCATEGORY on
         STG_D_PRD.id =TEMP_D_SUBCATEGORY.id 

        """
cs.execute(query5)

# loading the file from stg to temp of customer
cs.execute("TRUNCATE TABLE TEMP_D_CUST")

query6 = """ INSERT INTO TEMP_D_CUST(ID,customer_first_name,customer_middle_name,customer_last_name,customer_address)
        SELECT STG_D_CUST.id as id,STG_D_CUST.customer_first_name as customer_first_name,
        STG_D_CUST.customer_middle_name as customer_middle_name,STG_D_CUST.customer_last_name as
        customer_last_name, STG_D_CUST.customer_address as customer_address
        FROM DWH_STG.STG_D_CUST
        """
cs.execute(query6)

# loading the file from stg to temp of sales
cs.execute("TRUNCATE TABLE TEMP_F_SLS_T")

# query7 = """ INSERT INTO TEMP_F_SLS_T(
#
#             id ,store_id ,product_id,customer_id, store_key , product_key ,customer_key , transaction_time,
#             quantity ,amount,  discount)
#             SELECT
#             STG_F_SLS_T.id as id,
#             STG_D_STR.store_id as store_id,
#             STG_D_PRD.product_id as product_id,
#             STG_D_CUST.customer_id as customer_id,
#             TEMP_D_STR.store_key as store_key,
#             TEMP_D_PRD.product_key as product_key,
#             TEMP_D_CUST.customer_key as customer_key,
#             STG_F_SLS_T.transaction_time as transaction_time,
#             STG_F_SLS_T.quantity as quantity,
#             STG_F_SLS_T.amount as amount,
#             STG_F_SLS_T.discount as discount
#             FROM DWH_STG.STG_F_SLS_T STG_F_SLS_T
#             join DWH_TEMP.TEMP_D_STR ST on S.store_id = ST.store_id
#             join TEMP_D_PRD P on S.product_id = p.id
#             join TEMP_D_CUST C on S.customer_id = C.id
#
#             """
# cs.execute(query7)

# loading the file from stg to temp of sales
cs.execute("TRUNCATE TABLE TEMP_F_SLS_T")

query7 = """ INSERT INTO TEMP_F_SLS_T(

            id ,store_key , product_key ,customer_key , transaction_time,
            quantity ,amount,  discount)
            SELECT 
            STG_F_SLS_T.id as id,
            TEMP_D_STR.store_key as store_key,
            TEMP_D_PRD.product_key as product_key,
            TEMP_D_CUST.customer_key as customer_key,
            STG_F_SLS_T.transaction_time as transaction_time,
            STG_F_SLS_T.quantity as quantity,
            STG_F_SLS_T.amount as amount,
            STG_F_SLS_T.discount as discount
            FROM DWH_STG.STG_F_SLS_T STG_F_SLS_T
            join DWH_TEMP.TEMP_D_STR TEMP_D_STR on STG_F_SLS_T.store_id = TEMP_D_STR.id
            join DWH_TEMP.TEMP_D_PRD TEMP_D_PRD on STG_F_SLS_T.product_id = TEMP_D_PRD.id
            join DWH_TEMP.TEMP_D_CUST TEMP_D_CUST on STG_F_SLS_T.customer_id = TEMP_D_CUST.id

            """
cs.execute(query7)