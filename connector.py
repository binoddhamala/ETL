import snowflake.connector
from credentials import MyCredentials

# Establishing the connection and setting up cursor
ctx = snowflake.connector.connect(
    user=MyCredentials.get('user'),
    password=MyCredentials.get('password'),
    account=MyCredentials.get('account'),
    warehouse='COMPUTE_WH',
    database='ETL_TASK'
)
cs = ctx.cursor()