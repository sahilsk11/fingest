import pandas as pd
import sf_import
import passwords
from dotenv import load_dotenv
import os
load_dotenv()
api_key = os.getenv("OPENAI_API_KEY")
print(api_key)

sf = sf_import.SnowflakeImportEngine(
    user=passwords.get_snowflake_user(),
    password=passwords.get_snowflake_password(),
    account=passwords.get_snowflake_account(),
    schema=passwords.get_snowflake_schema(),
    database=passwords.get_snowflake_database()
)

# print(sf.create_import_run("test", "test", "test"))

csv_as_df = pd.read_csv("test.csv")
sf.create_import_table_from_csv(
    csv_as_df,
    [""],
    "robinhood"
)

# sf.import_csv("test.csv")
sf.close()