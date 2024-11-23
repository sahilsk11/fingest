import uuid
import pandas as pd
from baml_client.globals import reset_baml_env_vars
import sf_import
import passwords
from dotenv import load_dotenv
import os
import csv
load_dotenv()

reset_baml_env_vars(dict(os.environ))

sf = sf_import.SnowflakeImportEngine(
    user=passwords.get_snowflake_user(),
    password=passwords.get_snowflake_password(),
    account=passwords.get_snowflake_account(),
    schema=passwords.get_snowflake_schema(),
    database=passwords.get_snowflake_database()
)

# print(sf.create_import_run("test", "test", "test"))

# Read CSV, stopping at the first empty line
rows = []
with open("sample_data/amex.csv", 'r') as f:
    reader = csv.reader(f)
    for row in reader:
        if any(cell.strip() for cell in row):
            rows.append(row)
        else:
            break

# Convert to DataFrame and remove any remaining empty rows
csv_as_df = pd.DataFrame(rows[1:], columns=rows[0]).dropna(how='all').reset_index(drop=True)

sf.import_csv(csv_as_df, "AMEX")

# # sf.import_csv("test.csv")
# sf.close()