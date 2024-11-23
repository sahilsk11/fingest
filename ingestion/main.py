import sf_import
import ingestion.passwords as passwords


sf = sf_import.SnowflakeImportEngine(
    user=passwords.get_snowflake_user(),
    password=passwords.get_snowflake_password(),
    account=passwords.get_snowflake_account(),
    schema=passwords.get_snowflake_schema(),
    database=passwords.get_snowflake_database()
)

print(sf.create_import_run("test", "test", "test"))

# sf.import_csv("test.csv")
sf.close()