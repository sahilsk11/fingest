import sf_import
import passwords


sf = sf_import.SnowflakeImportEngine(
    passwords.get_snowflake_user(),
    passwords.get_snowflake_password(),
    passwords.get_snowflake_account()
)

sf.import_csv("test.csv")
sf.close()