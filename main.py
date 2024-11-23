import sf_import
import passwords


conn = sf_import.get_connection(
    user=passwords.get_user(),
    password=passwords.get_password(), 
    account=passwords.get_account(),
    database=passwords.get_database(),
    schema=passwords.get_schema()
    # we've configured user to have
    # default warehouse and role
)

sf_import.import_csv("test.csv", conn)