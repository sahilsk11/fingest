from typing import Tuple, List, Optional
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

def get_connection(user: str, password: str, account: str, database: str, schema: str) -> snowflake.connector.SnowflakeConnection:
    conn = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        # warehouse=warehouse,
        database=database,
        schema=schema,
    )
    return conn

def import_csv(file_path: str, snowflake_conn: snowflake.connector.SnowflakeConnection) -> bool:
    """
    Import a CSV file into Snowflake, either matching an existing table or creating a new one.

    Args:
        file_path: Path to the CSV file to import
        snowflake_conn: Active Snowflake connection object

    Returns:
        bool: Success status of the import operation
    """
    # Read CSV file
    df: pd.DataFrame = pd.read_csv(file_path)
    headers: List[str] = list(df.columns)

    # Get list of existing tables
    cursor: snowflake.connector.SnowflakeCursor = snowflake_conn.cursor()
    cursor.execute("SHOW TABLES")
    existing_tables: List[Tuple] = cursor.fetchall()

    # For each table, check if columns match
    matching_table: Optional[str] = None
    for table in existing_tables:
        table_name: str = table[1]  # Table name is in second position
        try:
            cursor.execute(f"DESC TABLE {table_name}")
            table_columns: List[str] = [col[0].lower() for col in cursor.fetchall()]

            if sorted(table_columns) == sorted([h.lower() for h in headers]):
                matching_table = table_name
                break
        except snowflake.connector.errors.ProgrammingError:
            # Skip tables we can't access or don't exist
            continue

    if matching_table:
        # If matching table found, insert data
        success, nchunks, nrows, _ = write_pandas(
            snowflake_conn, df, matching_table, auto_create_table=False
        )
        print(f"Inserted {nrows} rows into existing table {matching_table}")
    else:
        # Create new table with sanitized name based on file name
        import os
        import re

        base_name: str = os.path.splitext(os.path.basename(file_path))[0]
        # Sanitize name to valid SQL table name
        new_table: str = re.sub(r"[^a-zA-Z0-9_]", "_", base_name).upper()

        success, nchunks, nrows, _ = write_pandas(
            snowflake_conn, df, new_table, auto_create_table=True
        )
        print(f"Created new table {new_table} and inserted {nrows} rows")

    cursor.close()
    return success
