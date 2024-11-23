from typing import Tuple, List, Optional
import uuid
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from baml_client import b
import re

from baml_client.types import AccountType

class SnowflakeImportEngine:
    def __init__(self, user: str, password: str, account: str, database: str, schema: str):
        self.conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
            database=database,
            schema=schema,
        )

    def close(self):
        self.conn.close()

    def create_import_run(self, category: str, origin: str, error_message: str) -> uuid.UUID:
        cursor = self.conn.cursor()
        import_id = uuid.uuid4()
        cursor.execute(
            "INSERT INTO import_run (import_run_id, category, created_at, error_message, origin) VALUES (%s, %s, current_timestamp(), %s, %s)",
            (str(import_id), category, error_message, origin),
        )
        return import_id
    
    def _csv_format_matches_existing_table(self, headers: List[str], existing_import_table_names: List[str]) -> Optional[str]:
        cursor = self.conn.cursor()
        # For each table, check if columns match
        for table_name in existing_import_table_names:
            try:
                cursor.execute(f"DESC TABLE {table_name}")
                table_columns: List[str] = {col[0].lower() for col in cursor.fetchall()}

                # the CSV headers should be a subset of the table columns
                csv_headers = {h.lower() for h in headers}
                if csv_headers.issubset(table_columns):
                    cursor.close()
                    return table_name

            except snowflake.connector.errors.ProgrammingError:
                # Skip tables we can't access or don't exist
                continue
        cursor.close()
        return None

    def import_csv(self, csv_as_df: pd.DataFrame, import_run_id: uuid.UUID, source_institution: str) -> bool:
        # Sanitize headers to only contain alphanumeric and underscore characters, and make uppercase
        headers = [
            re.sub(r"[^a-zA-Z0-9_]", "_", col).upper()
            for col in csv_as_df.columns
        ]
        csv_as_df.columns = headers

        cursor = self.conn.cursor()
        cursor.execute((
            "select * from import_table_registry" + 
            f" where source_institution = '{source_institution}'" + 
            f" and file_source_format = 'CSV'"
        ))
        import_tables: List[Tuple] = cursor.fetchall()
        import_table_names = [t[1] for t in import_tables]


        matching_table = (
            self._csv_format_matches_existing_table(headers, import_table_names)
            or
            self.create_import_table_from_csv(csv_as_df, import_table_names, source_institution)
        )

        row_ids = [str(uuid.uuid4()) for _ in range(len(csv_as_df))]
        import_run_ids = [str(import_run_id)] * len(csv_as_df)
        csv_as_df.insert(0, f"{matching_table}_ID", row_ids)
        csv_as_df.insert(1, "IMPORT_RUN_ID", import_run_ids)
        csv_as_df.columns = csv_as_df.columns.str.upper()

        success, _, nrows, _ = write_pandas(
            self.conn, csv_as_df, matching_table, auto_create_table=False
        )
        print(f"Inserted {nrows} rows into existing table {matching_table}")

        cursor.close()
        return success

    def create_import_table_from_csv(self, csv_as_df: pd.DataFrame, import_table_names: list[str], source_institution: str) -> str:
        # assumes table with existing format
        # does not exist
        headers: List[str] = list(csv_as_df.columns)
        rows = [list(row) for row in csv_as_df.head(2).to_numpy()]
        if len(rows) == 0:
            raise ValueError("No data found in CSV file")
        

        categories = b.CategorizeCsvData(source_institution, headers, rows)
        account_type = categories.account_type
        data_type = categories.data_type

        # Create unique table name from source and category
        table_name = f"{source_institution}_{account_type.value}_{data_type.value}".upper()
        counter = 1
        
        # Ensure unique table name
        while table_name in import_table_names:
            table_name = f"{table_name}_{counter}"
            counter += 1

        # Build CREATE TABLE statement
        create_stmt = (
            f"CREATE TABLE FINGEST.PUBLIC.{table_name} (\n" +
            f"{table_name}_ID VARCHAR(36) not null,\n"
            "IMPORT_RUN_ID VARCHAR(36) not null,\n"
        )
        
        # Add columns based on headers
        for header in headers:            
            # Check if all values in column are numeric
            is_numeric = True
            for val in csv_as_df[header].dropna():
                try:
                    float(val)
                except (ValueError, TypeError):
                    is_numeric = False
                    break
                    
            # Use NUMERIC type if all values are numeric, otherwise VARCHAR
            col_type = "NUMERIC" if is_numeric else "VARCHAR(255)"
            create_stmt += f"    {header} {col_type},\n"
        
        # Remove trailing comma and close statement
        create_stmt = create_stmt.rstrip(",\n") + "\n)"

        print(create_stmt)

        # Execute create table within transaction
        cursor = self.conn.cursor()
        try:
            cursor.execute("BEGIN")
            
            cursor.execute(create_stmt)
            
            # Register the new table
            registry_id = str(uuid.uuid4())
            cursor.execute(
                """
                INSERT INTO import_table_registry 
                (import_table_registry_id, table_name, source_institution, account_type, data_type, file_source_format)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (registry_id, table_name, source_institution, account_type.value.upper(), data_type.value.upper(), 'CSV')
            )
            cursor.execute("COMMIT")
            cursor.close()
            return table_name
            
        except Exception as e:
            cursor.execute("ROLLBACK")
            cursor.close()
            raise e

       