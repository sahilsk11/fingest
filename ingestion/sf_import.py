from typing import Set, Tuple, List, Optional
import uuid
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from baml_client import b
import re

from baml_client.types import AccountType, DataType

from typing import TypedDict, Optional

class ImportTableRegistry(TypedDict, total=False):
    import_table_registry_id: uuid.UUID
    table_name: str
    source_institution: str
    account_type: AccountType
    data_type: DataType
    file_source_format: str
    versioned_normalization_pipeline_id: Optional[uuid.UUID]



class SnowflakeWrapper:
    def __init__(
        self, user: str, password: str, account: str, database: str, schema: str
    ):
        self.conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
            database=database,
            schema=schema,
        )

    def close(self):
        self.conn.close()

    def get_import_run(self, import_run_id: uuid.UUID) -> Optional[dict]:
        cursor = self.conn.cursor()
        cursor.execute(
            f"SELECT * FROM FINGEST.PUBLIC.import_run WHERE import_run_id = '{import_run_id}'"
        )
        result = cursor.fetchone()
        if not result:
            cursor.close()
            return None
        columns = [desc[0] for desc in cursor.description]
        cursor.close()

        return dict(zip(columns, result))

    def create_import_run(
        self,
        import_id: uuid.UUID,
        source_institution: str,
        account_type: AccountType,
        data_type: DataType,
        file_source_format: str,
        table_name: str,
        s3_bucket: Optional[str] = None,
        s3_path: Optional[str] = None,
        file_name: Optional[str] = None,
    ) -> None:
        # todo - add error handling/return value
        cursor = self.conn.cursor()
        cursor.execute(
            """INSERT INTO import_run (
                import_run_id, 
                source_institution,
                account_type, 
                data_type, 
                file_source_format,
                table_name,
                s3_bucket,
                s3_path,
                file_name,
                created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, current_timestamp())""",
            (
                str(import_id),
                source_institution,
                account_type.value,
                data_type.value,
                file_source_format,
                table_name,
                s3_bucket,
                s3_path,
                file_name,
            ),
        )
        cursor.close()

    def get_table_headers(self, table_name: str) -> Optional[Set[str]]:
        cursor = self.conn.cursor()
        cursor.execute(f"DESC TABLE {table_name}")
        result = cursor.fetchone()
        if not result:
            return None

        cursor.execute(f"DESC TABLE {table_name}")
        out = {col[0] for col in cursor.fetchall()}
        cursor.close()
        return out
    
    def get_inserted_data(self, table_name: str, import_run_id: uuid.UUID) -> Optional[pd.DataFrame]:
        cursor = self.conn.cursor()
        cursor.execute(
            f"SELECT * FROM FINGEST.PUBLIC.{table_name} WHERE import_run_id = '{import_run_id}'"
        )
        result = cursor.fetchall()
        cursor.close()
        if not result:
            return None
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(result, columns=columns)
        return df
    
    def get_import_table_names(self, source_institution: str, file_source_format: str) -> List[str]:
        cursor = self.conn.cursor()
        cursor.execute(
            (
                "select table_name from import_table_registry"
                + f" where source_institution = '{source_institution}'"
                + f" and file_source_format = '{file_source_format}'"
            )
        )
        cursor.close()
        import_tables = cursor.fetchall()
        return [t[0] for t in import_tables]
    
    def get_import_table_attributes(self, table_name: str) -> Optional[ImportTableRegistry]:
        cursor = self.conn.cursor()
        cursor.execute(
            f"SELECT account_type, data_type, file_source_format, versioned_normalization_pipeline_id FROM import_table_registry WHERE table_name = '{table_name}'"
        )
        table_attributes = cursor.fetchone()
        cursor.close()
        if not table_attributes:
            return None
        return {
            "account_type": AccountType(table_attributes[0]),
            "data_type": DataType(table_attributes[1]),
            "file_source_format": table_attributes[2],
            "versioned_normalization_pipeline_id": table_attributes[3],
        }
    def get_normalization_pipeline(self, versioned_normalization_pipeline_id: uuid.UUID) -> Optional[Tuple[str, str]]:
        cursor = self.conn.cursor()
        cursor.execute(
            f"SELECT python_code, feedback_or_error FROM versioned_normalization_pipeline WHERE versioned_normalization_pipeline_id = '{versioned_normalization_pipeline_id}'"
        )
        result = cursor.fetchone()
        cursor.close()
        if not result:
            return None

        return result[0], result[1]
    
    def save_pipeline(self, versioned_normalization_pipeline_id: uuid.UUID, python_code: str, feedback_or_error: Optional[str] = None) -> None:
        cursor = self.conn.cursor()
        cursor.execute(
            f"UPDATE versioned_normalization_pipeline SET python_code = %s, feedback_or_error = %s WHERE versioned_normalization_pipeline_id = %s",
            (python_code, feedback_or_error, versioned_normalization_pipeline_id)
        )
        cursor.close()

class SnowflakeImportEngine:
    def __init__(
        self,
        user: str,
        password: str,
        account: str,
        database: str,
        schema: str,
        sf_wrapper: SnowflakeWrapper
    ):
        self.conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
            database=database,
            schema=schema,
        )
        self.sf_wrapper = sf_wrapper

    def create_import_run(
        self,
        source_institution: str,
        account_type: AccountType,
        data_type: DataType,
        file_source_format: str,
        table_name: str,
        s3_bucket: Optional[str] = None,
        s3_path: Optional[str] = None,
        file_name: Optional[str] = None,
    ) -> uuid.UUID:
        import_id = uuid.uuid4()
        self.sf_wrapper.create_import_run(
            import_id,
            source_institution,
            account_type,
            data_type,
            file_source_format,
            table_name,
            s3_bucket,
            s3_path,
            file_name,
        )
        return import_id

    def _csv_format_matches_existing_table(
        self, headers: List[str], existing_import_table_names: List[str]
    ) -> Optional[str]:
        # For each table, check if columns match
        for table_name in existing_import_table_names:
            try:
                table_headers = self.sf_wrapper.get_table_headers(table_name)
                if not table_headers:
                    continue
                table_columns = {h.lower() for h in table_headers}

                # the CSV headers should be a subset of the table columns
                csv_headers = {h.lower() for h in headers}
                if csv_headers.issubset(table_columns):
                    return table_name

            except snowflake.connector.errors.ProgrammingError:
                # Skip tables we can't access or don't exist
                continue
        return None

    def import_csv(self, csv_as_df: pd.DataFrame, source_institution: str) -> bool:
        # Sanitize headers to only contain alphanumeric and underscore characters, and make uppercase
        headers = [
            re.sub(r"[^a-zA-Z0-9_]", "_", col).upper() for col in csv_as_df.columns
        ]
        csv_as_df.columns = pd.Index(headers)

        # todo - could optimize this so we don't
        # need to get the attributes later
        import_table_names = self.sf_wrapper.get_import_table_names(source_institution, "CSV")

        matching_table = self._csv_format_matches_existing_table(
            headers, import_table_names
        ) or self.create_import_table_from_csv(
            csv_as_df, import_table_names, source_institution
        )

        table_attributes = self.sf_wrapper.get_import_table_attributes(matching_table)
        if not table_attributes:
            raise ValueError(f"No attributes found for table {matching_table}")

        import_run_id = self.create_import_run(
            source_institution,
            table_attributes["account_type"],
            table_attributes["data_type"],
            "CSV",
            matching_table,
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

        return success

    def create_import_table_from_csv(
        self,
        csv_as_df: pd.DataFrame,
        import_table_names: list[str],
        source_institution: str,
    ) -> str:
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
        table_name = (
            f"{source_institution}_{account_type.value}_{data_type.value}".upper()
        )
        counter = 1

        # Ensure unique table name
        while table_name in import_table_names:
            table_name = f"{table_name}_{counter}"
            counter += 1

        # Build CREATE TABLE statement
        create_stmt = (
            f"CREATE TABLE FINGEST.PUBLIC.{table_name} (\n"
            + f"{table_name}_ID VARCHAR(36) not null,\n"
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
                (
                    registry_id,
                    table_name,
                    source_institution,
                    account_type.value.upper(),
                    data_type.value.upper(),
                    "CSV",
                ),
            )
            cursor.execute("COMMIT")
            cursor.close()
            return table_name

        except Exception as e:
            cursor.execute("ROLLBACK")
            cursor.close()
            raise e
