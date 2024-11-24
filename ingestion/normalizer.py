import datetime
import json
from typing import Optional, Tuple
import uuid
import pandas as pd
import psycopg2  # type: ignore
from snowflake.connector import SnowflakeConnection

from baml_client.types import AccountType, DataType
from baml_client import b
from sf_import import ImportTableRegistry, SnowflakeWrapper


def get_pg_table_name(account_type: AccountType, data_type: DataType) -> Optional[str]:
    return {
        ("bank", "transaction"): "bank_account_transaction",
        ("brokerage", "transaction"): "brokerage_account_transaction",
    }.get((account_type.value.lower(), data_type.value.lower()), None)


def describe_pg_table(pg_table: str, pg_conn: psycopg2.extensions.connection) -> dict[str, dict]:
    cursor = pg_conn.cursor()
    cursor.execute(
        f"""
            SELECT 
                LOWER(c.column_name) AS column_name,
                c.data_type,
                CASE 
                    WHEN c.data_type = 'USER-DEFINED' THEN 
                        string_agg(UPPER(e.enumlabel), ', ') 
                    ELSE '' 
                END AS enum_values
            FROM 
                information_schema.columns c
            LEFT JOIN 
                pg_type t ON c.udt_name = t.typname
            LEFT JOIN 
                pg_enum e ON t.oid = e.enumtypid
            WHERE 
                c.table_name = '{pg_table}'
            GROUP BY 
                c.column_name, c.data_type;
            """
    )

    result = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    result_with_headers = [dict(zip(columns, row)) for row in result]
    out = {}
    for col_data in result_with_headers:
        r = {
            "data_type": col_data["data_type"]
        }
        if col_data["enum_values"]:
            r["enum_values"] = col_data["enum_values"].split(", ")
        out[col_data["column_name"]] = r
    return out


def get_or_generate_normalization_pipeline(
    sf_conn: SnowflakeWrapper,
    import_table_registry: ImportTableRegistry,
    inserted_data: pd.DataFrame,
) -> Tuple[str, Optional[str]]:
    existing_pipeline: Optional[str] = None
    existing_feedback_or_error: Optional[str] = None

    existing_version_id = import_table_registry.get(
        "versioned_normalization_pipeline_id"
    )
    if existing_version_id:
        existing = sf_conn.get_normalization_pipeline(existing_version_id)

        if existing:
            existing_pipeline, existing_feedback_or_error = existing
            if not existing_feedback_or_error:
                return existing_pipeline, None

    (account_type, data_type) = (
        import_table_registry["account_type"],
        import_table_registry["data_type"],
    )

    pg_table_name = get_pg_table_name(account_type, data_type)
    if not pg_table_name:
        raise ValueError(
            f"No matching table found for account type {account_type.value} and data type {data_type.value}"
        )
    
    pg_conn = psycopg2.connect(
        host="localhost", database="postgres", user="postgres", password="postgres", port="5441"
    )
    pg_table_attrs = describe_pg_table(pg_table_name, pg_conn=pg_conn)

    # at this point, we know what the output format should
    # look like based on pg_table_attrs, and we know
    # what we have already based on inserted_data
    # now, we need LLM to figure out a pipeline that makes
    # sense, validate it, save it, and apply it

    final_pipeline, err = generate_pipeline(
        import_table_registry["import_table_registry_id"],
        sf_conn,
        pg_table_name,
        pg_table_attrs,
        inserted_data,
        existing_pipeline,
        existing_feedback_or_error,
        existing_version_id=existing_version_id,
    )

    return final_pipeline, err


def generate_pipeline(
    import_table_registry_id: uuid.UUID,
    sf_conn: SnowflakeWrapper,
    pg_table_name: str,
    pg_table_attrs: dict,
    inserted_data: pd.DataFrame,
    existing_pipeline: Optional[str] = None,
    existing_feedback_or_error: Optional[str] = None,
    existing_version_id: Optional[uuid.UUID] = None,
) -> Tuple[str, Optional[str]]:
    remaining_runs = 1

    # remove primary key from pg_table_attrs
    del pg_table_attrs[pg_table_name.lower()+"_id"]
    del pg_table_attrs["import_run_id"]
    if "created_at" in pg_table_attrs:
        del pg_table_attrs["created_at"]
    if "updated_at" in pg_table_attrs:
        del pg_table_attrs["updated_at"]

    current, error = existing_pipeline, existing_feedback_or_error

    print(inserted_data)

    inserted_data_json = inserted_data.to_json()

    while (not current or error) and remaining_runs > 0:
        existing_pipeline = None
        current = b.GeneratePipeline(
            pg_table_name,
            pg_table_attrs_json=json.dumps(pg_table_attrs),
            sample_data_json=inserted_data_json,
            existing_pipeline=existing_pipeline,
            existing_feedback_or_error=existing_feedback_or_error,
        )
        if current.startswith("```python\n"):
            current = current[len("```python\n"):]
        if current.endswith("\n```"):
            current = current[:-len("\n```")]
        current = current.strip()
        print(current)
        error = None
        try:
            validate_pipeline(current, inserted_data, pg_table_attrs)
        except Exception as e:
            error = repr(e)

        new_version_id = sf_conn.save_pipeline(import_table_registry_id, current, error, previous_version_id=existing_version_id)
        existing_version_id = new_version_id
        remaining_runs -= 1

    if not current:
        raise ValueError(f"Failed to generate pipeline after {remaining_runs} attempts")

    return current, error

def apply_pipeline(pipeline_code: str, inserted_data: pd.DataFrame) -> pd.DataFrame:
    # Create a local scope for exec to define transformed_data and include inserted_data
    local_scope = {'inserted_data': inserted_data, 'pd': pd, 'datetime': datetime}
    exec(pipeline_code, {}, local_scope)  # Execute the pipeline code in a restricted scope
    transformed_data = local_scope.get('transformed_data')  # Retrieve transformed_data from local scope

    if transformed_data is None or not isinstance(transformed_data, pd.DataFrame):
        raise ValueError("Pipeline did not produce transformed_data")
    
    return transformed_data

def validate_pipeline(pipeline_code: str, inserted_data: pd.DataFrame, pg_table_attrs: dict) -> pd.DataFrame:
    transformed_data = apply_pipeline(pipeline_code, inserted_data)
    
    print(transformed_data)
    print(transformed_data.to_json())

    # ensure that all columns are present in the output
    for col in pg_table_attrs:
        if col not in transformed_data.columns:
            raise ValueError(f"Column {col} not found in transformed_data")
        
    # check for extra columns
    extra_cols = set(transformed_data.columns) - set(pg_table_attrs.keys())
    if extra_cols:
        raise ValueError(f"Extra columns found in transformed_data: {extra_cols}")

    return transformed_data


def normalize_data(sf_conn: SnowflakeWrapper, import_run_id: uuid.UUID) -> pd.DataFrame:
    import_run = sf_conn.get_import_run(import_run_id)
    if not import_run:
        raise ValueError(f"Import run with id {import_run_id} not found")

    inserted_data = sf_conn.get_inserted_data(import_run["TABLE_NAME"], import_run_id)
    if inserted_data is None:
        raise ValueError(f"No inserted data found for table {import_run['TABLE_NAME']}")

    attr = sf_conn.get_import_table_attributes(import_run["TABLE_NAME"])
    if not attr:
        raise ValueError(f"No attributes found for table {import_run['TABLE_NAME']}")

    pipeline, err = get_or_generate_normalization_pipeline(
        sf_conn,
        attr,
        inserted_data,
    )
    if err:
        raise ValueError(f"Error in pipeline: {err}")

    # i feel like we should validate the pipeline here
    transformed = apply_pipeline(pipeline, inserted_data)

    return transformed
