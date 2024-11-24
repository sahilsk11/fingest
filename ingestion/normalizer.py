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
    }.get((account_type.value, data_type.value), None)


def describe_pg_table(pg_table: str, pg_conn: psycopg2.extensions.connection) -> dict:
    cursor = pg_conn.cursor()
    cursor.execute(
        f"select column_name, data_type from INFORMATION_SCHEMA.COLUMNS where table_name ='{pg_table}'"
    )

    result = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    return dict(zip(columns, result))


def get_or_generate_normalization_pipeline(
    sf_conn: SnowflakeWrapper,
    import_table_registry: ImportTableRegistry,
    inserted_data: pd.DataFrame,
) -> Tuple[str, Optional[str]]:
    existing_pipeline: Optional[str] = None
    existing_feedback_or_error: Optional[str] = None
    if import_table_registry["versioned_normalization_pipeline_id"]:
        existing = sf_conn.get_normalization_pipeline(
            import_table_registry["versioned_normalization_pipeline_id"]
        )

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
    pg_table_attrs = describe_pg_table(pg_table_name, sf_conn.conn)

    # at this point, we know what the output format should
    # look like based on pg_table_attrs, and we know
    # what we have already based on inserted_data
    # now, we need LLM to figure out a pipeline that makes
    # sense, validate it, save it, and apply it

    final_pipeline, err = generate_pipeline(
        sf_conn,
        pg_table_name,
        pg_table_attrs,
        inserted_data,
        existing_pipeline,
        existing_feedback_or_error,
    )

    return final_pipeline, err


def generate_pipeline(
    sf_conn: SnowflakeWrapper,
    pg_table_name: str,
    pg_table_attrs: dict,
    inserted_data: pd.DataFrame,
    existing_pipeline: Optional[str] = None,
    existing_feedback_or_error: Optional[str] = None,
) -> Tuple[str, Optional[str]]:
    remaining_runs = 5
    pg_table_attrs_as_list = []
    for v in pg_table_attrs.values():
        pg_table_attrs_as_list.append(v)

    current, error = existing_pipeline, existing_feedback_or_error

    while (not current or error) and remaining_runs > 0:
        current = b.GeneratePipeline(
            pg_table_name,
            pg_table_attrs_as_list,
            inserted_data.values.tolist(),
            existing_pipeline,
            existing_feedback_or_error,
        )
        try:
            apply_pipeline(current, inserted_data)
        except Exception as e:
            error = str(e)

        sf_conn.save_pipeline(uuid.uuid4(), current, error)
        remaining_runs -= 1

    if not current:
        raise ValueError(f"Failed to generate pipeline after {remaining_runs} attempts")

    return current, error

def apply_pipeline(pipeline_code: str, inserted_data: pd.DataFrame) -> pd.DataFrame:
    return inserted_data

def normalize_data(sf_conn: SnowflakeWrapper, import_run_id: uuid.UUID) -> pd.DataFrame:
    import_run = sf_conn.get_import_run(import_run_id)
    if not import_run:
        raise ValueError(f"Import run with id {import_run_id} not found")

    inserted_data = sf_conn.get_inserted_data(import_run["TABLE_NAME"], import_run_id)
    if not inserted_data:
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
    
    transformed = apply_pipeline(pipeline, inserted_data)

    return transformed
