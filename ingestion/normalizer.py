import datetime
import json
from pprint import pprint
from typing import Optional, Tuple
import uuid
import pandas as pd
import psycopg2  # type: ignore
import numpy as np
import re

from baml_client.types import AccountType, DataType
from baml_client import b
from sf_import import ImportTableRegistry, NormalizationPipeline, SnowflakeWrapper


def get_pg_table_name(account_type: AccountType, data_type: DataType) -> Optional[str]:
    return {
        ("bank", "transaction"): "bank_account_transaction",
        ("brokerage", "transaction"): "brokerage_account_transaction",
    }.get((account_type.value.lower(), data_type.value.lower()), None)


def describe_pg_table(
    pg_table: str, pg_conn: psycopg2.extensions.connection
) -> dict[str, dict]:
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
        r = {"data_type": col_data["data_type"]}
        if col_data["enum_values"]:
            r["enum_values"] = col_data["enum_values"].split(", ")
        out[col_data["column_name"]] = r
    return out


def get_or_generate_normalization_pipeline(
    sf_conn: SnowflakeWrapper,
    import_table_registry: ImportTableRegistry,
    inserted_data: pd.DataFrame,
) -> NormalizationPipeline:
    existing_version_id = import_table_registry.get(
        "versioned_normalization_pipeline_id"
    )
    existing_pipeline = None
    if existing_version_id:
        existing_pipeline = sf_conn.get_normalization_pipeline(existing_version_id)

        if existing_pipeline and not existing_pipeline["feedback_or_error"]:
            print("existing pipeline")
            return existing_pipeline

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
        host="localhost",
        database="postgres",
        user="postgres",
        password="postgres",
        port="5441",
    )
    pg_table_attrs = describe_pg_table(pg_table_name, pg_conn=pg_conn)

    # will return the existing pipeline if it exists
    # and is valid
    final_pipeline = generate_normalization_pipeline(
        import_table_registry["import_table_registry_id"],
        sf_conn,
        pg_table_name,
        pg_table_attrs,
        inserted_data,
        existing_pipeline,
    )

    return final_pipeline


def validate_transformed_column(
    new_df: pd.DataFrame, pg_table_attrs: dict, column: str
):
    if column not in pg_table_attrs:
        raise ValueError(f"Column {column} not found in pg_table_attrs")

    if pd.api.types.is_datetime64_any_dtype(new_df[column]):
        if new_df[column].isna().any():
            raise ValueError(
                f"Column {column} contains NaT values - should be valid datetimes"
            )


def generate_column_transformation_code(
    pg_table_name: str,
    column: str,
    pg_table_attrs: dict,
    inserted_data: pd.DataFrame,
) -> str:
    inserted_data_json = inserted_data.to_json()
    attrs = pg_table_attrs[column]

    # todo - we could attempt debugging here
    # if we made these iterative
    transformation_plan = b.PlanTransformation(
        pg_table_name,
        pg_table_column=column,
        pg_table_column_attributes_json=json.dumps(attrs),
        sample_data_json=inserted_data_json,
        df_headers=inserted_data.columns.to_list(),
    )

    relevant_columns = transformation_plan.df_headers
    planned_steps = transformation_plan.planned_steps

    relevant_data = inserted_data[relevant_columns]
    transformation_steps = b.GenerateTransformation(
        pg_table_name,
        pg_table_column=column,
        pg_table_column_attributes_json=json.dumps(attrs),
        sample_data_json=relevant_data.to_json(),
        planned_steps=planned_steps,
        df_headers=relevant_columns,
    )
    column_transformation_code = [x.transformation_code for x in transformation_steps]

    new_df = pd.DataFrame()
    for i, transformation in enumerate(column_transformation_code):
        works = False
        tries = 3
        while not works and tries > 0:
            tries -= 1
            try:
                local_scope = {
                    "inserted_data": inserted_data,
                    "pd": pd,
                    "datetime": datetime,
                    "np": np,
                    "re": re,
                    "new_df": new_df,
                }
                exec(transformation, {}, local_scope)
                new_df = local_scope.get("new_df")  # type:ignore
                works = True
                validate_transformed_column(new_df, pg_table_attrs, column)
            except Exception as e:
                if tries == 0:
                    raise ValueError(
                        f"Failed to execute transformation for column {column}: {transformation}. Error: {repr(e)}"
                    )
                transformation = b.SelfCorrectColumnTransformation(
                    instruction=transformation,
                    previous_code=column_transformation_code[:i],
                    code=transformation,
                    error=repr(e),
                )

    # at this point, new_df should have a single column, and
    # it should be ready to be added to output_df
    # however, the goal is just to validate the code, not actually apply it

    # consider more validation here
    column_transformation_code.append(f"output_df['{column}'] = new_df['{column}']")
    return ("; ".join(column_transformation_code)) + "; "


def generate_df_transformation_code(
    pg_table_name: str,
    pg_table_attrs: dict,
    inserted_data: pd.DataFrame,
) -> Tuple[str, Optional[str]]:
    """
    attempt to generate transformations on all of the columns,
    then stitch the data back together
    """
    code = ""

    for column in pg_table_attrs.keys():
        code += generate_column_transformation_code(
            pg_table_name,
            column,
            pg_table_attrs,
            inserted_data,
        )

    return code, None


def generate_normalization_pipeline(
    import_table_registry_id: uuid.UUID,
    sf_conn: SnowflakeWrapper,
    pg_table_name: str,
    pg_table_attrs: dict,
    inserted_data: pd.DataFrame,
    existing_pipeline: Optional[NormalizationPipeline] = None,
) -> NormalizationPipeline:
    """
    creates, tests, and saves a normalization pipeline
    if an existing pipeline is provided, it will be used as a starting point
    if the existing pipeline is valid, it will just use it, so specify None
    if we want to start from scratch
    """

    # todo - remove the primary key from inserted_data

    # remove primary key from pg_table_attrs
    del pg_table_attrs[pg_table_name.lower() + "_id"]
    del pg_table_attrs["import_run_id"]
    if "created_at" in pg_table_attrs:
        del pg_table_attrs["created_at"]
    if "updated_at" in pg_table_attrs:
        del pg_table_attrs["updated_at"]

    code = None
    error = None
    existing_version_id = None

    # if the existing pipeline exists and is valid, return it
    # otherwise, save elements from the existing pipeline
    if existing_pipeline:
        code = existing_pipeline["python_code"]
        error = existing_pipeline["feedback_or_error"]
        existing_version_id = existing_pipeline["previous_version_id"]
        if not error:
            return existing_pipeline

    remaining_runs = 1
    while (not code or error) and remaining_runs > 0:
        error = None
        # right now we're ignoring the error
        # because it doesn't return one
        code, _ = generate_df_transformation_code(
            pg_table_name,
            pg_table_attrs,
            inserted_data,
        )

        try:
            transformed_data = apply_pipeline_transformation(code, inserted_data)
            validate_transformed_data(transformed_data, pg_table_attrs)
        except Exception as e:
            error = repr(e)

        new_version_id = sf_conn.save_pipeline(
            import_table_registry_id,
            code,
            error,
            previous_version_id=existing_version_id,
        )
        existing_version_id = new_version_id

        remaining_runs -= 1

    if not existing_version_id:
        raise ValueError("Failed to save pipeline")
    if not code:
        raise ValueError(f"Failed to generate pipeline after {remaining_runs} attempts")

    return {
        "normalization_pipeline_id": existing_version_id,
        "python_code": code,
        "feedback_or_error": error,
        "previous_version_id": existing_version_id,
        "created_at": datetime.datetime.now(),
    }


def apply_pipeline_transformation(
    pipeline_code: str, inserted_data: pd.DataFrame
) -> pd.DataFrame:
    # Create a local scope for exec to define transformed_data and include inserted_data
    new_df = pd.DataFrame()
    output_df = pd.DataFrame()
    local_scope = {
        "inserted_data": inserted_data,
        "pd": pd,
        "datetime": datetime,
        "np": np,
        "re": re,
        "new_df": new_df,
        "output_df": output_df,
    }
    exec(pipeline_code, {}, local_scope)
    output_df = local_scope.get("output_df")  # type:ignore

    if output_df is None or not isinstance(output_df, pd.DataFrame):
        raise ValueError("Pipeline did not produce transformed_data")

    print(output_df)

    return output_df


def validate_transformed_data(transformed_data: pd.DataFrame, pg_table_attrs: dict):
    # ensure that all columns are present in the output
    for col in pg_table_attrs:
        if col not in transformed_data.columns:
            raise ValueError(f"Column {col} not found in transformed_data")

    # check for extra columns
    extra_cols = set(transformed_data.columns) - set(pg_table_attrs.keys())
    if extra_cols:
        raise ValueError(f"Extra columns found in transformed_data: {extra_cols}")


def normalize_data(sf_conn: SnowflakeWrapper, import_run_id: uuid.UUID) -> pd.DataFrame:
    import_run = sf_conn.get_import_run(import_run_id)
    if not import_run:
        raise ValueError(f"Import run with id {import_run_id} not found")

    inserted_data = sf_conn.get_inserted_data(import_run["TABLE_NAME"], import_run_id)
    if inserted_data is None:
        raise ValueError(f"No inserted data found for table {import_run['TABLE_NAME']}")

    # Remove the primary key from inserted_data
    primary_key = import_run["TABLE_NAME"].upper() + "_ID"
    if primary_key in inserted_data.columns:
        inserted_data = inserted_data.drop(columns=[primary_key])
    else:
        raise ValueError(f"Primary key {primary_key} not found in inserted data")

    # adds noise; we don't need
    inserted_data = inserted_data.drop(columns=["IMPORT_RUN_ID"])

    attr = sf_conn.get_import_table_attributes(import_run["TABLE_NAME"])
    if not attr:
        raise ValueError(f"No attributes found for table {import_run['TABLE_NAME']}")

    pipeline = get_or_generate_normalization_pipeline(
        sf_conn,
        attr,
        inserted_data,
    )
    if pipeline["feedback_or_error"]:
        raise ValueError(f"Error in pipeline: {pipeline['feedback_or_error']}")

    # i feel like we should validate the pipeline here
    transformed = apply_pipeline_transformation(pipeline["python_code"], inserted_data)

    sf_conn.add_brokerage_account_transactions(
        transformed,
        import_run_id=import_run_id,
        versioned_normalization_pipeline_id=pipeline["normalization_pipeline_id"],
    )

    return transformed
