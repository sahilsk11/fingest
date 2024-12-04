import datetime
import json
import logging
from pprint import pprint
from typing import Any, Callable, Optional, Tuple
import uuid
import pandas as pd
import psycopg2  # type: ignore
import numpy as np
import re

from baml_client.types import AccountType, DataType
from baml_client import b
from domain.normalizer import (
    CodeStep,
    NormalizationPipeline,
    TransformerOutputColumn,
    TransformerOutputSchema,
)
from broker import MessageBroker
from sf_import import ImportTableRegistry, SnowflakeWrapper


class NormalizationService:
    def __init__(
        self,
        sf_conn: SnowflakeWrapper,
        broker: MessageBroker,
    ):
        self.sf_conn = sf_conn
        self.broker = broker

    def get_or_generate_normalization_pipeline(
        self,
        output_schema: TransformerOutputSchema,
        inserted_data: pd.DataFrame,
        pub: Callable[[str, dict[str, object]], None],
    ) -> NormalizationPipeline:
        """
        right now this just generates a new pipeline
        every time
        """
       
        # existing_pipeline = None
        # if existing_version_id:
        #     existing_pipeline = self.sf_conn.get_normalization_pipeline(
        #         existing_version_id
        #     )

        #     if existing_pipeline and not existing_pipeline.feedback_or_error:
        #         pub(
        #             "NORMALIZATION_PIPELINE_GENERATED",
        #             {
        #                 "status": "found existing transformation pipeline",
        #                 "description": existing_pipeline.python_code_by_column,
        #             },
        #         )
        #         return existing_pipeline

        # will return the existing pipeline if it exists
        # and is valid
        final_pipeline = self.generate_normalization_pipeline(
            output_schema,
            inserted_data,
            pub,
            existing_pipeline=None,
        )
        pub(
            "NORMALIZATION_PIPELINE_GENERATED",
            {
                "status": "generated new transformation pipeline",
                "description": final_pipeline.python_code_by_column,
            },
        )

        return final_pipeline

    def validate_transformed_column(
        self, new_df: pd.DataFrame, column: TransformerOutputColumn
    ):
        return None

    def generate_column_transformation_code(
        self,
        output_schema_description: str,
        column: TransformerOutputColumn,
        inserted_data: pd.DataFrame,
    ) -> list[CodeStep]:
        inserted_data_json = inserted_data.to_json()

        # todo - we should do some of our own preprocessing
        # to fix null or empty columns from inserted data.
        # we should also do the paranthesis fix here

        # todo - we could attempt debugging here
        # if we made these iterative
        transformation_plan = b.PlanTransformation(
            output_schema_description,
            column.column_name,
            column.data_type.value,
            column.description,
            column.is_nullable,
            sample_data_json=inserted_data_json,
            df_headers=inserted_data.columns.to_list(),
        )

        relevant_columns = transformation_plan.df_headers
        planned_steps = transformation_plan.planned_steps

        relevant_data = inserted_data[relevant_columns]
        transformation_steps = b.GenerateTransformation(
            output_schema_description,
            column.column_name,
            column.data_type.value,
            column.description,
            column.is_nullable,
            sample_data_json=relevant_data.to_json(),
            planned_steps=planned_steps,
            df_headers=relevant_columns,
        )
        # column_transformation_code = [x.transformation_code for x in transformation_steps]
        # column_transformation_instructions = [x.instruction for x in transformation_steps]

        out: list[CodeStep] = []

        new_df = pd.DataFrame()
        for i, transformation_step in enumerate(transformation_steps):
            works = False
            tries = 3
            transformation_code = transformation_step.transformation_code
            transformation_instruction = transformation_step.instruction
            while not works and tries > 0:
                tries -= 1
                try:
                    local_scope = {
                        "inserted_data": inserted_data,
                        "new_df": new_df,
                    }
                    global_scope = {
                        "pd": pd,
                        "datetime": datetime,
                        "np": np,
                        "re": re,
                    }
                    exec(transformation_code, global_scope, local_scope)
                    new_df = local_scope.get("new_df")  # type:ignore
                    works = True
                    self.validate_transformed_column(new_df, column)
                except Exception as e:
                    if tries == 0:
                        raise ValueError(
                            f"Failed to execute transformation for column {column}: {transformation_code}. Error: {repr(e)}"
                        )
                    transformation_code = b.SelfCorrectColumnTransformation(
                        instruction=transformation_instruction,
                        previous_code=[
                            x.transformation_code for x in transformation_steps[:i]
                        ],
                        code=transformation_code,
                        error=repr(e),
                    ).corrected_code

                out.append(CodeStep(transformation_code, transformation_instruction))

        # at this point, new_df should have a single column, and
        # it should be ready to be added to output_df
        # however, the goal is just to validate the code, not actually apply it

        # consider more validation here
        out.append(
            CodeStep(f"output_df['{column}'] = new_df['{column}']", f"set {column}")
        )
        return out

    def generate_df_transformation_code(
        self,
        output_schema: TransformerOutputSchema,
        inserted_data: pd.DataFrame,
        pub: Callable[[str, dict[str, object]], None],
    ) -> Tuple[dict[str, list[CodeStep]], Optional[str]]:
        """
        attempt to generate transformations on all of the columns,
        then stitch the data back together
        """
        code_by_column: dict[str, list[CodeStep]] = {}
        logging.info("generating code by column")
        for column in output_schema.columns:
            logging.info(f"generating code for column {column.column_name}")
            code_by_column[column.column_name] = (
                self.generate_column_transformation_code(
                    output_schema.description,
                    column,
                    inserted_data,
                )
            )

        return code_by_column, None

    def generate_normalization_pipeline(
        self,
        output_schema: TransformerOutputSchema,
        inserted_data: pd.DataFrame,
        pub: Callable[[str, dict[str, object]], None],
        existing_pipeline: Optional[NormalizationPipeline] = None,
    ) -> NormalizationPipeline:
        """
        creates, tests, and saves a normalization pipeline
        if an existing pipeline is provided, it will be used as a starting point
        if the existing pipeline is valid, it will just use it, so specify None
        if we want to start from scratch
        """

        code = None
        error = None
        existing_version_id = None

        # if the existing pipeline exists and is valid, return it
        # otherwise, save elements from the existing pipeline
        # if existing_pipeline:
        #     # TODO - fix this
        #     code = str(existing_pipeline.python_code_by_column)
        #     error = existing_pipeline.feedback_or_error
        #     existing_version_id = existing_pipeline.previous_version_id
        #     if not error:
        #         return existing_pipeline

        logging.info("whatever this is")
        print(code, error)
        remaining_runs = 1
        code_by_column = None
        while (not code or error) and remaining_runs > 0:
            logging.info("generating code")
            error = None
            # right now we're ignoring the error
            # because it doesn't return one
            code_by_column, _ = self.generate_df_transformation_code(
                output_schema,
                inserted_data,
                pub,
            )
            # code_by_column = existing_pipeline["python_code_by_column"]
            # this is lazy
            code_by_column_serializable = {
                column: [step.__dict__ for step in steps]
                for column, steps in code_by_column.items()
            }
            code = json.dumps(code_by_column_serializable)

            try:
                # we don't store this result - just apply to test it
                transformed_data = self.apply_pipeline_transformation(
                    code_by_column, inserted_data
                )
                self.validate_transformed_data(transformed_data, output_schema)
            except Exception as e:
                print(e)
                error = "failed to apply or validate pipeline: " + repr(e)

            # TODO - how should we store the pipeline?
            new_version_id = self.sf_conn.save_pipeline(
                output_schema.hash(),
                code,
                error,
                previous_version_id=existing_version_id,
            )
            existing_version_id = new_version_id

            remaining_runs -= 1

        if not existing_version_id:
            raise ValueError("Failed to save pipeline")
        if not code:
            raise ValueError(
                f"Failed to generate pipeline after {remaining_runs} attempts"
            )

        return NormalizationPipeline(
            normalization_pipeline_id=existing_version_id,
            python_code_by_column=code_by_column,
            feedback_or_error=error,
            previous_version_id=existing_version_id,
            created_at=datetime.datetime.now(),
            output_schema=output_schema,
        )

    def apply_pipeline_transformation(
        self, code_by_column: dict[str, list[CodeStep]], inserted_data: pd.DataFrame
    ) -> pd.DataFrame:
        new_df = pd.DataFrame()
        output_df = pd.DataFrame()

        for column, code_details in code_by_column.items():
            # todo - skip failed columns
            for d in code_details:
                pipeline_code = d.code
                local_scope = {
                    "inserted_data": inserted_data,
                    "new_df": new_df,
                    "output_df": output_df,
                }
                global_scope = {
                    "pd": pd,
                    "datetime": datetime,
                    "np": np,
                    "re": re,
                }
                try:
                    exec(pipeline_code, global_scope, local_scope)
                except Exception as e:
                    # i don't think we should self-correct here. we can consider how to store the error
                    # but another function should handle
                    raise Exception(
                        f"Error executing pipeline code on column {column} running {pipeline_code}: {repr(e)}"
                    )
                output_df = local_scope.get("output_df")  # type:ignore
                new_df = local_scope.get("new_df")  # type:ignore

        if output_df is None or not isinstance(output_df, pd.DataFrame):
            raise ValueError("Pipeline did not produce transformed_data")

        return output_df

    def validate_transformed_data(
        self, transformed_data: pd.DataFrame, output_schema: TransformerOutputSchema
    ):
        return None

    def normalize_data(
        self,
        import_run_id: uuid.UUID,
        output_schema: TransformerOutputSchema,
    ) -> pd.DataFrame:
        import_run = self.sf_conn.get_import_run(import_run_id)
        if not import_run:
            raise ValueError(f"Import run with id {import_run_id} not found")

        inserted_data = self.sf_conn.get_inserted_data(
            import_run["TABLE_NAME"], import_run_id
        )
        if inserted_data is None:
            raise ValueError(
                f"No inserted data found for table {import_run['TABLE_NAME']}"
            )

        # Remove the primary key from inserted_data
        primary_key = import_run["TABLE_NAME"].upper() + "_ID"
        if primary_key in inserted_data.columns:
            inserted_data = inserted_data.drop(columns=[primary_key])
        else:
            raise ValueError(f"Primary key {primary_key} not found in inserted data")

        # adds noise; we don't need
        inserted_data = inserted_data.drop(columns=["IMPORT_RUN_ID"])

        pub = lambda event, payload: self.broker.publish(
            event,
            payload,
            import_run_id,
        )

        pipeline = self.get_or_generate_normalization_pipeline(
            output_schema,
            inserted_data,
            pub,
        )
        if pipeline.feedback_or_error:
            raise ValueError(f"Error in pipeline: {pipeline.feedback_or_error}")

        if not pipeline.python_code_by_column:
            raise Exception("code by column is empty - please regenerate")

        # i feel like we should validate the pipeline here
        transformed = self.apply_pipeline_transformation(
            pipeline.python_code_by_column, inserted_data
        )

        # TODO - get the table name and insert the data
        # i think we should also store the target normalization
        # schema on the import run

        # sf_conn.add_brokerage_account_transactions(
        #     transformed,
        #     import_run_id=import_run_id,
        #     versioned_normalization_pipeline_id=pipeline["normalization_pipeline_id"],
        # )

        return transformed
