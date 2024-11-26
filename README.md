# fingest

Multi-stage ETL pipelines for ingesting and normalizing structured/unstructued financial data.

1. raw data is stored in s3
2. pipeline A converts raw data to rows (skip this step if CSV or JSON) and stores in Snowflake
3. pipeline B converts rows from native format to normalized format (as defined by Postgres tables)


Pipeline B uses an LLM-assisted workflow that generates self-correcting Python code to transform the data to the final, normalized form.
