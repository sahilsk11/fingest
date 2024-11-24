// baml function that generates python code
// to apply onto a DF to transform it into
// the format of a pg table

function GeneratePipeline(
    pg_table_name: string,
    pg_table_attrs: string[][],
    sample_data: string[][] | float[][],
    existing_pipeline: string | null,
    existing_feedback_or_error: string | null,
) -> string {
    client Main

    prompt #"
        Here's the name of the table in Postgres that you're trying to normalize:
        {{ pg_table_name }}

        Here's the attributes of the table in Postgres that you're trying to normalize:
        {{ pg_table_attrs }}

        Now, you need to write a python function that takes a dataframe as input
        and returns a dataframe as output. The dataframe should have the same
        columns as the Postgres table, and the rows should be in the same order.

        The dataframe should be in the format of the Postgres table, and the
        function should be able to handle missing values and nulls.

        You can use the following functions to help you:

        - df.dropna(how='all') to drop rows with missing values
        - df.fillna(value) to fill missing values with a specific value
        - df.isnull() to check for null values
        - df.dropna(how='all').dropna(how='all') to drop rows with null values
        - df.apply(lambda x: x.astype(float)) to convert all values to float
        - df.apply(lambda x: x.astype(int)) to convert all values to int
        - df.apply(lambda x: x.astype(str)) to convert all values to str

        You can use the following libraries to help you:

        - numpy
        - pandas
        - psycopg2

        Here's an example of what the function should look like:

        def normalize_data(df: pd.DataFrame) -> pd.DataFrame:
            # Your code here
            return df

        Make sure to include the type hints for the function parameters and return value.

        Now, write the function that takes a dataframe as input and returns a dataframe as output.
        Make sure to include the type hints for the function parameters and return value.

        Don't include any other code or explanations in the function.
    "#

}