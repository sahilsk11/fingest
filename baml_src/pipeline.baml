// baml function that generates python code
// to apply onto a DF to transform it into
// the format of a pg table

enum ColumnType {
    NUMERIC
    TEXT
    DATE
}

class PlanTransformationResult {
    planned_steps string[] @description(#"
        the plan, step by step, in english. each step should be it's own element in the array.
    "#)
    df_headers string[] @description(#"
        the headers of the dataframe that we'll use in the transformation.
    "#)
}

function PlanTransformation(
    pg_table_name: string,
    pg_table_column: string,
    pg_table_column_attributes_json: string,
    sample_data_json: string,
    df_headers: string[]
) -> PlanTransformationResult {
    client Main

    prompt #"
        I have some data in a pandas dataframe that i've transformed into JSON and included below. I need to transform it and load it into a particular column of a postgres table called {{ pg_table_name }}. Here's the attributes of the table in Postgres that we need to put the data into:

        column name:
        {{pg_table_column}}

        column attributes:
        {{pg_table_column_attributes_json}}

        Here are the headers from the dataframe from which we need to extract {{pg_table_column}}:
        {{df_headers}}

        Now here's the snippet of the dataframe from which we need to extract {{pg_table_column}}:

        {{sample_data_json}}


        We have an existing dataframe called `inserted_data`, and an empty dataframe called `new_df`. We need to update `new_df` with a single column named {{pg_table_column}}.
        
        Please provide a 1-2 sentence explanation of how we can populate the particular column mentioned above. 
        
        For simple examples, like when the postgres column is "price" and there is a "price" column in the dataframe, we can simply say something like: "use the price column from the dataframe". We'll use your output to generate some pandas code - please don't provide code in your output - but hint at how we might extract the data.

        For more complex examples - say something like "amount" where we only have price and quantity or volume in the dataframe, we may want to output something like: "multiply price and quantity"

        Also be sure to look out for formatting issues:
            - It's common in financial systems to denote negative values by wrapping them in parentheses. For example, (100) means -100; ($240) means -240.
            - Sometimes a column might have numeric values for some rows, but other rows might be empty or have strings. You should specify how to handle.
            - Dates and times can be especially tricky - we may see all kinds of weird formats. Provide exact specifictions on how to convert from the existing format into a datetime in the target dataframe.

        You should always include a step for handling null, missing, or empty values. If you're using a numeric column, you should check for nulls and replace them with NaN. For text columns, you can set them to empty strings. For date columns, you can leave as empty strings. Never skip a row. And always make handling of the nulls, missing, and empty values the LAST step.

        Remember, you're not writing code here. You're describing to another system how they can write code. Return the plan as a string and the headers from the input dataframe that we will use in the transformation.

        Keep the response short and concise. Don't include any preambles or explanations.

        {{ctx.output_format}}
    "#
}

class TransformationStep {
    instruction string @description(#"
        the instruction you were given. copy this verbatim.
    "#)
    transformation_code string @description(#"
        the Python code that will perform the instruction exactly as described. This will go into an exec() function so please do not use any newlines and instead seperate commands with semicolons.

        Should always start with new_df['<column_name>'] =
    "#)
}

function GenerateTransformation(
    pg_table_name: string,
    pg_table_column: string,
    pg_table_column_attributes_json: string,
    sample_data_json: string,
    planned_steps: string[],
    df_headers: string[]
) -> TransformationStep[] {
    client Main
    prompt #"
        I have some data in a pandas dataframe that i've transformed into JSON and included below. I need to transform it and load it into a particular column of a postgres table called {{ pg_table_name }}. Here's the attributes of the table in Postgres that we need to put the data into:

        column name:
        {{pg_table_column}}

        column attributes:
        {{pg_table_column_attributes_json}}

        now here are all the columns from the dataframe we need to extract from:
        {{df_headers}}

        Now here's the snippet of the dataframe from which we need to extract {{pg_table_column}}:

        {{sample_data_json}}

        Please generate Python code that will update an existing but empty dataframe called `new_df` with a single column named {{pg_table_column}}. It should be populated with the data from the provided dataframe above. The provided dataframe is called `inserted_data` and is in scope.

        If you're using any regex strings, you can use the re module (in scope). Also prefix any regex strings with r<string> to help with formatting (I've been seeing a lot of <string>:1: SyntaxWarning: invalid escape sequence '\$' from your output)

        You have the following imports already in scope:
        import pandas as pd
        import numpy as np
        import re
        import datetime

        
        Here's how you should update `new_df`. Follow these steps exactly:
        {{planned_steps}}

        For each step outlined above, generate python code that matches the instruction. The output should look like: 
        {{ctx.output_format}}

        Note that there is no scope shared between the steps except for new_df and inserted_data. You should not use any other variables.

        For example, if the plan steps are:
        ["if the value has a number wrapped in parentheses, remove the parentheses and negate the number", "convert empty values to NaN", "convert all values to float"]
        then the output should look like:

        ["new_df['AMOUNT'] = inserted_data['AMOUNT'].replace(r'\(', -1, regex=True).replace(r'\)', '', regex=True)", "new_df['AMOUNT'] = new_df['AMOUNT'].dropna()", "new_df['AMOUNT'] = new_df['AMOUNT'].astype(float)"]
    "#

}

        // Finally, you should account for the fact that the dataframe might have null values or missing values. If you're using a numeric column, you should check for nulls and replace them with NaN. If you're using a text column, you should check for empty strings and replace them with a default value. If you're using a date column, you should check for nulls and replace them with a default value.


function SelfCorrectColumnTransformation(
    instruction: string,
    previous_code: string[],
    code: string,
    error: string
) -> string {
    client Main

    prompt #"
        You generated the following code:
        {{code}}

        It generated an error:
        {{error}}

        Here's the instructions provided.
        {{instruction}}

        Here is the code from previous steps:
        {{previous_code}}

        Please correct the code so that it will work without errors. The result will be used in a python exec() function so please do not use any newlines and instead seperate commands with semicolons. You should always start with new_df['<column_name>'] =

        The only variables in scope are new_df and inserted_data. You should not use any other variables. You have access to the following imports:
        import pandas as pd
        import numpy as np
        import re
        import datetime

        {{ctx.output_format}}
    "#
}

function PlanPipeline(
    pg_table_name: string,
    pg_table_attrs_json: string,
    sample_data_json: string,
) -> string {
    client Main

    prompt #"
        I have some data in a dataframe called inserted_data. I need to transform it so I can insert it into a postgres table called {{ pg_table_name }}. Here's the attributes of the table in Postgres that we need to put the data into:
        {{ pg_table_attrs_json }}

        now here's a snippet of the dataframe in JSON format. we'll need to transform it into a dataframe that matches the postgres table EXACTLY:

        {{ sample_data_json }}

        For each of the columns in the postgres table, please write down the exact transformation or method you will need to apply to the data in the dataframe to get it into the necessary format. Describe this in plain english.

        It's common in financial systems to denote negative values by wrapping them in parentheses. For example, (100) means -100; ($240) means -240.

        Note that we never want to include $ or other currency or numerical symbols in the output. If the field represents a number, we want to parse it as a float. Ensure to check fields before attempting to parse them - sometimes numeric fields can be empty and trying to parse a number from them can cause an error.

        For example, if the postgres table has a column called 'amount' and the data in the dataframe has a column called 'AMOUNT_FACE_VALUE', you could output:

        1. amount: parse the 'AMOUNT_FACE_VALUE' column and convert it to a float. if the value is null or empty, set it to null.

        and maybe if there was a column called 'PRICE' in the dataframe and in the postgres table, you could output:

        2. price: parse the 'PRICE' column and convert it to a float. if the value is null or empty, set it to null.

        Plan out your steps sequentially before you begin. Once you're able to express your plan in english and you cross-checked that it makes sense and you've included all the columns from the postgres table, behin the output. Do not include any extraneous columns that are not specified in the postgres table.

        Don't include any headers or preambles or conclusions - just the enumerated steps. Note that this guide will be used to generate python code using pandas library - don't write code here but include hints how we could perform any transformations in python.

        {{ ctx.output_format }}
       "#
}

function GeneratePipeline(
    pg_table_name: string,
    pg_table_attrs_json: string,
    sample_data_json: string,
    plan: string,
    // existing_pipeline: string | null,
    // existing_feedback_or_error: string | null,
) -> string {
    client Main

    prompt #"
        I have some data in a dataframe called inserted_data. I need to transform it so I can insert it into a postgres table called {{ pg_table_name }}. Here's the attributes of the table in Postgres that we need to put the data into:
        {{ pg_table_attrs_json }}

        now here's a snippet of the dataframe in JSON format. we'll need to transform it into a dataframe that matches the postgres table EXACTLY:

        {{ sample_data_json }}


        Now, you need to write a python script that takes a dataframe as input
        and returns a dataframe as output. The dataframe should have the same
        columns as the Postgres table with no missing or extra columns.

        Here is how I want you to to perform the transformation:

        {{ plan }}

        Be careful with date formats - the inputs may have super wonky date formats. Ensure that we're able to parse them into a datetime object within the output dataframe. I've included the datetime library in scope (import datetime); feel free to import other libraries you may need if you think it's necessary.


        You can use the following functions to help you:

        - df.dropna(how='all') to drop rows with missing values
        - df.fillna(value) to fill missing values with a specific value. please check that the column exists before attempting to fill it.
        - df.isnull() to check for null values
        - df.dropna(how='all').dropna(how='all') to drop rows with null values
        - df.apply(lambda x: x.astype(float)) to convert all values to float
        - df.apply(lambda x: x.astype(int)) to convert all values to int
        - df.apply(lambda x: x.astype(str)) to convert all values to str

        You can use the following libraries to help you:

        - pandas

        assume that the variable inserted_data: pd.DataFrame is already defined and in scope. the outputted code should simply create a new variable called transformed_data and assign the output of the transformations to it.

        I'm going to take the exact output you generate and put it into an exec() function call, so use semicolons to separate each line of code. do not put newlines in the output to seperate the lines.

        Don't include any other code or explanations in the function. Do not attempt to format or apply markdown - it's going straight into the exec() function.

        Your output should begin with: transformed_data = ...


        {{ ctx.output_format }}
    "#

}

// here's what you generated last time (may be empty):

        // {{ existing_pipeline }}

        // here's the error message from that result (may be empty):

        // {{ existing_feedback_or_error }}

        // You may use that to adjust your code.

test TestPipeline {
  functions [GeneratePipeline]
  args {
    pg_table_name #"
      brokerage_account_transaction
    "#
    pg_table_attrs_json #"
        {"amount": {"data_type": "numeric"}, "brokerage_account_transaction_id": {"data_type": "uuid"}, "description": {"data_type": "text"}, "external_transaction_id": {"data_type": "text"}, "import_run_id": {"data_type": "uuid"}, "price": {"data_type": "numeric"}, "quantity": {"data_type": "numeric"}, "side": {"data_type": "USER-DEFINED", "enum_values": ["sell", "buy"]}, "symbol_or_cusip": {"data_type": "text"}, "transaction_date": {"data_type": "timestamp with time zone"}}
    "#
    sample_data_json #"
         {"SCHWAB_BROKERAGE_TRANSACTION_ID":{"0":"a74265a8-f887-40e8-93cb-1f374bfe1e0e","1":"c340b492-2366-47ea-937f-29fc2cc9ee9e","2":"8859d89c-27b9-4e70-9658-5ea8aade9236","3":"da913b31-0703-46b3-bdfa-2eefd7125cb3","4":"46816820-f210-460f-b9c4-ec82f0acd5f8","5":"caa210f3-eff5-462b-a816-59a75a3dcda3","6":"cfebab17-f53a-4e4f-90d4-9851af223152","7":"af657c11-51ed-4348-9135-3ba3e8d9cbe6","8":"c0ed5864-4a51-4ff2-97a8-d36a2f705119"},"IMPORT_RUN_ID":{"0":"6d123e75-154b-47db-be63-39e9afd62771","1":"6d123e75-154b-47db-be63-39e9afd62771","2":"6d123e75-154b-47db-be63-39e9afd62771","3":"6d123e75-154b-47db-be63-39e9afd62771","4":"6d123e75-154b-47db-be63-39e9afd62771","5":"6d123e75-154b-47db-be63-39e9afd62771","6":"6d123e75-154b-47db-be63-39e9afd62771","7":"6d123e75-154b-47db-be63-39e9afd62771","8":"6d123e75-154b-47db-be63-39e9afd62771"},"SYMBOL":{"0":"SNOW","1":"QQQ","2":"HOOD","3":"DASH","4":"PANW","5":"KLAC","6":"QQQ","7":"HOOD","8":"META"},"STRATEGY_NAME":{"0":"","1":"","2":"","3":"","4":"","5":"","6":"","7":"","8":""},"NAME_OF_SECURITY":{"0":"SNOWFLAKE INC A","1":"INVSC QQQ TRUST SRS 1 ETF","2":"ROBINHOOD MKTS INC A","3":"DOORDASH INC A","4":"PALO ALTO NETWORKS I","5":"KLA CORP","6":"INVSC QQQ TRUST SRS 1 ETF","7":"ROBINHOOD MKTS INC A","8":"META PLATFORMS INC A"},"STATUS":{"0":"Filled","1":"Filled","2":"Filled","3":"Filled","4":"Filled","5":"Filled","6":"Filled","7":"Filled","8":"Filled"},"ACTION":{"0":"Buy","1":"Buy","2":"Buy","3":"Buy","4":"Buy","5":"Buy","6":"Buy","7":"Buy","8":"Buy"},"QUANTITY_FACE_VALUE":{"0":"8 Shares","1":"2 Shares","2":"34 Shares","3":"10 Shares","4":"2 Shares","5":"1 Shares","6":"3 Shares","7":"50 Shares","8":"2 Shares"},"PRICE":{"0":"Limit $128.96","1":"Limit $512.50","2":"Limit $30.11","3":"Limit $172.00","4":"Limit $378.00","5":"Limit $672.31","6":"Limit $485.74","7":"Limit $23.07","8":"Limit $600.00"},"TIMING":{"0":"Day","1":"Day","2":"Day","3":"Day","4":"Day","5":"Day","6":"Day","7":"Day","8":"Day"},"FILL_PRICE":{"0":"$128.80","1":"$512.355","2":"$29.995","3":"$169.13","4":"$377.68","5":"$671.24","6":"$485.4383","7":"$23.0076","8":"$595.1229"},"FILL_PRICE_IS_AVERAGE":{"0":"No","1":"No","2":"No","3":"No","4":"No","5":"No","6":"No","7":"No","8":"No"},"TIME_AND_DATE_ET_":{"0":"2:50 PM 11\/20\/2024","1":"11:44 AM 11\/07\/2024","2":"11:43 AM 11\/07\/2024","3":"3:01 AM 11\/07\/2024","4":"12:14 PM 10\/21\/2024","5":"12:12 PM 10\/21\/2024","6":"12:15 PM 10\/07\/2024","7":"12:00 PM 10\/07\/2024","8":"11:58 AM 10\/07\/2024"},"LAST_ACTIVITY_DATE_ET_":{"0":"2:50 PM 11\/20\/2024","1":"11:44 AM 11\/07\/2024","2":"11:43 AM 11\/07\/2024","3":"9:30 AM 11\/07\/2024","4":"12:14 PM 10\/21\/2024","5":"12:12 PM 10\/21\/2024","6":"12:15 PM 10\/07\/2024","7":"12:00 PM 10\/07\/2024","8":"11:58 AM 10\/07\/2024"},"REINVEST_CAPITAL_GAINS":{"0":"","1":"","2":"","3":"","4":"","5":"","6":"","7":"","8":""},"ORDER_NUMBER":{"0":1002273498711,"1":1002127605714,"2":1002127605648,"3":1002120224829,"4":1001944456386,"5":1001944456310,"6":1001807100782,"7":1001806475750,"8":1001806475650}}
    "#
    existing_pipeline #"
      
    "#
    existing_feedback_or_error #"
      
    "#
  }
}
