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
            - It's common in financial systems to denote negative values by wrapping them in parentheses. For example, (100) means -100; ($240) means -240. If you're including this in the instructions, do it in two steps. In the first step, remove the paranthesis, symbols, and invert the number. Then in the next step, try to convert all the values to float. Doing both of those in one step is very error-prone.
            - Sometimes a column might have numeric values for some rows, but other rows might be empty or have strings. You should specify how to handle.
            - Dates and times can be especially tricky - we may see all kinds of weird formats. Provide exact specifictions on how to convert from the existing format into a datetime in the target dataframe.

        You should always include a step for handling null, missing, or empty values. If you're using a numeric column, you should check for nulls or empty strings and replace them with NaN. For text columns, you can set them to empty strings. For date columns, you can leave as empty strings. Never skip a row. And always make handling of the nulls, missing, and empty values is the LAST step.

        Remember, you're not writing code here. You're describing to another system how they can write code. Return the plan as a string and the headers from the input dataframe that we will use in the transformation.

        Keep the response short and concise. Don't include any preambles or explanations.

        For example, if you're dealing with some numeric column, you might say:
        ["remove $ and , occurences from the rows", "if the data has a number wrapped in parentheses, remove the parentheses and negate the number", "convert empty values to NaN", "convert all values to float"]

        If you have multiple columns that match and you're unsure which to pick, go with the ones that reflect details of the *actual* event. For example, if you have columns "price" and "executed price", it would seem "executed price" is more indicative of what actually happaned. similar for date columns like "order placed date" and "order executed date", use the latter is it is more indicative of the actual event. use this similar logic anytime multiple columns may be in question and one reflects the actual event more then the other.

        

        {{ctx.output_format}}
    "#
}

class TransformationStep {
    instruction string @description(#"
        the instruction you were given. copy this verbatim.
    "#)
    transformation_code string @description(#"
        the Python code that will perform the instruction exactly as described. This will go into an exec() function so please do not use any newlines and instead seperate commands with semicolons.

        You should only do the bare minimum to satisfy the associated instruction - do not attempt to jump ahead and add code for other instruction steps.

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

        NEVER EVER use .dropna() because we NEVER want to drop any rows. This would mess up being able to insert the data into postgres. Always ensure the number of rows we start with is the same as the number of rows we end with after every step.

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

        ["new_df['amount'] = inserted_data['AMOUNT'].str.replace(r'\((\d+\.?\d*)\)', lambda x: f'-{x.group(1)}', regex=True).replace('', np.nan).astype(float)
", "new_df['amount'] = new_df['amount'].replace('', np.nan)", "new_df['amount'] = new_df['amount'].astype(float)"]
    "#

}

        // Finally, you should account for the fact that the dataframe might have null values or missing values. If you're using a numeric column, you should check for nulls and replace them with NaN. If you're using a text column, you should check for empty strings and replace them with a default value. If you're using a date column, you should check for nulls and replace them with a default value.


class SelfCorrectColumnTransformationResult {
    explanation string @description(#"
        Explain why the code failed in 1-2 sentences. Include the type of cell (row value) that would cause the failure, and the part of the code that would cause the issue.

        Also include how your adjustment will actually fix the present issue
    "#)
    corrected_code string
}


function SelfCorrectColumnTransformation(
    instruction: string,
    previous_code: string[],
    code: string,
    error: string
) -> SelfCorrectColumnTransformationResult {
    client Main

    prompt #"
        You generated the following code. There is some issue with it:
        {{code}}

        It generated an error:
        {{error}}

        Here's the instructions provided.
        {{instruction}}

        Here is the code from previous steps that I wrote. These steps cannot be modified, because they're already executed:
        {{previous_code}}

        Please figure out why the code you generated did not work, then regenerate only the code you generated. Do not return or modify any of the code I generated. I'm going to attempt to re-run your code once you fix it here.

        The only variables in scope are new_df and inserted_data. You should not use any other variables. You have access to the following imports:
        import pandas as pd
        import numpy as np
        import re
        import datetime

        Please correct the code so that it will work without errors. The result will be used in a python exec() function so please do not use any newlines and instead seperate commands with semicolons. Do not include any preamble, explanations, or other text; only the corrected python code. Do not apply any formatting (i.e. do not start with python```). You do NOT need to include the code from previous steps - this is only provided so you have context. Just fix the code you generated that has an issue.
        
        You should always start with new_df['<column_name>'] =

        {{ctx.output_format}}
    "#
}
