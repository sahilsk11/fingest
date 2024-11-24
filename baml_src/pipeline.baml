// baml function that generates python code
// to apply onto a DF to transform it into
// the format of a pg table

function GeneratePipeline(
    pg_table_name: string,
    pg_table_attrs_json: string,
    sample_data_json: string,
    existing_pipeline: string | null,
    existing_feedback_or_error: string | null,
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

        The dataframe should be in the format of the Postgres table, and the
        function should be able to handle missing values and nulls.

        Be careful with date formats - the inputs may have super wonky date formats. Ensure that we're able to parse them into a datetime object within the output dataframe. I've included the datetime library in scope (import datetime); feel free to import other libraries you may need if you think it's necessary.


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

        assume that the variable inserted_data: pd.DataFrame is already defined and in scope. the outputted code should simply create a new variable called transformed_data and assign the output of the transformations to it.

        I'm going to take the exact output you generate and put it into an exec() function call, so use semicolons to separate each line of code.

        Don't include any other code or explanations in the function. Do not attempt to format or apply markdown - it's going straight into the exec() function.

        here's what you generated last time (may be empty):

        {{ existing_pipeline }}

        here's the error message from that result (may be empty):

        {{ existing_feedback_or_error }}

        You may use that to adjust your code.

        {{ ctx.output_format }}
    "#

}

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
