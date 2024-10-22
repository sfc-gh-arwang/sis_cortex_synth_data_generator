# Import python packages
import streamlit as st
import pandas as pd
import numpy as np
import time
from io import StringIO
from snowflake.cortex import Complete
from snowflake.snowpark.context import get_active_session

def save_func(edited_df):
    try:
        converted_df = session.create_dataframe(pd.DataFrame(edited_df))
        converted_df.write.mode("overwrite").save_as_table(state.example_table)
        st.write(session.table(state.example_table).show())
        st.success("Table updated")
        #time.sleep(5)
    except:
        st.warning("Error updating table")

#initialize session state
state = st.session_state
if "num_rows" not in state:
    state.num_rows = None;
if "example_table" not in state:
    state.example_table = None;
if "attr_values" not in state:
    state.attr_values = None;
if "gen_pd" not in state:
    state.gen_pd = None;

st.title("Cortex Analyst Synthetic Data Generator")
st.write(
    """ Create a Synthetic Data Set using Customer Provided DDL.
    """
)

# Get the current credentials
session = get_active_session()

#For model selection details - see https://docs.snowflake.com/en/user-guide/snowflake-cortex/llm-functions#large-models
model = st.sidebar.selectbox("Select base model:",["mistral-large2", "llama3.1-405b", "reka-core", "llama3.1-70b", "snowflake-arctic", "reka-flash", "mixtral-8x7b", "jamba-instruct", "jamba-1.5-mini", "jamba-1.5.large", "llama3.2-1b", "llama3.2-3b", "llama3.1-8b"])
st.sidebar.markdown(f"Selected Model: `{model}`")

#Have user run SQL to execute provided DDL
with st.expander("Step 1 - Input Customer Provided DDL"):
    input_ddl = st.text_input(label='Paste in DDL')
    submit_button = st.button('Create Table From DDL')
    if submit_button:
        with st.spinner("Running SQL"):
            try:
                result = session.sql(input_ddl).collect()
                st.success(result)
            except Exception as error:
                st.warning("Error running SQL.")
                st.write(error)

#allow user to input sample values
with st.expander("Step 2 (Optional) - Add Sample Data"):
    #get table
    try: 
        table_created = session.sql("""select table_name from information_schema.tables where created > DATEADD(DAY, -1, CURRENT_TIMESTAMP) and table_type = 'BASE TABLE' order by created desc LIMIT 1""").to_pandas().iloc[0,0]
        state.example_table = table_created
        st.write ("Add some Sample Data to help inform the Synthetic Data Creation:")
        column_names = session.table(state.example_table).columns
        df = pd.DataFrame(None,columns = column_names)
        edited_df = st.data_editor(df, num_rows="dynamic", hide_index = True)
        save_it = st.button("Save Changes")
        if save_it:
            save_func(edited_df)
    except:
        st.warning("Please run previous steps first.")

#have user define sample attributes (optional)
with st.expander("Step 3 (Optional) - Define Other Important Attributes"):
    user_input = st.text_area("Write attributes in bullets.")
    save_input = st.button("Save Attributes")
    if save_input:
        if len(user_input) == 0:
            state.attr_values = "No additional rules or properties required."
        else:
            state.attr_values = user_input
        st.success("Attributes Saved.")

#Generate synthetic data with LLM
with st.expander("Step 4 - Generate Synthetic Data (<100 rows)"):
    try:
        tbl_as_str = session.table(state.example_table).to_pandas().to_string()
        state.num_rows = st.text_input(label='How many rows of data do you need generated?')
        generate = st.button("Generate CSV")
        if generate:
            prompt =f"""
            Generate me a set of comma separated values, given the following parameters:
            \n
            Use this example table (a pandas dataframe interpreted as a string) as a starting point:
            {tbl_as_str}
            \n
            Generate this many NEW rows of data:
            {state.num_rows} 
            \n
            Infer a reasonable value for each cell from column names, column data types, and row and column relationships. 
            Use example table values as reference only IF they have been provided.
            Here are examples of what is meant by row and column level relationships, which need to be respected.
            For example:
            - One or more columns may have a relationship, ex Movie Title and Movie Rating. In this case, the value for the Movie Rating should be related to the Movie Title.
            - One or more rows may have a relationship, ex. multiple rows may report different properties of the same Movie.
            \n
            Finally, the following ADDITIONAL rules and properties MUST be obeyed:
            {state.attr_values}
            \n
            Assume that there is no confusion and the prompt is EXACTLY what the user requires.
            ALWAYS include the header from the example table.
            DO NOT generate synthetic data values which have commas.
            NEVER return any descriptive text such as "Here is the generated set of comma-separated values:".
            """
            with st.spinner("Generating"):
                try:
                    time.sleep(1)
                    returned_csv = Complete(model, prompt, session=get_active_session())
                except:
                    st.warning("The selected Model may not exist in your region. Please select another one.")
            st.write("Raw LLM Output:")
            st.code(returned_csv)
            st.write("Generated Table:")
            try:
               mystr = StringIO(returned_csv)
               state.gen_pd = pd.read_csv(mystr)
               st.dataframe(state.gen_pd)
            except:
               st.write("An error occurred with displaying generated data.")
    except:
        st.warning("Please run previous steps first.")

#Save Table
with st.expander("Step 6 - Save Synthetic Data"):
    save_opt = st.button("Save Table")
    if save_opt:
        save_func(state.gen_pd)

#Experimental only - generate code for synthetic data when requested rows >100
with st.expander("[EXPERIMENTAL ONLY] - Generate Synthetic Data (>100 rows)"):
    state.num_rows = st.text_input(label='How many rows of data would you like to generate?')
    submit_prompt= st.button("Submit and Generate")
    if submit_prompt:
        try:
            tbl_as_str = session.table(state.example_table).to_pandas().to_string()
            prompt =f"""
            Given the following Example Table (a pandas dataframe interpreted as a string):
            {tbl_as_str}
            
            AND making sure to obey the following rules: 
            {state.attr_values}

            Generate me a SQL script that will INSERT {state.num_rows} rows of appropriately generated synthetic data.
            Use SQL only and do not use Python.
            """
            with st.spinner("Generating"):
                try:
                    time.sleep(1)
                    llm_code = Complete(model, prompt, session=get_active_session())
                    st.write("Run the below in a SQL Worksheet:")
                    st.code(llm_code)
                except:
                    st.warning("The selected Model may not exist in your region. Please select another one.")
        except:
            st.warning("Something went wrong.")