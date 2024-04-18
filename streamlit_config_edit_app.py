# Import python packages
import streamlit as st
from snowflake.snowpark.functions import col
import requests
import pandas as pd

cnx = st.connection("snowflake")
session = cnx.session()

# Function to fetch tables from Snowflake
def get_tables():
    cursor = cnx.cursor()
    cursor.execute("SHOW TABLES")
    tables = [table[1] for table in cursor.fetchall()]
    return tables

# Streamlit app
def main():
    st.title("Config Table Editor :lab_coat:")
    
    # Fetch tables from Snowflake
    tables = get_tables()
    
    # Let user select a table
    selected_table = st.selectbox("Select a table", tables)
    
    # Fetch data from selected table
    cursor = cnx.cursor()
    cursor.execute(f"SELECT * FROM {selected_table}")
    data = cursor.fetchall()
    #st.dataframe(data)
    df = pd.DataFrame(data, columns=[desc[0] for desc in cursor.description])
    
    # Display dataframe
    st.experimental_data_editor(df)

# Run the Streamlit app
if __name__ == "__main__":
    main()
