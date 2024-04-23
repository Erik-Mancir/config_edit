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

@st.cache(allow_output_mutation=True)  # Cache edited data while allowing updates
def edit_row(index, original_df):
    """
    Creates a form for editing a specific row in the DataFrame.

    Args:
        index (int): The index of the row to edit.
        original_df (pd.DataFrame): The original DataFrame.

    Returns:
        pd.DataFrame: The updated DataFrame if changes were made, or None otherwise.
    """

    row_data = original_df.iloc[index].to_dict()  # Get row data for pre-filling

    form = st.form(key=f"edit_form_{index}")  # Unique key per form
    name = form.text_input(label="Name", value=row_data["ACCOUNT_NAME"])
    role = form.text_input(label="Role", value=row_data["Role"])
    submit = form.form_submit_button(label="Save")

    if submit:
        updated_df = original_df.copy()  # Avoid modifying original DataFrame
        updated_df.iloc[index] = {"Name": name, "Role": role}
        return updated_df
    else:
        return None

# Streamlit app
def main():

    tabs = st.tabs(["Editor", "Form", "View logs"])

    # Editing the table using the Streamlit Data Editor
    with tabs[0]:
        st.title("Config Table Editor")
        # Fetch tables from Snowflake
        tables = get_tables()
        
        # Let user select a table
        selected_table = st.selectbox("Select a table", tables, key=1)
        
        # Fetch data from selected table
        cursor = cnx.cursor()
        cursor.execute(f"SELECT * FROM {selected_table}")
        data = cursor.fetchall()
        #st.dataframe(data)
        df = pd.DataFrame(data, columns=[desc[0] for desc in cursor.description])
        
        # Display dataframe
        #st.data_editor(df, num_rows='dynamic')
        with st.form("data_editor_form"):
            st.caption("Edit the dataframe below")
            edited = st.data_editor(df, use_container_width=True, num_rows="dynamic") 
            submit_button = st.form_submit_button("Submit")

        if submit_button:
            try:
                #Note the quote_identifiers argument for case insensitivity
                st.dataframe(edited)
                session.write_pandas(edited, selected_table, overwrite=True, auto_create_table=True, quote_identifiers=False)
                st.success("Table updated")
                #time.sleep(5)
            except:
                st.warning("Error updating table")
    
    with tabs[1]:
        st.title("Config Table Form")

        tables_form = get_tables()
        # Let user select a table
        selected_table_form = st.selectbox("Select a table", tables_form, key=2)
        # Fetch data from selected table
        cursor = cnx.cursor()
        cursor.execute(f"SELECT * FROM {selected_table_form}")
        data = cursor.fetchall()
        #st.dataframe(data)
        df = pd.DataFrame(data, columns=[desc[0] for desc in cursor.description])

        st.dataframe(df, use_container_width=True)

        selected_index = None  # Store the index of the clicked row

        for index, row in df.iterrows():
            # Display row data
            st.write(row)

            # Create a button for each row
            if st.button(f"Edit Row {index+1}", key=f"edit_button_{index}"):
                selected_index = index

        # Check if a button was clicked and call the edit_row function
        if selected_index is not None:
            updated_df = edit_row(selected_index, df.copy())  # Pass a copy

            # Update the DataFrame if changes were made
            if updated_df is not None:
                df = updated_df
                st.write("Successfully updated data!")
            else:
                st.write("No changes made.")

        # Display the updated DataFrame
        st.write(df)
                            #st.experimental_rerun()
# Run the Streamlit app
if __name__ == "__main__":
    main()
