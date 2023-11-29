# Import python packages
import streamlit as st
import pandas as pd
from snowflake.snowpark import Session
from snowflake.snowpark.types import *
from snowflake.snowpark.functions import *
import json


st.set_page_config(page_title="JCI Mapping Editor", page_icon="📋", layout="wide")

@st.cache_resource
def init_connection():
    try:
        # connect to Snowflake
        with open('creds.json') as f:
            connection_parameters = json.load(f)  
            session = Session.builder.configs(connection_parameters).create()
        
        return session
        
    except Exception as e:
        st.error("Connection Failed. Please try again! The pages will not work unless a succesfull connection is made" + '\n' + '\n' + "error: " + str(e))


@st.cache_data
def get_bu():
    try:
        
        bu_list_sql = f"""
            select distinct bu
            from mapping.jci_region_bu_mapping
        """
        bu_list = session.sql(bu_list_sql).collect()
        
        return bu_list
        
    except Exception as e:
        st.sidebar.error("Sorry, An error occcured in get_bu(): " + str(e))
        

@st.cache_data
def get_mapping_tables(bu):
    try:
        tables_list_sql = f"""
            select table_schema || '.' || table_name
            from information_schema.tables t
            where t.table_schema = '{bu}'
            """
        tables_list = session.sql(tables_list_sql).collect()
        return tables_list
    except Exception as e:
        st.sidebar.error("Sorry, An error occcured in get_mapping_tables(): " + str(e))



def get_table_to_edit(table_name):
    try:
        select_stmt = f"""
        SELECT * EXCLUDE( LOAD_TS, UPDATED_BY)
        FROM {table_name}
        """
        table_edit_df = session.sql(select_stmt).to_pandas()
        return table_edit_df
    except Exception as e:
        st.sidebar.error("Sorry, An error occcured in get_table_to_edit(): " + str(e))

@st.cache_data
def get_primary_keys (table_name):
    try:
        # for merging into a table we need to know which Column to merge on. We need to
        # check if a PK key exists and get its name    
        # Hard coding for now
        get_PK_sql = "show primary keys in " + table_name
        session.sql(get_PK_sql).collect()
        #because this is a show command we need to get the QueryID from the show command and execute again
        get_PK_sql = 'SELECT * FROM table(RESULT_SCAN(LAST_QUERY_ID(-1)))'
        pk_list_df = session.sql(get_PK_sql).to_pandas()
        return pk_list_df
    except Exception as e:
        st.sidebar.error("Sorry, An error occcured in get_primary_keys(): " + str(e))

try:
    session = init_connection()
    #open the connection

except Exception as e:
        st.error("Connection Failed.  Please try again! The pages will not work unless a sucessfull connection is made" + '\n' + '\n' + "error: " + str(e))

@st.cache_data
def get_col_list_sql(table_name):
    try:
        tbl = table_name.split('.')[1]
        sch = table_name.split('.')[0]
        col_list_sql = f"""
        SELECT LISTAGG(
                'VALUE:' || COLUMN_NAME || '::' || DATA_TYPE || ' AS ' || COLUMN_NAME,
                ','
            ) WITHIN GROUP (
                ORDER BY ORDINAL_POSITION
            ) || ', VALUE:DEL::VARCHAR AS DEL' COL_SELECT_FOR_JSON,
            LISTAGG(
                CASE
                    WHEN COLUMN_NAME = '{PK_COL}' THEN NULL
                    ELSE ' tgt.' || COLUMN_NAME || ' =  src.' || COLUMN_NAME
                END,
                ', '
            ) WITHIN GROUP (
                ORDER BY ORDINAL_POSITION
            ) COL_LIST_FOR_MERGE_UPDATE,
            '(' || LISTAGG(COLUMN_NAME, ',') WITHIN GROUP (
                ORDER BY ORDINAL_POSITION
            ) || ')                               
                            ' || 'VALUES (' || LISTAGG('src.' || COLUMN_NAME, ', ') WITHIN GROUP (
                ORDER BY ORDINAL_POSITION
            ) || ')' COL_LIST_FOR_MERGE_INSERT
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{tbl}'
        AND TABLE_SCHEMA = '{sch}';
        """

        #st.write(col_list_sql)


            #get one row row back with the 3 column lists 
            ##store each column list into its own  variable to be used later
        table_list_df = session.sql(col_list_sql).to_pandas()
        
        return table_list_df

        
    except Exception as e:
        st.sidebar.error("Sorry, An error occcured in  get_col_list_sql(): " + str(e))


##add some markdown to the page with a desc 
st.header("JCI Mapping Editor 📋")

bu_list_df = get_bu()




#formatting to contain the select box to only one column, so it doesnt span the entire width
col1, col2 = st.columns(2)
with col1:
    
    bu = st.selectbox('Business Unit', bu_list_df)
    
if bu is not None:
    try:
        
        table_list_df = get_mapping_tables(bu)
        
        table = st.selectbox('Table to edit', table_list_df)
        
        
        col1, col2 = st.columns(2) 

   

        st.write("Editing table: " + table)
        results = get_primary_keys(table)

        if len(results) != 1:
            with col1:
                st.error('Only tables with 1 PK column are supported: Your Table has less than 1 or more than 1 PK column')
        else:
            PK_COL = results.iloc[0]['column_name']

            df = get_table_to_edit(table)
            edited_df = st.data_editor(df
                                                    , key="data_editor"
                                                    , use_container_width=True
                                                    , column_config = {
                                                        "BRANCH_TYPE": st.column_config.SelectboxColumn(
                                                            "Type",
                                                            width="medium",
                                                            options=["DIRECT",
                                                                     "INDIRECT",
                                                                     "NONE"]
                                                        )
                                                    }
                                                    , num_rows="dynamic"
                                                    )
            ######## DEBUGGING ###########
            #  remove the next two lines to see output of changed DF ###### 
            # st.write("Here's the session state:")
            # st.write(st.session_state["data_editor"])
            ###### END DEBUGGING ######
    
            #save the session state into a variable
            json_raw = st.session_state["data_editor"]
            
            submit =st.button("Submit Changes")
            
            col_list_df = get_col_list_sql(table)
            COL_SELECT_FOR_JSON = col_list_df.iloc[0]['COL_SELECT_FOR_JSON']
            COL_LIST_FOR_MERGE_UPDATE = col_list_df.iloc[0]['COL_LIST_FOR_MERGE_UPDATE']
            COL_LIST_FOR_MERGE_INSERT = col_list_df.iloc[0]['COL_LIST_FOR_MERGE_INSERT']
    
    
            #### DEBUGGING ##################
            # st.write(COL_SELECT_FOR_JSON)
            # st.write(COL_LIST_FOR_MERGE_UPDATE)
            # st.write(COL_LIST_FOR_MERGE_INSERT)
            # END DEBUGGING #############
    
            #create an empty dataframe to merge edits, inserts and delete DFs info 
            merged_df = pd.DataFrame()
    
    
            #when submit butten is clicked, we can begin processing the JSON state and create 3 dataframes for edits, inderts and delets. This will get dumpted to JSON
            if submit: 
                                
                #loop through the session state JSON object
                for key in json_raw:
                    value = json_raw[key]
                    
                    #handle edit and check is the edit has values 
                    if key == "edited_rows" and  len(json_raw['edited_rows']) > 0:
                        
                        #create a Dataframe from the JSON object 
                        edit_df = pd.DataFrame.from_dict(json_raw['edited_rows'], orient='index')
                        edit_df.index.name='ROW'      
                        edit_df.rename_axis(None, inplace=True)
                        edit_df.reset_index(inplace=True)
                        edit_df= edit_df.rename(columns={"index": "ROW"})
                        edit_df['ROW']=edit_df['ROW'].astype(int)
                        
                        cols_to_merge = df.columns.difference(edit_df.columns)
                        
                        # DEBUGGING
                        # st.write(cols_to_merge)  
                        # st.write(edit_df)
                      
                        #merge/join with orginal dataframe to get the column values that were changes 
                        edit_df = pd.merge(edit_df, df[cols_to_merge], left_on="ROW", right_index=True)
                        
                        #remove the unneeded colunm
                        edit_df.drop(columns=['ROW'], inplace=True)
                        #add a column denoting this is not a delete operation 
                        edit_df['DEL'] = 'N'
                        
                        #append the edit DF to a single merged dataframe to use at end 
                        merged_df = merged_df.append(edit_df)
            
                        #### DEBUGGING ######
                        #st.write('edit dataframe:')
                        #st.dataframe(edit_df)
                        #######################
    
                    ############ INSERTS ###############
                    # handle added row logic and check if there are values in the added rows key
                    if key == "added_rows" and len(json_raw['added_rows']) > 0 :
                        add_df_all= pd.DataFrame
                        for key in json_raw['added_rows']:
                            #st.write(key)
                            add_df = pd.DataFrame.from_dict(key, orient='index', columns=['VAL'])
                            add_df= add_df.T
    
                            #st.write(add_df)
                            #rename columns so we get the column names from the orig DF based on the values  that chaged from those columns
                            
                            # for col in add_df.columns:
                            #     add_df.rename(columns={str(col): df.columns[int(col)-1]}, inplace=True)
            
                            add_df['DEL'] = 'N'
                            add_df_all = pd.concat([add_df], ignore_index=True )
                            
                        #### DEBUGGING ##############
                        # st.write('insert dataframe:')
                        # st.write(add_df_all)    
                        ##### END DEBUGGING          
                        
                        # append the insert DF to a single merged dataframe to use at end             
                        merged_df = merged_df.append(add_df_all)
    
                    ############## DELETES ###################   
                    # #handle delete logic and check if there are values in the deleted rows key
                    if key == "deleted_rows" and len(json_raw['deleted_rows']) > 0:
                        
                        del_df = pd.DataFrame.from_dict(json_raw['deleted_rows'])
                        del_df.columns = ['VAL']
                        #st.write(df_new)
                        delete_df = pd.merge(del_df, df, left_on='VAL', right_index=True)
                
                        delete_df.drop(columns=['VAL'], inplace=True)
                        delete_df['DEL'] = 'Y'
                        
                        #### DEBUGGING #############
                        # st.write('delete dataframe:')
                        # st.write(delete_df)
                        ##### END DEBUGGING ##################
            
                        # add the delete DF into the merged DF
                        merged_df = merged_df.append(delete_df)
                        
                #now we have all the DFs so we can progess them to JSON and Snowflake
                #merged_df = pd.concat([operation_list], ignore_index=True )
                
                ######## DEBUGGING   ###########
                # st.write('merged dataframe:')
                # st.write(merged_df)
                ####### END DEBUGGING #####$#
    
                #error handling to make sure some data was changed before trying to process
                if len(json_raw['deleted_rows']) + len(json_raw['edited_rows']) +  len(json_raw['added_rows']) == 0:
                    st.error('No changed, deleted or added data was detected. Please make edits before submitting.')
                else: #process the modified data

                    #st.dataframe(merged_df)
                    #print DF to Json
                    result = merged_df.to_json(orient="records", date_format='iso')
                    parsed = json.loads(result)
                    json_data=json.dumps(parsed, indent=4)  
                    
                    ##### MERGE VIEW ###
                    ##create a view to wrap around the JSON data with the same column names as our source table.
                    # note: this is a temporary view and is destroyed after the session. if you'd like to view thw 
                    #       View DDL you can remove the temporary keyword 
                    # SRC_VIEW_SQL = "CREATE OR REPLACE VIEW STREAMLIT_MERGE_VW AS (            \
                    #     SELECT " +   COL_SELECT_FOR_JSON + " FROM                             \
                    #     ( SELECT PARSE_JSON(' " + json_data + "') as JSON_DATA),              \
                    #     LATERAL FLATTEN (input => JSON_DATA));" 
                    SRC_VIEW_SQL = f"""
                    CREATE OR REPLACE VIEW STREAMLIT_MERGE_VW AS (
                        SELECT {COL_SELECT_FOR_JSON}
                        FROM (
                                SELECT PARSE_JSON('{json_data}') as JSON_DATA
                            ),
                            LATERAL FLATTEN (input => JSON_DATA)
                    );
                    """
                    
                    
                    session.sql(SRC_VIEW_SQL).collect()
    
                    MERGE_SQL = f"""
                    MERGE INTO {table} tgt 
                    USING (
                      SELECT 
                        CASE 
                            WHEN ID IS NULL 
                        THEN 
                            mapping.dd_seq.nextval
                        ELSE 
                            ID 
                        END ID
                        , BRANCH_TYPE
                        , CASE_OWNER_BRANCH
                        , COMMENTS
                        , DEL
                        , CASE 
                        WHEN LOAD_TS IS NULL
                        THEN 
                            CURRENT_TIMESTAMP() 
                        ELSE LOAD_TS 
                        END LOAD_TS
                        , CASE 
                        WHEN UPDATED_BY IS NULL
                        THEN
                            CURRENT_USER()
                        ELSE UPDATED_BY
                        END UPDATED_BY
                          FROM STREAMLIT_MERGE_VW
                    ) src 
                    ON tgt.{PK_COL}::INT = src.{PK_COL}::INT
                    WHEN MATCHED
                    AND src.DEL = 'Y' THEN DELETE
                    WHEN MATCHED THEN
                    UPDATE
                    SET  {COL_LIST_FOR_MERGE_UPDATE}
                        WHEN NOT MATCHED THEN
                    INSERT {COL_LIST_FOR_MERGE_INSERT};
                    """
                    
                    
                    session.sql(MERGE_SQL).collect()
                    #drop the view
                    #session.sql("DROP VIEW STREAMLIT_MERGE_VW").collect() 
                    
                    st.success ('Edited data successfully written back to Snowflake!') 

            
    except Exception as e:
        st.write(e)

    