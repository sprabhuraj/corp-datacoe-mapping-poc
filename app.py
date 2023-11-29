import streamlit as st
import pandas as pd
import numpy as np
import json
from snowflake.snowpark import Session

# connect to Snowflake
with open('creds.json') as f:
    connection_parameters = json.load(f)  
session = Session.builder.configs(connection_parameters).create()
print(session)

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


st.header("JCI Uploader :snowflake:")


bu_list_df = get_bu()
col1, col2 = st.columns(2)
with col1:
    
    bu = st.selectbox('Business Unit', bu_list_df, None)
    
if bu is not None:
    file = st.file_uploader("Drop your CSV here to load to Snowflake", type={"csv"})
    
    

    if file is not None:
        try:
            
            
            table_name = file.name.split('.csv')[0]
            file_df = pd.read_csv(file)
            
            #### ADD AUDIT COLUMNS ###
            
            file_df.index = np.arange(1, len(file_df) + 1)
            file_df.index.name = 'ID'
            file_df.reset_index(inplace=True)
            file_df['LOAD_TS'] = pd.Timestamp.now()
            file_df['LOAD_TS'] = file_df['LOAD_TS'].dt.tz_localize('UTC')
            user = session.sql('SELECT CURRENT_USER()').collect()[0][0]
            file_df['UPDATED_BY'] = user
            st.dataframe(file_df)
            #st.write(file_df.columns)
            
            sql_ddl = pd.io.sql.get_schema(file_df, f'{table_name}').replace('CREATE ', 'CREATE OR REPLACE TRANSIENT ')
            sql_ddl2 = sql_ddl.replace('"ID" INTEGER', 'ID INTEGER DEFAULT MAPPING.DD_SEQ.NEXTVAL')\
                .replace(f'TABLE "{table_name}"', f'TABLE {bu}.{table_name}')\
                .replace(f')', f', CONSTRAINT PK PRIMARY KEY (ID))')
            

            session.sql(sql_ddl2).collect()
            
            
            snowparkDf=session.write_pandas(file_df,table_name,schema=bu) #, auto_create_table=False, overwrite=True, table_type='transient'
            st.subheader("Data has been uploaded to Snowflake")
        except Exception as e:
            st.warning("Failed to load data :boom:")
            st.write(e)
else:
    st.write("Please choose a Business")
    


