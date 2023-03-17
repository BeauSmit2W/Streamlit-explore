import streamlit as st
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
import numpy as np
import folium
from st_aggrid import AgGrid
from streamlit_folium import st_folium

# Initialize connection.
# Uses st.cache_resource to only run once.
@st.cache_resource
def init_connection():
    return snowflake.connector.connect(
        **st.secrets["snowflake"], client_session_keep_alive=True
    )

conn = init_connection()

# default table when app loads
table_name = "MYTABLE"

# Uses st.cache_data to only rerun when the query changes or after 10 min.
# @st.cache_data(ttl=600)
def run_query(query):
    with conn.cursor() as cur:
        cur.execute(query)
        dat = cur.fetchall()
        df = pd.DataFrame(dat, columns=[col[0] for col in cur.description])
        return df

df_table_names = run_query(f"select table_name from STREAMLIT_POC.INFORMATION_SCHEMA.TABLES where table_schema = 'BSMIT'")
table_names = []
st.write(tbl for tbl in df_table_names['TABLE_NAME'])

option = st.selectbox(
        "How would you like to be contacted?",
        (table_names)
    )

df_table = run_query(f"SELECT * from {table_name}")

if 'init' not in st.session_state: st.session_state['init']=False
if 'store' not in st.session_state: st.session_state['store']={}
if 'store_d' not in st.session_state: st.session_state['store_d']={}
if 'edit' not in st.session_state: st.session_state['edit']=True

if st.session_state.init == False:
    st.session_state.store_d = df_table.to_dict()
    st.session_state.init = True

# @st.cache_data(ttl=600)
def fetch_data():
    return pd.DataFrame(st.session_state.store_d)

def saveDefault():
    st.session_state.store_d = st.session_state.store

    # trunc and load table
    cur = conn.cursor()
    sql = f"truncate table {table_name}"
    cur.execute(sql)
    # TODO: merge into would be more efficient
    success, nchunks, nrows, _ = write_pandas(
        conn = conn, 
        df_table = pd.DataFrame(st.session_state.store_d), 
        table_name = table_name
        )
    return

def app():

    st.header('Table Editor')

    c1,c2=st.columns(2)
    lock=c1.button('Lock', key='lock', on_click=saveDefault)
    unlock=c2.button('Unlock', key='unlock', on_click=saveDefault)
    if lock: st.session_state.edit = False
    if unlock: st.session_state.edit = True

    df_table = fetch_data()
    ag = AgGrid(df_table, editable=st.session_state.edit, height=200)
    df_table2=ag['data']
    st.session_state.store=df_table2.to_dict()
    st.dataframe(df_table2)

if __name__ == '__main__':
    app()