import streamlit as st
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
import numpy as np
from st_aggrid import AgGrid

st. set_page_config(layout="wide")

TABLE_NAME = "FOOD_INSPECTIONS_SMALL"

# Initialize connection.
# Uses st.cache_resource to only run once.
@st.cache_resource
def init_connection():
    return snowflake.connector.connect(
        **st.secrets["snowflake"], client_session_keep_alive=True
    )

conn = init_connection()

if 'store' not in st.session_state: st.session_state['store']={}
if 'store_d' not in st.session_state: st.session_state['store_d']={}

# Uses st.cache_data to only rerun when the query changes or after 10 min.
# @st.cache_data(ttl=600)
def run_query(query):
    with conn.cursor() as cur:
        cur.execute(query)
        dat = cur.fetchall()
        df = pd.DataFrame(dat, columns=[col[0] for col in cur.description])
        return df

df_table = run_query(f"SELECT * from {TABLE_NAME}")
st.session_state.store_d = df_table.to_dict()

# @st.cache_data(ttl=600)
def fetch_data():
    df = pd.DataFrame(st.session_state.store_d)
    df_sub = df.loc[df.DELETE != 1]
    st.session_state.store_del = df.loc[df.DELETE == 1]
    return df_sub

def saveDefault():
    st.session_state.store_d = st.session_state.store

    df = pd.concat([
        pd.DataFrame(st.session_state.store_d),
        st.session_state.store_del
    ], axis=0).sort_index()
    print(df)
    
    # trunc and load table
    cur = conn.cursor()
    sql = f"truncate table {TABLE_NAME}"
    cur.execute(sql)
    # TODO: merge into would be more efficient. But we need to know all the column names for that
    success, nchunks, nrows, _ = write_pandas(
        conn = conn, 
        # df = pd.DataFrame(st.session_state.store_d), 
        df = df, 
        table_name = TABLE_NAME
        )
    return

def add_row():
    df = pd.DataFrame(st.session_state.store)
    new_row = pd.DataFrame([[None for _ in df.columns]], columns=df.columns)
    df = pd.concat([df, new_row], axis=0, ignore_index=True)
    df.loc[df.DELETE.isnull(), 'DELETE'] = 0
    st.session_state.store = df.to_dict()
    saveDefault()
    return

def app():
    st.header('Table Editor')

    save = st.button('Save', key='save', on_click=saveDefault, type='primary')

    df_table = fetch_data()
    ag = AgGrid(df_table, editable=True, height=400, fit_columns_on_grid_load=True)
    df_table2 = ag['data']
    st.session_state.store = df_table2.to_dict()

    c1,c2 = st.columns(2)
    addrow = c1.button('Add row to bottom', key='addrow', on_click=add_row)
    deleterow = c2.button('Delete rows', key='deleterow', on_click=saveDefault)

if __name__ == '__main__':
    app()