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

# Perform query.
# Uses st.cache_data to only rerun when the query changes or after 10 min.
@st.cache_data(ttl=600)
def run_query(query):
    with conn.cursor() as cur:
        cur.execute(query)
        dat = cur.fetchall()
        df = pd.DataFrame(dat, columns=[col[0] for col in cur.description])
        return df

df = run_query("SELECT * from FOOD_INSPECTIONS_TEMP")

if 'init' not in st.session_state: st.session_state['init']=False
if 'store' not in st.session_state: st.session_state['store']={}
if 'store_d' not in st.session_state: st.session_state['store_d']={}
if 'edit' not in st.session_state: st.session_state['edit']=True

if st.session_state.init == False:
    st.session_state.store_d = df.to_dict()
    st.session_state.init = True

@st.cache_data(ttl=600)
def fetch_data():
    return pd.DataFrame(st.session_state.store_d)

def saveDefault():
    st.session_state.store_d = st.session_state.store

    # trunc and load table
    cur = conn.cursor()
    sql = "truncate table FOOD_INSPECTIONS_TEMP"
    cur.execute(sql)
    # TODO: merge into would be more efficient
    success, nchunks, nrows, _ = write_pandas(
        conn = conn, 
        df = pd.DataFrame(st.session_state.store_d), 
        table_name = 'FOOD_INSPECTIONS_TEMP'
        )
    return

def next_question():
    not_reviewed = df.loc[df.allow_access == '']
    for idx, row in not_reviewed.iterrows():
        return idx, row.DBA_Name

def insert_into_df(idx, options):
    print()
    print(idx)
    print(options)
    print()
    return

def app():

    st.header('Data Security Policy Editor')

    c1,c2 = st.columns(2)
    lock = c1.button(
        label = 'Save', 
        key = 'lock', 
        help = 'Overwrites values in Snowflake', 
        on_click = saveDefault,
        type = "primary"
        )
    # unlock=c2.button('Edit', key='unlock', on_click=saveDefault)
    unlock = c2.button('Edit', key='unlock', help='Allow modification of Snowflake table')
    if lock: st.session_state.edit = False
    if unlock: st.session_state.edit = True

    df = fetch_data()
    ag = AgGrid(df, editable=st.session_state.edit, height=200)
    df2 = ag['data']
    st.session_state.store=df2.to_dict()

    next_button = st.button('Next', key='next', on_click=saveDefault)
    idx = None
    if next_button:
        idx, question = next_question()
        st.write(question)

    options = st.multiselect(
    'Which business units are allowed access?',
    ['unit 1', 'unit 2', 'unit 3', 'unit 4'],
    ['unit 1'])

    st.write('You selected:', options)
    enter = st.button('Enter', key='enter', on_click=insert_into_df, kwargs={'idx': idx, 'options': options})
        

if __name__ == '__main__':
    app()