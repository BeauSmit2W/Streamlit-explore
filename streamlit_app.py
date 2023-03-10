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

st.write(f"dataframe shape: {df.shape}")

if 'init' not in st.session_state: st.session_state['init']=False
if 'store' not in st.session_state: st.session_state['store']={}
if 'store_d' not in st.session_state: st.session_state['store_d']={}
if 'edit' not in st.session_state: st.session_state['edit']=True

if st.session_state.init == False:
    st.session_state.store_d = df.to_dict()
    st.session_state.init = True

@st.cache(allow_output_mutation=True)
def fetch_data():
    return pd.DataFrame(st.session_state.store_d)

def saveDefault():
    st.session_state.store_d = st.session_state.store

    # trunc and load table
    cur = conn.cursor()
    sql = "truncate table FOOD_INSPECTIONS_TEMP"
    cur.execute(sql)

    success, nchunks, nrows, _ = write_pandas(
        conn = conn, 
        df = pd.DataFrame(st.session_state.store_d), 
        table_name = 'FOOD_INSPECTIONS_TEMP'
        )
    return

def app():
    c1,c2=st.columns(2)
    lock=c1.button('Save', key='lock', on_click=saveDefault)
    unlock=c2.button('Edit', key='unlock', on_click=saveDefault)
    if lock: st.session_state.edit = False
    if unlock: st.session_state.edit = True

    df = fetch_data()
    ag = AgGrid(df, editable=st.session_state.edit, height=200)
    df2=ag['data']
    st.session_state.store=df2.to_dict()
    # st.dataframe(df2)

if __name__ == '__main__':
    app()



# df.LONGITUDE = df.LONGITUDE.astype(float)
# df.LATITUDE = df.LATITUDE.astype(float)

# # Print results.
# st.dataframe(df)

# # map of Chicago
# m = folium.Map(location=[41.88148170412358, -87.63162352073498], zoom_start=15)

# # add markers to map
# folium.Marker(
#     [41.87768968860344, -87.63705780095162], popup="2nd Watch Office", tooltip="2nd Watch Office"
# ).add_to(m)

# for idx, row in df.iterrows():
#     if pd.notnull(row.LATITUDE) & pd.notnull(row.LONGITUDE):
#         folium.Marker(
#             [row.LATITUDE, row.LONGITUDE], popup=row.DBA_Name, tooltip=row.AKA_Name
#         ).add_to(m)

# # call to render Folium map in Streamlit
# st_data = st_folium(m, width=725)