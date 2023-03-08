import streamlit as st
import snowflake.connector
import pandas as pd
import folium
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

df = run_query("SELECT * from FOOD_INSPECTIONS where RESULTS='Fail' limit 50;")

st.write(f"dataframe shape: {df.shape}")

df.LONGITUDE = df.LONGITUDE.astype(float)
df.LATITUDE = df.LATITUDE.astype(float)

# Print results.
st.dataframe(df)

# map of Chicago
m = folium.Map(location=[41.88148170412358, -87.63162352073498], zoom_start=15)

# add markers to map
folium.Marker(
    [41.87768968860344, -87.63705780095162], popup="2nd Watch Office", tooltip="2nd Watch Office"
).add_to(m)

for idx, row in df.iterrows():
    if pd.notnull(row.LATITUDE) & pd.notnull(row.LONGITUDE):
        folium.Marker(
            [row.LATITUDE, row.LONGITUDE], popup=row.DBA_Name, tooltip=row.AKA_Name
        ).add_to(m)

# call to render Folium map in Streamlit
st_data = st_folium(m, width=725)