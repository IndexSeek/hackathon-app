import streamlit as st
from streamlit_pandas_profiling import st_profile_report
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import pandas_profiling
import pandas as pd

if "session" not in st.session_state:
    with st.sidebar:
        st.image("https://upload.wikimedia.org/wikipedia/commons/thumb/f/ff/Snowflake_Logo.svg/368px-Snowflake_Logo.svg.png?20210330073721")
        st.write("")
        with st.form("Snowflake Login"):
            account = st.text_input(label="Account")
            user = st.text_input(label="Username")
            password = st.text_input(label="Password", type="password")
            if st.form_submit_button(label="Login"):
                connection_parameters = {"account": account, "user": user, "password": password, "role": "SYSADMIN", "warehouse": "COMPUTE_WH", "database": "HACKATHON", "schema": "PUBLIC"}
                session = Session.builder.configs(connection_parameters).create()
                st.session_state.session = session
                st.experimental_rerun()
else:
    with st.sidebar:
        st.image("https://upload.wikimedia.org/wikipedia/commons/thumb/f/ff/Snowflake_Logo.svg/368px-Snowflake_Logo.svg.png?20210330073721")
        st.write("")
        #TODO: Who are we logged in as?
        if st.button("Logout"):
            st.session_state.session.close()
            del st.session_state.session
            st.experimental_rerun()

st.image("hackathon.png", width=300)

di_tab, de_tab, dm_tab, dv_tab = st.tabs(["Data Ingestion", "Data Profiling", "Data Modeling", "Data Visualization"])

with di_tab:
    st.markdown("## Data Ingestion")
    st.markdown("### Upload CSVs to Google Cloud Storage.")
    st.code("""
from google.cloud import storage
from pathlib import Path

storage_client = storage.Client()
bucket = storage_client.bucket('torqata-808s')
files = [file for file in Path(".").glob("*.csv")]

for file in files:
    blob = bucket.blob(f"csv_data/{file.name}")
    blob.upload_from_filename(file.name)
""")

    st.markdown("### Create Dataflow in Google Cloud for CSV to Parquet conversion using Managed Notebooks.")
    st.code("""
import pandas as pd
from google.cloud import storage
from io import BytesIO

client = storage.Client()
bucket = client.get_bucket('torqata-808s')

for file in bucket.list_blobs(prefix="csv_data/"):
    if '.csv' in file.name:
        df = pd.read_csv(BytesIO(file.download_as_string()))
        blob = bucket.blob(file.name.replace('csv', 'parquet'))
        blob.upload_from_string(df.to_parquet())
""")

    st.markdown("### Configure Snowflake Storage Integration and External Stage for Parquet files.")
    st.code("""

USE ROLE ACCOUNTADMIN;
CREATE STORAGE INTEGRATION GCP_INTEGRATION
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'GCS'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('gcs://torqata-808s/');
  
-- DESCRIBE STORAGE INTEGRATION GCP_INTEGRATION; 
-- (This will give us the STORAGE_GCP_SERVICE_ACCOUNT for permissions.)

GRANT USAGE ON INTEGRATION GCP_INTEGRATION TO ROLE SYSADMIN;

USE ROLE SYSADMIN;

CREATE OR REPLACE STAGE GCS_PARQUET_STAGE
  URL = 'gcs://torqata-808s/parquet_data/'
  STORAGE_INTEGRATION = GCP_INTEGRATION
  DIRECTORY = (ENABLE = TRUE
               REFRESH_ON_CREATE = TRUE);
""")

    st.markdown("### Create Snowflake Snowpark stored procedure to ingest Parquet files into SQL tables.")
    st.code("""
USE WAREHOUSE COMPUTE_WH;
  
CREATE OR REPLACE PROCEDURE LOAD_TABLES_FROM_PARQUET()
  RETURNS VARCHAR
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.8'
  PACKAGES = ('snowflake-snowpark-python', 'pandas')
  HANDLER = 'run'
AS
$$
import pandas as pd

def run(session):
    for file in session.sql("LS @GCS_PARQUET_STAGE").select('"name"').collect():
        table_name = file.name.split("/")[-1].upper().split(".")[0]
        filename = file.name.split("/")[-1].split(".")[0]
        df = session.read.parquet(f"@GCS_PARQUET_STAGE/{filename}.parquet").to_pandas()
        df.columns = [col.upper() for col in df.columns]
        session.create_dataframe(df).write.save_as_table(table_name, mode="overwrite")
    return "Success!"
$$;

CALL LOAD_TABLES_FROM_PARQUET();
""")

with de_tab:
    if "session" in st.session_state:
        st.markdown("## Data Exploration")
        tables = list(st.session_state.session.sql("SHOW TABLES;").filter(col('"kind"') == "TABLE").select('"name"').to_pandas()["name"])
        table = st.selectbox("Select a table", [""] + tables)
        if table:
            df = st.session_state.session.table(table).to_pandas()
            pr = df.profile_report()
            st_profile_report(pr)
