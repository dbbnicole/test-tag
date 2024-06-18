from databricks import sql
from databricks.sdk.core import Config
import streamlit as st
import numpy as np
import pandas as pd
import os
from streamlit.web.server.websocket_headers import _get_websocket_headers

cfg = Config()


def _get_user_info():
    headers = _get_websocket_headers()
    return dict(
        user_name=headers.get("X-Forwarded-Preferred-Username"),
        user_email=headers.get("X-Forwarded-Email"),
        user_id=headers.get("X-Forwarded-User"),
        access_token=headers.get("X-Forwarded-Access-Token")
    )

user_info = _get_user_info()

def _get_user_credentials() -> dict[str, str]:
    return dict(Authorization=f"Bearer {user_info.get('access_token')}")



st.set_page_config(page_title="Streamlit App in Databricks!", page_icon="", layout="wide")


# add current user info
st.sidebar.title("User Info")
st.sidebar.write(f"User Name: {user_info.get('user_name')}")
st.sidebar.write(f"User Email: {user_info.get('user_email')}")
st.sidebar.write(f"User ID: {user_info.get('user_id')}")


st.sidebar.title("Authentication Method")
service_principal = 'Service Principal'
app_user = 'App User'
options = [app_user, service_principal]
default_index = options.index(app_user)
st.session_state.authentication_method = st.sidebar.selectbox('Choose the authentication method', options, index=default_index)


def get_credentials(method):
    if method == app_user:
        return lambda: _get_user_credentials
    else:
        return lambda: cfg.authenticate


@st.cache_data(ttl=30)
def get_scatter_data(method):
    os.write(1, f"Fetching data from Databricks using {method}\n".encode())
    os.write(1, f"{get_credentials(method)()()}\n".encode())

    connection = sql.connect(
        server_hostname=cfg.host,
        http_path=f"""/sql/1.0/warehouses/{os.getenv("DATABRICKS_WAREHOUSE_ID")}""",
        credentials_provider=get_credentials(method),
    )
    cursor = connection.cursor()

    try:
        cursor.execute(
            f"""
            SELECT
                tpep_pickup_datetime as pickup_time,
                trip_distance,
                fare_amount
            FROM andre.nyctaxi.trips;
            """
        )

        df = cursor.fetchall_arrow().to_pandas()
        return df
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        connection.close()


def get_taxi_df():
    return get_scatter_data(st.session_state.authentication_method)

st.header("NYC Taxi Trips")
data = get_taxi_df()

if st.checkbox('Show raw data'):
    st.subheader("Raw Data")
    st.dataframe(data=data, height=600, use_container_width=True)


st.subheader("Number of pickups by hour")
if data is not None:
    hist_values = np.histogram(data['pickup_time'].dt.hour, bins=24, range=(0, 24))[0]
    st.bar_chart(hist_values)
else:
    st.write("No data available")

# st.subheader("Distance/Fare Amount Scatter Plot")
# if data is not None:
#     data['pickup_time'] = pd.to_datetime(data['pickup_time']).dt.date
#     st.scatter_chart(data=data, x="pickup_time", y="trip_distance", color="fare_amount", size=6)
# else:
#     st.write("No data available")

