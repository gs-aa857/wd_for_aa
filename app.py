import streamlit as st
import snowflake.connector
import pandas as pd
import datetime
import matplotlib.pyplot as plt
# from bokeh.plotting import figure
# from bokeh.models import ColumnDataSource
# from bokeh.palettes import Category10
import io

# ------------------------------
# Set page config and apply dark theme styling
# ------------------------------
st.set_page_config(page_title="Weather Data Downloader & Visualizer", layout="wide")
# Basic dark theme CSS for the Streamlit app
st.markdown(
    """
    <style>
    .reportview-container {
        background-color: #1e1e1e;
        color: #ffffff;
    }
    .sidebar .sidebar-content {
        background-color: #1e1e1e;
    }
    </style>
    """,
    unsafe_allow_html=True
)

# ------------------------------
# Snowflake Connection (secrets stored in .streamlit/secrets.toml)
# ------------------------------
@st.cache_resource(show_spinner=False)
def get_connection():
    try:
        private_key_p8 = st.secrets["snowflake"]["private_key"]
        # private_key_base64 = st.secrets["snowflake"]["private_key"]
        # private_key_p8 = base64.b64decode(private_key_base64).decode("utf-8")
        
        return snowflake.connector.connect(
            user=st.secrets["snowflake"]["user"],
            # password=st.secrets["snowflake"]["password"],
            account=st.secrets["snowflake"]["account"],
            warehouse=st.secrets["snowflake"]["warehouse"],
            database=st.secrets["snowflake"]["database"],
            schema=st.secrets["snowflake"]["schema"],
            private_key=private_key_p8
        )
    except Exception as e:
        st.error(f"Failed to connect to Snowflake: {e}")
        return None

# ------------------------------
# Get column names from a table (returns list of column names)
# ------------------------------
def get_table_columns(table_name):
    query = f"SELECT * FROM {st.secrets['snowflake']['database']}.{st.secrets['snowflake']['schema']}.{table_name} LIMIT 0"
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(query)
        cols = [desc[0] for desc in cur.description]
        cur.close()
        return cols
    except Exception as e:
        st.error(f"Error retrieving columns: {e}")
        return []

# ------------------------------
# Main App Interface
# ------------------------------
st.title("Weather Data Downloader & Visualizer")

# Frequency Selection
frequency = st.radio("Select Data Frequency", options=["Daily", "Weekly"])

# Define table, date column, and hidden (always included) columns based on frequency
if frequency == "Daily":
    table_name = "weather_daily"
    date_column = "RECORD_DATE"
    hidden_columns = ["DAILY_WEATHER_ID", "RECORD_DATE"]  # always included but not selectable
elif frequency == "Weekly":
    table_name = "weather_weekly"
    date_column = "RECORD_WEEK"
    hidden_columns = ["WEEKLY_WEATHER_ID", "RECORD_WEEK"]

# Country selection (filter is done on COUNTRY_CODE)
country = st.selectbox("Select Country", options=["EE", "LV", "LT", "FI"])

# Date range selection: from 2019-01-01 until end of last month
today = datetime.date.today()
last_month = today.replace(day=1) - datetime.timedelta(days=1)
start_date = datetime.date(2019, 1, 1)
end_date = last_month
date_range = st.date_input(
    "Select Date Range",
    value=(start_date, end_date),
    min_value=start_date,
    max_value=end_date
)

# Retrieve all columns from the selected table
all_columns = get_table_columns(table_name)
if not all_columns:
    st.stop()

# Exclude the hidden columns and COUNTRY_CODE from the selectable list
selectable_columns = [col for col in all_columns if col not in hidden_columns and col != "COUNTRY_CODE"]

# Preselect default variables if available (for both daily and weekly)
default_vars = ["TEMP_AVG", "APP_TEMP_AVG", "RAIN_SUM", "SNOWFALL_SUM"]
default_select = [col for col in selectable_columns if col in default_vars]

# Option for selecting all columns
select_all = st.checkbox("Select All Columns", value=False)
if select_all:
    selected_columns = st.multiselect("Select Columns", options=selectable_columns, default=selectable_columns)
else:
    selected_columns = st.multiselect("Select Columns", options=selectable_columns, default=default_select)

if selected_columns == []:
    st.warning("No selectable variables have been chosen. Only hidden columns will be included in the download and visualization.")

# ------------------------------
# Data Retrieval Button and Query Execution
# ------------------------------
if st.button("Download Data"):
    # Build the list of columns to retrieve: always include the hidden columns plus the user-selected ones.
    columns_to_select = hidden_columns + selected_columns
    query = f"""
    SELECT {', '.join(columns_to_select)}
    FROM {st.secrets['snowflake']['database']}.{st.secrets['snowflake']['schema']}.{table_name}
    WHERE COUNTRY_CODE = '{country}'
      AND {date_column} BETWEEN '{date_range[0]}' AND '{date_range[1]}'
    ORDER BY {date_column}
    """
    try:
        with st.spinner("Querying Snowflake..."):
            conn = get_connection()
            df = pd.read_sql(query, conn)
            #conn.close()
        
        # Store the DataFrame in session state to prevent re-querying
        st.session_state["df"] = df
        
        st.success("Data retrieved successfully!")
        st.dataframe(df)

    except Exception as e:
        st.error(f"Error retrieving data: {e}")
        
if "df" in st.session_state:
    df = st.session_state["df"]
    
    # ------------------------------
    # Download Format Selection: CSV or Excel
    # ------------------------------
    download_format = st.radio("Select Download Format", options=["CSV", "Excel"])
    if download_format == "CSV":
        csv_data = df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="Download as CSV",
            data=csv_data,
            file_name="weather_data.csv",
            mime="text/csv"
        )
        
    else:
        # Write DataFrame to an in-memory Excel file
        towrite = io.BytesIO()
        with pd.ExcelWriter(towrite, engine="openpyxl") as writer:  # Use openpyxl instead of xlsxwriter
            df.to_excel(writer, index=False, sheet_name="WeatherData")
        # with pd.ExcelWriter(towrite, engine='xlsxwriter') as writer:
        #     df.to_excel(writer, index=False, sheet_name="WeatherData")
        #     writer.save()
        towrite.seek(0)
        st.download_button(
            label="Download as Excel",
            data=towrite,
            file_name="weather_data.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

# ------------------------------
# Visualization using Bokeh (only if data has been loaded)
# ------------------------------
if 'df' in locals() and not df.empty:
    st.subheader("Data Visualization")

    # Check if the date column exists
    if date_column not in df.columns:
        st.warning(f"The data does not contain the required date column '{date_column}' for plotting.")
    else:
        # Convert the date column to datetime
        try:
            df[date_column] = pd.to_datetime(df[date_column])
        except Exception as e:
            st.error(f"Error converting {date_column} to datetime: {e}")

        # Create a Matplotlib figure
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.set_facecolor("#2f2f2f")  # Dark background
        fig.patch.set_facecolor("#2f2f2f")

        # Define colors
        colors = plt.cm.tab10.colors

        # Plot each selected variable as a normalized time series
        for i, var in enumerate(selected_columns):
            if var not in df.columns:
                continue
            series = df[var]
            # Normalize using min-max scaling
            if series.max() != series.min():
                norm_series = (series - series.min()) / (series.max() - series.min())
            else:
                norm_series = series  # Avoid division by zero if constant column

            ax.plot(df[date_column], norm_series, label=var, color=colors[i % len(colors)], linewidth=0.5, alpha=0.6)

        # Format the plot
        ax.set_title("Seasonality Plot", color="white")
        ax.set_xlabel("Date", color="white")
        ax.set_ylabel("Normalized Value", color="white")
        ax.tick_params(axis='x', colors="white")
        ax.tick_params(axis='y', colors="white")
        ax.legend(facecolor="#2f2f2f", edgecolor="white", labelcolor="white")

        # Display the plot in Streamlit
        st.pyplot(fig)
