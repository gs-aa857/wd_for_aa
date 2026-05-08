import streamlit as st
import snowflake.connector
import pandas as pd
import datetime
import matplotlib.pyplot as plt
import io

# ------------------------------
# Set page config and apply dark theme styling
# ------------------------------
st.set_page_config(page_title="Weather Data Downloader & Visualizer", layout="wide")
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
# Snowflake Connection
# ------------------------------
@st.cache_resource(show_spinner=False)
def get_connection():
    try:
        private_key_p8 = st.secrets["snowflake"]["private_key"]
        return snowflake.connector.connect(
            user=st.secrets["snowflake"]["user"],
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
# Get column names from a table/view
# ------------------------------
def get_table_columns(table_name):
    query = (
        f"SELECT * FROM {st.secrets['snowflake']['database']}."
        f"{st.secrets['snowflake']['schema']}.{table_name} LIMIT 0"
    )
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
# Map frequency to enhanced view name and join keys
# ------------------------------
ENHANCED_VIEW = {
    "Daily":  {
        "table":      "weather_daily_deviations",
        "date_col":   "RECORD_DATE",
        "join_on":    ["RECORD_DATE", "COUNTRY_CODE"],
        # Columns that are already in the base view — exclude from selectable list
        "exclude":    {
            "DAILY_WEATHER_ID", "RECORD_DATE", "COUNTRY_CODE", "DOY",
            "TEMP_AVG", "TEMP_MIN", "TEMP_MAX",
            "APP_TEMP_AVG", "APP_TEMP_MIN", "APP_TEMP_MAX",
            "RAIN_SUM", "SNOWFALL_SUM", "SNOW_DEPTH_AVG", "SNOW_DEPTH_MAX",
            "CLOUD_COVER_AVG", "WIND_SPEED_AVG", "WIND_GUSTS_MAX",
            "WET_BULB_TEMP_AVG", "SUNSHINE_DURATION",
            "SW_RAD_AVG", "SW_RAD_MAX",
            "DIRECT_RAD_AVG", "DIRECT_RAD_MAX",
            "DIFFUSE_RAD_AVG", "DIFFUSE_RAD_MAX",
        },
    },
    "Weekly": {
        "table":      "weather_weekly_deviations",
        "date_col":   "RECORD_WEEK",
        "join_on":    ["RECORD_WEEK", "COUNTRY_CODE"],
        "exclude":    {
            "WEEKLY_WEATHER_ID", "RECORD_WEEK", "COUNTRY_CODE",
            "TEMP_AVG", "TEMP_AVG_HIGH", "TEMP_MIN", "TEMP_MAX",
            "APP_TEMP_AVG", "APP_TEMP_AVG_HIGH", "APP_TEMP_MIN", "APP_TEMP_MAX",
            "RAIN_AVG", "RAIN_SUM", "SNOWFALL_AVG", "SNOWFALL_SUM",
            "SNOW_DEPTH_AVG", "SNOW_DEPTH_AVG_HIGH", "SNOW_DEPTH_MAX",
            "CLOUD_COVER_AVG", "WIND_SPEED_AVG", "WIND_GUSTS_MAX",
            "WIND_GUSTS_AVG_HIGH", "WET_BULB_TEMP_AVG",
            "SUNSHINE_DURATION_AVG", "SUNSHINE_DURATION_MIN", "SUNSHINE_DURATION_MAX",
            "SW_RAD_AVG", "SW_RAD_AVG_HIGH", "SW_RAD_MAX",
            "DIRECT_RAD_AVG", "DIRECT_RAD_AVG_HIGH", "DIRECT_RAD_MAX",
            "DIFFUSE_RAD_AVG", "DIFFUSE_RAD_AVG_HIGH", "DIFFUSE_RAD_MAX",
        },
    },
}

# Seasonal anomaly config — always daily join via period_start/end
SEASONAL_ANOMALY_VIEW = "weather_seasonal_anomaly"

# ------------------------------
# Query additional variables from enhanced view
# ------------------------------
def query_enhanced(
    table_name: str,
    date_col: str,
    selected_cols: list,
    country: str,
    start_date: datetime.date,
    end_date: datetime.date,
) -> pd.DataFrame:
    cols = ", ".join(["COUNTRY_CODE", date_col] + selected_cols)
    query = f"""
        SELECT {cols}
        FROM {st.secrets['snowflake']['database']}.{st.secrets['snowflake']['schema']}.{table_name}
        WHERE COUNTRY_CODE = '{country}'
          AND {date_col} BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY {date_col}
    """
    try:
        conn = get_connection()
        return pd.read_sql(query, conn)
    except Exception as e:
        st.error(f"Error retrieving enhanced weather data: {e}")
        return pd.DataFrame()

# ------------------------------
# Query seasonal anomaly
# ------------------------------
def query_seasonal_anomaly(
    selected_cols: list,
    country: str,
    start_date: datetime.date,
    end_date: datetime.date,
    period_type: str,  # 'MONTH' or 'SEASON'
) -> pd.DataFrame:
    cols = ", ".join(
        ["COUNTRY_CODE", "PERIOD_TYPE", "PERIOD_START_DATE", "PERIOD_END_DATE"]
        + selected_cols
    )
    query = f"""
        SELECT {cols}
        FROM {st.secrets['snowflake']['database']}.{st.secrets['snowflake']['schema']}.{SEASONAL_ANOMALY_VIEW}
        WHERE COUNTRY_CODE = '{country}'
          AND PERIOD_TYPE = '{period_type}'
          AND PERIOD_START_DATE <= '{end_date}'
          AND PERIOD_END_DATE   >= '{start_date}'
        ORDER BY PERIOD_START_DATE
    """
    try:
        conn = get_connection()
        return pd.read_sql(query, conn)
    except Exception as e:
        st.error(f"Error retrieving seasonal anomaly data: {e}")
        return pd.DataFrame()

# ------------------------------
# Join seasonal anomaly to daily/weekly df via date range
# ------------------------------
def merge_seasonal_anomaly(
    base_df: pd.DataFrame,
    date_col: str,
    anomaly_df: pd.DataFrame,
    selected_cols: list,
) -> pd.DataFrame:
    if anomaly_df.empty:
        return base_df

    anomaly_df = anomaly_df.copy()
    anomaly_df["PERIOD_START_DATE"] = pd.to_datetime(anomaly_df["PERIOD_START_DATE"])
    anomaly_df["PERIOD_END_DATE"]   = pd.to_datetime(anomaly_df["PERIOD_END_DATE"])

    result = base_df.copy()
    result[date_col] = pd.to_datetime(result[date_col])

    # For each anomaly column, map value onto each date that falls in its window
    for col in selected_cols:
        renamed = f"OTHER-WeatherAnomaly_{col}"
        result[renamed] = None
        for _, row in anomaly_df.iterrows():
            mask = (
                (result[date_col] >= row["PERIOD_START_DATE"]) &
                (result[date_col] <= row["PERIOD_END_DATE"])
            )
            result.loc[mask, renamed] = row[col]

    return result


# ============================================================
# SEASONALITY GENERATORS  (pure Python / pandas)
# ============================================================

def _date_series(start: datetime.date, end: datetime.date) -> pd.Series:
    return pd.Series(pd.date_range(start, end, freq="D"), name="date")


def _week_series(start: datetime.date, end: datetime.date) -> pd.Series:
    s = pd.Timestamp(start) - pd.tseries.offsets.Week(weekday=0)
    e = pd.Timestamp(end)
    return pd.Series(pd.date_range(s, e, freq="W-MON"), name="date")


DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

def build_daily_seasonality(start: datetime.date, end: datetime.date) -> pd.DataFrame:
    dates = _date_series(start, end)
    df = pd.DataFrame({"DATE_NAME": dates})
    for i, day in enumerate(DAYS):
        df[f"S-Daily_{day}"] = (df["DATE_NAME"].dt.dayofweek == i).astype(int)
    return df


def _iso_month_week1_start(thursday: pd.Timestamp) -> pd.Timestamp:
    first_thu = pd.Timestamp(thursday.year, thursday.month, 4)
    return first_thu - pd.tseries.offsets.Week(weekday=0)


def build_weekly_seasonality(
    start: datetime.date, end: datetime.date, expand_to_daily: bool = False
) -> pd.DataFrame:
    weeks = _week_series(start, end)
    week_rows = []
    for week_start in weeks:
        week_end = week_start + pd.Timedelta(days=6)
        thursday = week_start + pd.Timedelta(days=3)

        month_wk1_start = _iso_month_week1_start(thursday)
        date_week = int((week_start - month_wk1_start).days // 7) + 1

        pd_week = int(
            week_end.month != week_start.month or week_start.day <= 8
        )

        prev_start = week_start - pd.Timedelta(days=7)
        prev_end   = week_end   - pd.Timedelta(days=7)
        week_after_pd = int(
            prev_end.month != prev_start.month or prev_start.day <= 8
        )

        week_rows.append({
            "DATE_NAME":                week_start,
            "S-Weekly_Week 1":          int(date_week == 1),
            "S-Weekly_Week 2":          int(date_week == 2),
            "S-Weekly_Week 3":          int(date_week == 3),
            "S-Weekly_Week 4":          int(date_week == 4),
            "S-Weekly_Week 5":          int(date_week == 5),
            "S-Weekly_PD Week":         pd_week,
            "S-Weekly_Week After PD":   week_after_pd,
        })

    df_weeks = pd.DataFrame(week_rows)

    if expand_to_daily:
        daily_rows = []
        sea_cols = [c for c in df_weeks.columns if c != "DATE_NAME"]
        for _, row in df_weeks.iterrows():
            for delta in range(7):
                day = row["DATE_NAME"] + pd.Timedelta(days=delta)
                entry = {"DATE_NAME": day}
                entry.update({c: row[c] for c in sea_cols})
                daily_rows.append(entry)
        df = pd.DataFrame(daily_rows)
        df = df[(df["DATE_NAME"].dt.date >= start) & (df["DATE_NAME"].dt.date <= end)]
    else:
        df = df_weeks
        df = df[(df["DATE_NAME"].dt.date >= start) & (df["DATE_NAME"].dt.date <= end)]

    return df.reset_index(drop=True)


MONTHS = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December"
]

def build_monthly_seasonality(start: datetime.date, end: datetime.date) -> pd.DataFrame:
    dates = _date_series(start, end)
    df = pd.DataFrame({"DATE_NAME": dates})
    for i, mon in enumerate(MONTHS, start=1):
        df[f"S-Monthly_{mon}"] = (df["DATE_NAME"].dt.month == i).astype(int)
    return df


# ============================================================
# HOLIDAY / SPECIAL DATE GENERATORS
# ============================================================

def _nth_weekday(year: int, month: int, weekday: int, n: int) -> datetime.date:
    first = datetime.date(year, month, 1)
    delta = (weekday - first.weekday()) % 7
    first_occurrence = first + datetime.timedelta(days=delta)
    return first_occurrence + datetime.timedelta(weeks=n - 1)


def _last_weekday(year: int, month: int, weekday: int) -> datetime.date:
    if month == 12:
        last = datetime.date(year + 1, 1, 1) - datetime.timedelta(days=1)
    else:
        last = datetime.date(year, month + 1, 1) - datetime.timedelta(days=1)
    delta = (last.weekday() - weekday) % 7
    return last - datetime.timedelta(days=delta)


def _dst_start(year: int) -> datetime.date:
    return _last_weekday(year, 3, 6)


def _dst_end(year: int) -> datetime.date:
    return _last_weekday(year, 10, 6)


def _black_weekend_dates(year: int) -> list:
    black_friday = _nth_weekday(year, 11, 4, 4)
    return [black_friday + datetime.timedelta(days=i) for i in range(3)]


def _e_esmaspaev_dates(year: int) -> list:
    KNOWN = {
        2014: [datetime.date(2014, 11, 10)],
        2015: [datetime.date(2015,  5, 11), datetime.date(2015, 11,  9)],
        2016: [datetime.date(2016,  5,  9), datetime.date(2016, 11, 14)],
        2017: [datetime.date(2017,  5,  8), datetime.date(2017, 11, 13)],
        2018: [datetime.date(2018,  5, 14), datetime.date(2018, 11, 12)],
        2019: [datetime.date(2019,  5, 13), datetime.date(2019, 11, 11)],
        2020: [datetime.date(2020,  5, 11), datetime.date(2020, 11,  9)],
        2021: [datetime.date(2021,  5, 10), datetime.date(2021, 11,  8)],
        2022: [datetime.date(2022,  5,  9), datetime.date(2022, 11, 14)],
        2023: [datetime.date(2023,  3, 13), datetime.date(2023,  5,  8),
               datetime.date(2023,  9, 11), datetime.date(2023, 11, 13)],
        2024: [datetime.date(2024,  3, 11), datetime.date(2024,  5, 13),
               datetime.date(2024,  9,  9), datetime.date(2024, 11, 11)],
        2025: [datetime.date(2025,  3, 10), datetime.date(2025,  5, 12),
               datetime.date(2025,  9,  8), datetime.date(2025, 11, 10)],
    }
    if year in KNOWN:
        return KNOWN[year]
    return [_nth_weekday(year, m, 0, 2) for m in (3, 5, 9, 11)]


def _easter(year: int) -> datetime.date:
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day   = ((h + l - 7 * m + 114) % 31) + 1
    return datetime.date(year, month, day)


OBSERVANCE_RULES = {
    "H-Mothers Day":             lambda y, cc: _nth_weekday(y, 5, 6, 2),
    "H-Fathers Day":             lambda y, cc: _nth_weekday(y, 11, 6, 2),
    "H-Valentines Day":          lambda y, cc: datetime.date(y, 2, 14),
    "H-Easter Sunday":           lambda y, cc: _easter(y),
    "H-Easter Monday":           lambda y, cc: _easter(y) + datetime.timedelta(days=1),
    "H-Good Friday":             lambda y, cc: _easter(y) - datetime.timedelta(days=2),
    "H-Pentecost":               lambda y, cc: _easter(y) + datetime.timedelta(days=49),
    "H-Midsummer Eve":           lambda y, cc: (
        _nth_weekday(y, 6, 4, 3) if cc == "FI" else datetime.date(y, 6, 23)
    ),
    "H-Halloween":               lambda y, cc: datetime.date(y, 10, 31),
    "H-Christmas Eve":           lambda y, cc: datetime.date(y, 12, 24),
    "H-Christmas Day":           lambda y, cc: datetime.date(y, 12, 25),
    "H-Boxing Day":              lambda y, cc: datetime.date(y, 12, 26),
    "H-New Years Eve":           lambda y, cc: datetime.date(y, 12, 31),
    "H-New Years Day":           lambda y, cc: datetime.date(y, 1, 1),
    "H-Black Friday":            lambda y, cc: _nth_weekday(y, 11, 4, 4),
    "H-Black and Cyber Weekend": lambda y, cc: _black_weekend_dates(y),
    "H-Cyber Monday":            lambda y, cc: _nth_weekday(y, 11, 4, 4) + datetime.timedelta(days=3),
    "H-DST Start":               lambda y, cc: _dst_start(y),
    "H-DST End":                 lambda y, cc: _dst_end(y),
    "H-E-esmaspaev":             lambda y, cc: _e_esmaspaev_dates(y) if cc == "EE" else None,
}

COUNTRY_FIXED_HOLIDAYS = {
    "EE": {
        "H-Independence Day":          (2, 24),
        "H-Victory Day":               (6, 23),
        "H-Restoration Day":           (8, 20),
        "H-Christmas Day (2nd)":       (12, 26),
        "H-Beginning of School Year":  (9, 1),
    },
    "LV": {
        "H-Independence Day":          (11, 18),
        "H-Restoration Day":           (5, 4),
        "H-Lacplesis Day":             (11, 11),
        "H-Beginning of School Year":  (9, 1),
    },
    "LT": {
        "H-Independence Day":          (2, 16),
        "H-Restoration Day":           (3, 11),
        "H-Statehood Day":             (7, 6),
        "H-All Saints Day":            (11, 1),
        "H-Beginning of School Year":  (9, 1),
    },
    "FI": {
        "H-Independence Day":          (12, 6),
        "H-Epiphany":                  (1, 6),
        "H-All Saints Day":            (11, 1),
    },
}

SHARED_FIXED = {
    "H-May Day":                  (5, 1),
    "H-International Womens Day": (3, 8),
}


def build_special_dates(
    start: datetime.date,
    end: datetime.date,
    frequency: str,
    country_code: str,
) -> pd.DataFrame:
    years = range(start.year, end.year + 1)
    holiday_dates: dict = {}

    for col, rule in OBSERVANCE_RULES.items():
        holiday_dates[col] = set()
        for y in years:
            try:
                result = rule(y, country_code)
                if result is None:
                    continue
                if isinstance(result, list):
                    for d in result:
                        holiday_dates[col].add(d)
                else:
                    holiday_dates[col].add(result)
            except Exception:
                pass

    for col, (m, day) in COUNTRY_FIXED_HOLIDAYS.get(country_code, {}).items():
        holiday_dates[col] = set()
        for y in years:
            try:
                holiday_dates[col].add(datetime.date(y, m, day))
            except Exception:
                pass

    for col, (m, day) in SHARED_FIXED.items():
        holiday_dates[col] = set()
        for y in years:
            try:
                holiday_dates[col].add(datetime.date(y, m, day))
            except Exception:
                pass

    if frequency == "Daily":
        dates = _date_series(start, end)
        df = pd.DataFrame({"DATE_NAME": dates})
        date_col = df["DATE_NAME"].dt.date
    else:
        weeks = _week_series(start, end)
        df = pd.DataFrame({"DATE_NAME": weeks})
        date_col = None

    all_cols = sorted(holiday_dates.keys())
    if frequency == "Daily":
        for col in all_cols:
            hset = holiday_dates[col]
            df[col] = date_col.apply(lambda d: 1 if d in hset else 0)
    else:
        for col in all_cols:
            hset = holiday_dates[col]
            def week_hit(week_start, hset=hset):
                for delta in range(7):
                    if (week_start + pd.Timedelta(days=delta)).date() in hset:
                        return 1
                return 0
            df[col] = df["DATE_NAME"].apply(week_hit)

    df = df[(df["DATE_NAME"].dt.date >= start) & (df["DATE_NAME"].dt.date <= end)]
    return df.reset_index(drop=True)


# ============================================================
# MERGE helper
# ============================================================

def merge_seasonality(
    weather_df: pd.DataFrame,
    date_col: str,
    seasonality_dfs: list,
) -> pd.DataFrame:
    result = weather_df.copy()
    result[date_col] = pd.to_datetime(result[date_col])

    for sea_df in seasonality_dfs:
        sea_df = sea_df.copy()
        sea_df["DATE_NAME"] = pd.to_datetime(sea_df["DATE_NAME"])
        result = result.merge(
            sea_df,
            left_on=date_col,
            right_on="DATE_NAME",
            how="left",
        )
        if "DATE_NAME" in result.columns and "DATE_NAME" != date_col:
            result.drop(columns=["DATE_NAME"], inplace=True)

    return result


# ============================================================
# MAIN APP
# ============================================================

st.title("Weather Data Downloader & Visualizer")

# --- Frequency ---
frequency = st.radio("Select Data Frequency", options=["Daily", "Weekly"])

if frequency == "Daily":
    table_name     = "weather_daily"
    date_column    = "RECORD_DATE"
    hidden_columns = ["DAILY_WEATHER_ID", "RECORD_DATE"]
else:
    table_name     = "weather_weekly"
    date_column    = "RECORD_WEEK"
    hidden_columns = ["WEEKLY_WEATHER_ID", "RECORD_WEEK"]

# --- Country ---
country = st.selectbox("Select Country", options=["EE", "LV", "LT", "FI"])

# --- Date range ---
today      = datetime.date.today()
last_month = today.replace(day=1) - datetime.timedelta(days=1)
start_date = datetime.date(2019, 1, 1)
end_date   = last_month
date_range = st.date_input(
    "Select Date Range",
    value=(start_date, end_date),
    min_value=start_date,
    max_value=end_date,
)

# ── Weather columns ────────────────────────────────────────────────────
st.subheader("Weather Variables")
all_columns = get_table_columns(table_name)
if not all_columns:
    st.stop()

selectable_columns = [
    col for col in all_columns
    if col not in hidden_columns and col != "COUNTRY_CODE"
]
default_vars   = ["TEMP_AVG", "APP_TEMP_AVG", "RAIN_SUM", "SNOWFALL_SUM"]
default_select = [col for col in selectable_columns if col in default_vars]

select_all = st.checkbox("Select All Weather Columns", value=False)
if select_all:
    selected_columns = st.multiselect(
        "Select Weather Columns", options=selectable_columns, default=selectable_columns
    )
else:
    selected_columns = st.multiselect(
        "Select Weather Columns", options=selectable_columns, default=default_select
    )

# ── Additional Weather Variables ───────────────────────────────────────
st.subheader("Additional Weather Variables")
st.caption(
    "Deviation and anomaly variables from enhanced views. "
    "Deviations are vs the 1991–2020 climate normal for that day-of-year."
)

enhanced_cfg = ENHANCED_VIEW[frequency]
enhanced_all_cols = get_table_columns(enhanced_cfg["table"])
enhanced_selectable = [
    col for col in enhanced_all_cols
    if col not in enhanced_cfg["exclude"]
]

select_all_enhanced = st.checkbox("Select All Additional Columns", value=False)
if select_all_enhanced:
    selected_enhanced = st.multiselect(
        "Select Additional Variables",
        options=enhanced_selectable,
        default=enhanced_selectable,
    )
else:
    selected_enhanced = st.multiselect(
        "Select Additional Variables",
        options=enhanced_selectable,
        default=[],
    )

# ── Seasonal Anomaly Variables ─────────────────────────────────────────
st.subheader("Seasonal Anomaly Variables")
st.caption(
    "Month- or season-level anomalies vs the 1991–2020 baseline. "
    "Each value is broadcast to all days/weeks within that period."
)

seasonal_period_type = st.radio(
    "Seasonal Period Type",
    options=["None", "MONTH", "SEASON"],
    horizontal=True,
)

selected_seasonal = []
if seasonal_period_type != "None":
    seasonal_all_cols = get_table_columns(SEASONAL_ANOMALY_VIEW)
    # Exclude identity/join columns from selection
    seasonal_exclude = {
        "COUNTRY_CODE", "PERIOD_TYPE", "PERIOD_YEAR", "PERIOD_VALUE",
        "SEASON_CODE", "PERIOD_START_DATE", "PERIOD_END_DATE", "DAY_COUNT",
    }
    seasonal_selectable = [
        col for col in seasonal_all_cols
        if col not in seasonal_exclude
    ]

    select_all_seasonal = st.checkbox("Select All Seasonal Anomaly Columns", value=False)
    if select_all_seasonal:
        selected_seasonal = st.multiselect(
            "Select Seasonal Anomaly Variables",
            options=seasonal_selectable,
            default=seasonal_selectable,
        )
    else:
        selected_seasonal = st.multiselect(
            "Select Seasonal Anomaly Variables",
            options=seasonal_selectable,
            default=[],
        )

# ── Seasonality options ────────────────────────────────────────────────
st.subheader("Seasonality Variables")

if frequency == "Daily":
    available_seas = ["Daily", "Weekly", "Monthly"]
else:
    available_seas = ["Weekly", "Monthly"]

selected_seas = st.multiselect(
    "Select Seasonality Types to Include",
    options=available_seas,
    default=[],
    help=(
        "Daily = Mon-Sun dummies | "
        "Weekly = ISO-week-in-month position dummies | "
        "Monthly = Jan-Dec dummies"
    ),
)

include_holidays = st.checkbox(
    "Include Special / Holiday Dates",
    value=False,
    help="Adds 0/1 dummy columns for national and cultural holidays for the selected country.",
)

# ── Download ───────────────────────────────────────────────────────────
if st.button("Download Data"):
    if len(date_range) != 2:
        st.warning("Please select a valid date range.")
        st.stop()

    range_start, range_end = date_range[0], date_range[1]

    # --- Base weather query ---
    columns_to_select = hidden_columns + selected_columns
    query = f"""
        SELECT {', '.join(columns_to_select)}
        FROM {st.secrets['snowflake']['database']}.{st.secrets['snowflake']['schema']}.{table_name}
        WHERE COUNTRY_CODE = '{country}'
          AND {date_column} BETWEEN '{range_start}' AND '{range_end}'
        ORDER BY {date_column}
    """
    try:
        with st.spinner("Querying Snowflake..."):
            conn = get_connection()
            df = pd.read_sql(query, conn)
    except Exception as e:
        st.error(f"Error retrieving weather data: {e}")
        st.stop()

    # --- Enhanced variables ---
    if selected_enhanced:
        with st.spinner("Querying enhanced weather variables..."):
            enhanced_df = query_enhanced(
                table_name   = enhanced_cfg["table"],
                date_col     = enhanced_cfg["date_col"],
                selected_cols= selected_enhanced,
                country      = country,
                start_date   = range_start,
                end_date     = range_end,
            )
        if not enhanced_df.empty:
            enhanced_df[enhanced_cfg["date_col"]] = pd.to_datetime(
                enhanced_df[enhanced_cfg["date_col"]]
            )
            df[date_column] = pd.to_datetime(df[date_column])
            df = df.merge(
                enhanced_df.drop(columns=["COUNTRY_CODE"], errors="ignore"),
                left_on  = date_column,
                right_on = enhanced_cfg["date_col"],
                how      = "left",
            )
            # Drop duplicate date col if it appeared under a different name
            if (
                enhanced_cfg["date_col"] != date_column
                and enhanced_cfg["date_col"] in df.columns
            ):
                df.drop(columns=[enhanced_cfg["date_col"]], inplace=True)

    # --- Seasonal anomaly variables ---
    if selected_seasonal and seasonal_period_type != "None":
        with st.spinner("Querying seasonal anomaly variables..."):
            anomaly_df = query_seasonal_anomaly(
                selected_cols = selected_seasonal,
                country       = country,
                start_date    = range_start,
                end_date      = range_end,
                period_type   = seasonal_period_type,
            )
        df = merge_seasonal_anomaly(
            base_df      = df,
            date_col     = date_column,
            anomaly_df   = anomaly_df,
            selected_cols= selected_seasonal,
        )

    # --- Seasonality ---
    seasonality_frames = []
    with st.spinner("Building seasonality variables..."):
        if "Daily" in selected_seas:
            seasonality_frames.append(build_daily_seasonality(range_start, range_end))
        if "Weekly" in selected_seas:
            seasonality_frames.append(
                build_weekly_seasonality(
                    range_start, range_end,
                    expand_to_daily=(frequency == "Daily"),
                )
            )
        if "Monthly" in selected_seas:
            seasonality_frames.append(build_monthly_seasonality(range_start, range_end))
        if include_holidays:
            seasonality_frames.append(
                build_special_dates(range_start, range_end, frequency, country)
            )

    if seasonality_frames:
        df = merge_seasonality(df, date_column, seasonality_frames)

    # --- Rename weather columns ---
    weather_col_rename = {
        col: f"OTHER-Weather_{col}"
        for col in selected_columns
        if col in df.columns
    }
    df.rename(columns=weather_col_rename, inplace=True)
    renamed_weather_cols = list(weather_col_rename.values())

    # --- Rename enhanced columns ---
    enhanced_col_rename = {
        col: f"OTHER-WeatherDev_{col}"
        for col in selected_enhanced
        if col in df.columns
    }
    df.rename(columns=enhanced_col_rename, inplace=True)
    renamed_enhanced_cols = list(enhanced_col_rename.values())

    st.session_state["df"]               = df
    st.session_state["date_column"]      = date_column
    st.session_state["selected_columns"] = renamed_weather_cols + renamed_enhanced_cols

    st.success("Data retrieved successfully!")
    st.dataframe(df)

# ── Show download buttons & chart whenever data exists in session ──────
if "df" in st.session_state:
    df               = st.session_state["df"]
    date_column      = st.session_state["date_column"]
    selected_columns = st.session_state.get("selected_columns", [])

    download_format = st.radio("Select Download Format", options=["CSV", "Excel"])
    if download_format == "CSV":
        csv_data = df.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="Download as CSV",
            data=csv_data,
            file_name="weather_data.csv",
            mime="text/csv",
        )
    else:
        towrite = io.BytesIO()
        with pd.ExcelWriter(towrite, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="WeatherData")
        towrite.seek(0)
        st.download_button(
            label="Download as Excel",
            data=towrite,
            file_name="import_data.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    if not df.empty and date_column in df.columns:
        st.subheader("Data Visualization")
        try:
            df[date_column] = pd.to_datetime(df[date_column])
        except Exception as e:
            st.error(f"Error converting {date_column} to datetime: {e}")

        plot_cols = [
            c for c in selected_columns
            if c in df.columns and pd.api.types.is_numeric_dtype(df[c])
        ]

        if not plot_cols:
            st.info("No numeric weather columns selected for visualization.")
        else:
            fig, ax = plt.subplots(figsize=(12, 6))
            ax.set_facecolor("#2f2f2f")
            fig.patch.set_facecolor("#2f2f2f")
            colors = plt.cm.tab10.colors

            for i, var in enumerate(plot_cols):
                series = df[var]
                if series.max() != series.min():
                    norm_series = (series - series.min()) / (series.max() - series.min())
                else:
                    norm_series = series
                ax.plot(
                    df[date_column], norm_series,
                    label=var,
                    color=colors[i % len(colors)],
                    linewidth=0.5,
                    alpha=0.6,
                )

            ax.set_title("Seasonality Plot", color="white")
            ax.set_xlabel("Date", color="white")
            ax.set_ylabel("Normalized Value", color="white")
            ax.tick_params(axis="x", colors="white")
            ax.tick_params(axis="y", colors="white")
            ax.legend(facecolor="#2f2f2f", edgecolor="white", labelcolor="white")
            st.pyplot(fig)
