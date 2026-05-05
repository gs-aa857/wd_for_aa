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
# Get column names from a table
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

# ============================================================
# SEASONALITY GENERATORS  (pure Python / pandas)
# ============================================================

# --- helpers -----------------------------------------------------------

def _date_series(start: datetime.date, end: datetime.date) -> pd.Series:
    return pd.Series(pd.date_range(start, end, freq="D"), name="date")


def _week_series(start: datetime.date, end: datetime.date) -> pd.Series:
    """Return Monday-anchored week-start dates covering [start, end]."""
    s = pd.Timestamp(start) - pd.tseries.offsets.Week(weekday=0)
    e = pd.Timestamp(end)
    return pd.Series(pd.date_range(s, e, freq="W-MON"), name="date")


# --- daily seasonality -------------------------------------------------

DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

def build_daily_seasonality(start: datetime.date, end: datetime.date) -> pd.DataFrame:
    dates = _date_series(start, end)
    df = pd.DataFrame({"DATE_NAME": dates})
    for i, day in enumerate(DAYS):
        df[f"S-Daily_{day}"] = (df["DATE_NAME"].dt.dayofweek == i).astype(int)
    return df


# --- weekly seasonality ------------------------------------------------
# Mirrors the Snowflake SQL logic exactly:
#   • date_week = ISO week number relative to the first ISO week of the month
#   • pd_week   = week whose [Mon..Sun] window straddles a month boundary
#                 OR whose Thursday falls in the first 8 days of the month
#   • week_after_pd = same test shifted one week earlier

def _iso_month_week1_start(thursday: pd.Timestamp) -> pd.Timestamp:
    """Monday of the first ISO week of thursday's month (uses day-4 anchor)."""
    first_thu = pd.Timestamp(thursday.year, thursday.month, 4)
    # Monday of that week
    return first_thu - pd.tseries.offsets.Week(weekday=0)


def build_weekly_seasonality(
    start: datetime.date, end: datetime.date, expand_to_daily: bool = False
) -> pd.DataFrame:
    """
    Build weekly seasonality dummies.

    When expand_to_daily=True (Daily frequency selected), each week's values
    are broadcast to all 7 days (Mon–Sun) so the result aligns to a daily grid.
    When False (Weekly frequency), one row per week-start Monday is returned.
    """
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
        # Broadcast each week's values across all 7 days (Mon–Sun)
        daily_rows = []
        sea_cols = [c for c in df_weeks.columns if c != "DATE_NAME"]
        for _, row in df_weeks.iterrows():
            for delta in range(7):
                day = row["DATE_NAME"] + pd.Timedelta(days=delta)
                entry = {"DATE_NAME": day}
                entry.update({c: row[c] for c in sea_cols})
                daily_rows.append(entry)
        df = pd.DataFrame(daily_rows)
        # Trim to the requested [start, end] window
        df = df[(df["DATE_NAME"].dt.date >= start) & (df["DATE_NAME"].dt.date <= end)]
    else:
        df = df_weeks
        df = df[(df["DATE_NAME"].dt.date >= start) & (df["DATE_NAME"].dt.date <= end)]

    return df.reset_index(drop=True)


# --- monthly seasonality -----------------------------------------------

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


# --- special / holiday dates -------------------------------------------

def _nth_weekday(year: int, month: int, weekday: int, n: int) -> datetime.date:
    """Return the n-th occurrence (1-based) of weekday (0=Mon) in month/year."""
    first = datetime.date(year, month, 1)
    delta = (weekday - first.weekday()) % 7
    first_occurrence = first + datetime.timedelta(days=delta)
    return first_occurrence + datetime.timedelta(weeks=n - 1)


def _last_weekday(year: int, month: int, weekday: int) -> datetime.date:
    """Return the last occurrence of weekday in month/year."""
    # Go to first of next month, subtract until we hit the weekday
    if month == 12:
        last = datetime.date(year + 1, 1, 1) - datetime.timedelta(days=1)
    else:
        last = datetime.date(year, month + 1, 1) - datetime.timedelta(days=1)
    delta = (last.weekday() - weekday) % 7
    return last - datetime.timedelta(days=delta)


# Country-specific observance rules
# Each entry: (column_name, lambda year, country_code -> datetime.date | None)
OBSERVANCE_RULES = {
    # Mothers Day: 2nd Sunday of May (EE, LV, LT, FI all observe this)
    "H-Mothers Day": lambda y, cc: _nth_weekday(y, 5, 6, 2),
    # Fathers Day: 2nd Sunday of November
    "H-Fathers Day": lambda y, cc: _nth_weekday(y, 11, 6, 2),
    # Valentines Day: Feb 14
    "H-Valentines Day": lambda y, cc: datetime.date(y, 2, 14),
    # Easter Sunday (Anonymous Gregorian algorithm)
    "H-Easter Sunday": lambda y, cc: _easter(y),
    # Easter Monday
    "H-Easter Monday": lambda y, cc: _easter(y) + datetime.timedelta(days=1),
    # Good Friday
    "H-Good Friday": lambda y, cc: _easter(y) - datetime.timedelta(days=2),
    # Midsummer / St Johns Eve:
    #   EE/LT/LV: Jun 23  |  FI: Friday between Jun 19-25
    "H-Midsummer Eve": lambda y, cc: (
        _nth_weekday(y, 6, 4, 3) if cc == "FI"   # 3rd Friday – covers Fri 19-25
        else datetime.date(y, 6, 23)
    ),
    # Christmas Eve: Dec 24
    "H-Christmas Eve": lambda y, cc: datetime.date(y, 12, 24),
    # Christmas Day: Dec 25
    "H-Christmas Day": lambda y, cc: datetime.date(y, 12, 25),
    # Boxing Day / 2nd Christmas: Dec 26
    "H-Boxing Day": lambda y, cc: datetime.date(y, 12, 26),
    # New Years Eve: Dec 31
    "H-New Years Eve": lambda y, cc: datetime.date(y, 12, 31),
    # New Years Day: Jan 1
    "H-New Years Day": lambda y, cc: datetime.date(y, 1, 1),
    # Black Friday: 4th Friday of November
    "H-Black Friday": lambda y, cc: _nth_weekday(y, 11, 4, 4),
}

# Country-specific public holidays (name -> (month, day))  – fixed dates only
COUNTRY_FIXED_HOLIDAYS = {
    "EE": {
        "H-Independence Day":        (2, 24),
        "H-Victory Day":             (6, 23),
        "H-Restoration Day":         (8, 20),
        "H-Christmas Day (2nd)":     (12, 26),
    },
    "LV": {
        "H-Independence Day":        (11, 18),
        "H-Restoration Day":         (5, 4),
        "H-Lacplesis Day":           (11, 11),
    },
    "LT": {
        "H-Independence Day":        (2, 16),
        "H-Restoration Day":         (3, 11),
        "H-Statehood Day":           (7, 6),
        "H-All Saints Day":          (11, 1),
    },
    "FI": {
        "H-Independence Day":        (12, 6),
        "H-Epiphany":                (1, 6),
        "H-All Saints Day":          (11, 1),
    },
}

SHARED_FIXED = {
    "H-May Day":                    (5, 1),
    "H-International Womens Day":   (3, 8),
}


def _easter(year: int) -> datetime.date:
    """Anonymous Gregorian algorithm for Easter Sunday."""
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


def build_special_dates(
    start: datetime.date,
    end: datetime.date,
    frequency: str,
    country_code: str,
) -> pd.DataFrame:
    """
    Build a DataFrame of 0/1 special-date dummies aligned to the date
    grid used by `frequency` ('Daily' or 'Weekly').
    """
    years = range(start.year, end.year + 1)

    # 1. Collect all (column_name -> set of dates) mappings
    holiday_dates: dict[str, set] = {}

    # Shared observances
    for col, rule in OBSERVANCE_RULES.items():
        holiday_dates[col] = set()
        for y in years:
            try:
                d = rule(y, country_code)
                if d is not None:
                    holiday_dates[col].add(d)
            except Exception:
                pass

    # Country fixed holidays
    for col, (m, day) in COUNTRY_FIXED_HOLIDAYS.get(country_code, {}).items():
        holiday_dates[col] = set()
        for y in years:
            try:
                holiday_dates[col].add(datetime.date(y, m, day))
            except Exception:
                pass

    # Shared fixed
    for col, (m, day) in SHARED_FIXED.items():
        holiday_dates[col] = set()
        for y in years:
            try:
                holiday_dates[col].add(datetime.date(y, m, day))
            except Exception:
                pass

    # 2. Build the date grid
    if frequency == "Daily":
        dates = _date_series(start, end)
        df = pd.DataFrame({"DATE_NAME": dates})
        date_col = df["DATE_NAME"].dt.date
    else:  # Weekly
        weeks = _week_series(start, end)
        df = pd.DataFrame({"DATE_NAME": weeks})
        # For weekly: a special week = 1 if any day in Mon-Sun is a holiday
        date_col = None  # handled per column below

    # 3. Fill dummies
    all_cols = sorted(holiday_dates.keys())
    if frequency == "Daily":
        for col in all_cols:
            df[col] = date_col.apply(lambda d: 1 if d in holiday_dates[col] else 0)
    else:
        for col in all_cols:
            hset = holiday_dates[col]

            def week_hit(week_start, hset=hset):
                for delta in range(7):
                    if (week_start + pd.Timedelta(days=delta)).date() in hset:
                        return 1
                return 0

            df[col] = df["DATE_NAME"].apply(week_hit)

    # 4. Filter to [start, end]
    df = df[(df["DATE_NAME"].dt.date >= start) & (df["DATE_NAME"].dt.date <= end)]
    return df.reset_index(drop=True)


# ============================================================
# MERGE helper
# ============================================================

def merge_seasonality(
    weather_df: pd.DataFrame,
    date_col: str,
    seasonality_dfs: list[pd.DataFrame],
) -> pd.DataFrame:
    """Left-join all seasonality DataFrames onto the weather DataFrame."""
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
    table_name   = "weather_daily"
    date_column  = "RECORD_DATE"
    hidden_columns = ["DAILY_WEATHER_ID", "RECORD_DATE"]
else:
    table_name   = "weather_weekly"
    date_column  = "RECORD_WEEK"
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

# ── Seasonality options ────────────────────────────────────────────────
st.subheader("Seasonality Variables")

# Which seasonality types are available depends on frequency
if frequency == "Daily":
    available_seas = ["Daily", "Weekly", "Monthly"]
else:
    available_seas = ["Weekly", "Monthly"]

selected_seas = st.multiselect(
    "Select Seasonality Types to Include",
    options=available_seas,
    default=[],
    help=(
        "Daily = Mon–Sun dummies | "
        "Weekly = ISO-week-in-month position dummies | "
        "Monthly = Jan–Dec dummies"
    ),
)

# Special / holiday dates
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

    # --- Weather query ---
    columns_to_select = hidden_columns + selected_columns
    query = f"""
        SELECT {', '.join(columns_to_select)}
        FROM {st.secrets['snowflake']['database']}.{st.secrets['snowflake']['schema']}.{table_name}
        WHERE COUNTRY_CODE = '{country}'
          AND {date_column} BETWEEN '{range_start}' AND '{range_end}'
        ORDER BY {date_column}
    """
    try:
        with st.spinner("Querying Snowflake…"):
            conn = get_connection()
            df = pd.read_sql(query, conn)
    except Exception as e:
        st.error(f"Error retrieving weather data: {e}")
        st.stop()

    # --- Build seasonality DataFrames ---
    seasonality_frames: list[pd.DataFrame] = []

    with st.spinner("Building seasonality variables…"):
        if "Daily" in selected_seas:
            seasonality_frames.append(
                build_daily_seasonality(range_start, range_end)
            )
        if "Weekly" in selected_seas:
            seasonality_frames.append(
                build_weekly_seasonality(
                    range_start, range_end,
                    expand_to_daily=(frequency == "Daily"),
                )
            )
        if "Monthly" in selected_seas:
            seasonality_frames.append(
                build_monthly_seasonality(range_start, range_end)
            )
        if include_holidays:
            seasonality_frames.append(
                build_special_dates(range_start, range_end, frequency, country)
            )

    # --- Merge ---
    if seasonality_frames:
        df = merge_seasonality(df, date_column, seasonality_frames)

    # --- Rename weather columns to OTHER-Weather_{col} ---
    weather_col_rename = {
        col: f"OTHER-Weather_{col}"
        for col in selected_columns
        if col in df.columns
    }
    df.rename(columns=weather_col_rename, inplace=True)
    # Update selected_columns to reflect renamed weather cols (used by chart)
    renamed_weather_cols = list(weather_col_rename.values())

    st.session_state["df"]                  = df
    st.session_state["date_column"]         = date_column
    st.session_state["selected_columns"]    = renamed_weather_cols

    st.success("Data retrieved successfully!")
    st.dataframe(df)

# ── Show download buttons & chart whenever data exists in session ──────
if "df" in st.session_state:
    df          = st.session_state["df"]
    date_column = st.session_state["date_column"]
    selected_columns = st.session_state.get("selected_columns", [])

    # --- Download format ---
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
            file_name="weather_data.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    # --- Visualization ---
    if not df.empty and date_column in df.columns:
        st.subheader("Data Visualization")
        try:
            df[date_column] = pd.to_datetime(df[date_column])
        except Exception as e:
            st.error(f"Error converting {date_column} to datetime: {e}")

        # Only plot numeric weather columns (skip binary seasonality dummies)
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
