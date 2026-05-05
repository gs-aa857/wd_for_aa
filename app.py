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

def _iso_month_week1_start(thursday: pd.Timestamp) -> pd.Timestamp:
    """Monday of the first ISO week of thursday's month (uses day-4 anchor)."""
    first_thu = pd.Timestamp(thursday.year, thursday.month, 4)
    return first_thu - pd.tseries.offsets.Week(weekday=0)


def build_weekly_seasonality(
    start: datetime.date, end: datetime.date, expand_to_daily: bool = False
) -> pd.DataFrame:
    """
    Build weekly seasonality dummies.

    When expand_to_daily=True (Daily frequency selected), each week's values
    are broadcast to all 7 days (Mon-Sun) so the result aligns to a daily grid.
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


# ============================================================
# HOLIDAY / SPECIAL DATE GENERATORS
# ============================================================

def _nth_weekday(year: int, month: int, weekday: int, n: int) -> datetime.date:
    """Return the n-th occurrence (1-based) of weekday (0=Mon) in month/year."""
    first = datetime.date(year, month, 1)
    delta = (weekday - first.weekday()) % 7
    first_occurrence = first + datetime.timedelta(days=delta)
    return first_occurrence + datetime.timedelta(weeks=n - 1)


def _last_weekday(year: int, month: int, weekday: int) -> datetime.date:
    """Return the last occurrence of weekday in month/year."""
    if month == 12:
        last = datetime.date(year + 1, 1, 1) - datetime.timedelta(days=1)
    else:
        last = datetime.date(year, month + 1, 1) - datetime.timedelta(days=1)
    delta = (last.weekday() - weekday) % 7
    return last - datetime.timedelta(days=delta)


def _dst_start(year: int) -> datetime.date:
    """EU DST start: last Sunday of March."""
    return _last_weekday(year, 3, 6)


def _dst_end(year: int) -> datetime.date:
    """EU DST end: last Sunday of October."""
    return _last_weekday(year, 10, 6)


def _black_weekend_dates(year: int) -> list:
    """Black Friday + Saturday + Sunday (the 'Black & Cyber Weekend' window)."""
    black_friday = _nth_weekday(year, 11, 4, 4)  # 4th Friday of November
    return [black_friday + datetime.timedelta(days=i) for i in range(3)]


def _e_esmaspaev_dates(year: int) -> list:
    """E-esmaspäev (EE only) - official campaign dates, 4x per year.

    History (sourced from Estonian E-Commerce Association announcements):
      2014: Nov 10          (inaugural; 2nd Mon of Nov)
      2015-2022: 2nd Mon of May + 2nd Mon of Nov  (twice/year)
      2023+: 2nd Mon of March, May, September, November  (four/year)

    Hard-coded known dates are used for 2014-2025 where confirmed.
    For 2026+ the rule "2nd Monday of Mar/May/Sep/Nov" is applied algorithmically.
    """
    # Hard-coded confirmed dates (verified against official EKL announcements)
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
    # 2026+: 2nd Monday of March, May, September, November
    return [_nth_weekday(year, m, 0, 2) for m in (3, 5, 9, 11)]


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


# ---------------------------------------------------------------------------
# OBSERVANCE_RULES
#   key   = output column name
#   value = lambda(year, country_code) -> datetime.date | list[datetime.date] | None
#   Return None to skip for a given country/year.
#   Return a list to mark multiple dates under one column name (e.g. weekends).
# ---------------------------------------------------------------------------
OBSERVANCE_RULES = {
    # Mothers Day: 2nd Sunday of May
    "H-Mothers Day":             lambda y, cc: _nth_weekday(y, 5, 6, 2),
    # Fathers Day: 2nd Sunday of November
    "H-Fathers Day":             lambda y, cc: _nth_weekday(y, 11, 6, 2),
    # Valentines Day: Feb 14
    "H-Valentines Day":          lambda y, cc: datetime.date(y, 2, 14),
    # Easter Sunday
    "H-Easter Sunday":           lambda y, cc: _easter(y),
    # Easter Monday
    "H-Easter Monday":           lambda y, cc: _easter(y) + datetime.timedelta(days=1),
    # Good Friday
    "H-Good Friday":             lambda y, cc: _easter(y) - datetime.timedelta(days=2),
    # Pentecost / Nelipühade 1. püha: 49 days after Easter (all countries)
    "H-Pentecost":               lambda y, cc: _easter(y) + datetime.timedelta(days=49),
    # Midsummer Eve: Jun 23 (EE/LV/LT) | 3rd Friday of Jun, covering 19-25 (FI)
    "H-Midsummer Eve":           lambda y, cc: (
        _nth_weekday(y, 6, 4, 3) if cc == "FI" else datetime.date(y, 6, 23)
    ),
    # Halloween: Oct 31
    "H-Halloween":               lambda y, cc: datetime.date(y, 10, 31),
    # Christmas Eve: Dec 24
    "H-Christmas Eve":           lambda y, cc: datetime.date(y, 12, 24),
    # Christmas Day: Dec 25
    "H-Christmas Day":           lambda y, cc: datetime.date(y, 12, 25),
    # Boxing Day / 2nd Christmas: Dec 26
    "H-Boxing Day":              lambda y, cc: datetime.date(y, 12, 26),
    # New Years Eve: Dec 31
    "H-New Years Eve":           lambda y, cc: datetime.date(y, 12, 31),
    # New Years Day: Jan 1
    "H-New Years Day":           lambda y, cc: datetime.date(y, 1, 1),
    # Black Friday: 4th Friday of November
    "H-Black Friday":            lambda y, cc: _nth_weekday(y, 11, 4, 4),
    # Black and Cyber Weekend: Black Friday + Sat + Sun (list of 3 dates)
    "H-Black and Cyber Weekend": lambda y, cc: _black_weekend_dates(y),
    # Cyber Monday: Monday after Black Friday (3 days after)
    "H-Cyber Monday":            lambda y, cc: _nth_weekday(y, 11, 4, 4) + datetime.timedelta(days=3),
    # DST Start (EU): last Sunday of March
    "H-DST Start":               lambda y, cc: _dst_start(y),
    # DST End (EU): last Sunday of October
    "H-DST End":                 lambda y, cc: _dst_end(y),
    # E-esmaspaev: EE only, 2-4 dates/year (hard-coded confirmed dates; 2nd Mon of Mar/May/Sep/Nov from 2023+)
    "H-E-esmaspaev":             lambda y, cc: _e_esmaspaev_dates(y) if cc == "EE" else None,
}

# Country-specific public holidays with fixed calendar dates
COUNTRY_FIXED_HOLIDAYS = {
    "EE": {
        "H-Independence Day":          (2, 24),
        "H-Victory Day":               (6, 23),
        "H-Restoration Day":           (8, 20),
        "H-Christmas Day (2nd)":       (12, 26),
        # School year starts Sep 1 in EE
        "H-Beginning of School Year":  (9, 1),
    },
    "LV": {
        "H-Independence Day":          (11, 18),
        "H-Restoration Day":           (5, 4),
        "H-Lacplesis Day":             (11, 11),
        # School year starts Sep 1 in LV
        "H-Beginning of School Year":  (9, 1),
    },
    "LT": {
        "H-Independence Day":          (2, 16),
        "H-Restoration Day":           (3, 11),
        "H-Statehood Day":             (7, 6),
        "H-All Saints Day":            (11, 1),
        # School year starts Sep 1 in LT
        "H-Beginning of School Year":  (9, 1),
    },
    "FI": {
        "H-Independence Day":          (12, 6),
        "H-Epiphany":                  (1, 6),
        "H-All Saints Day":            (11, 1),
        # FI school year start not included: no single fixed national date
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
    """
    Build a DataFrame of 0/1 special-date dummies aligned to the date
    grid used by `frequency` ('Daily' or 'Weekly').
    Rules returning a list mark all dates in that list as 1.
    Rules returning None are skipped for that country.
    """
    years = range(start.year, end.year + 1)

    # Collect column_name -> set of relevant dates
    holiday_dates: dict = {}

    # Observance rules (may return single date, list, or None)
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

    # Country-specific fixed holidays
    for col, (m, day) in COUNTRY_FIXED_HOLIDAYS.get(country_code, {}).items():
        holiday_dates[col] = set()
        for y in years:
            try:
                holiday_dates[col].add(datetime.date(y, m, day))
            except Exception:
                pass

    # Shared fixed holidays
    for col, (m, day) in SHARED_FIXED.items():
        holiday_dates[col] = set()
        for y in years:
            try:
                holiday_dates[col].add(datetime.date(y, m, day))
            except Exception:
                pass

    # Build the date grid
    if frequency == "Daily":
        dates = _date_series(start, end)
        df = pd.DataFrame({"DATE_NAME": dates})
        date_col = df["DATE_NAME"].dt.date
    else:  # Weekly
        weeks = _week_series(start, end)
        df = pd.DataFrame({"DATE_NAME": weeks})
        date_col = None  # handled per column below

    # Fill dummies
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

    # Filter to [start, end]
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
        with st.spinner("Querying Snowflake..."):
            conn = get_connection()
            df = pd.read_sql(query, conn)
    except Exception as e:
        st.error(f"Error retrieving weather data: {e}")
        st.stop()

    # --- Build seasonality DataFrames ---
    seasonality_frames = []

    with st.spinner("Building seasonality variables..."):
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
    renamed_weather_cols = list(weather_col_rename.values())

    st.session_state["df"]               = df
    st.session_state["date_column"]      = date_column
    st.session_state["selected_columns"] = renamed_weather_cols

    st.success("Data retrieved successfully!")
    st.dataframe(df)

# ── Show download buttons & chart whenever data exists in session ──────
if "df" in st.session_state:
    df               = st.session_state["df"]
    date_column      = st.session_state["date_column"]
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
            file_name="import_data.xlsx",
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
