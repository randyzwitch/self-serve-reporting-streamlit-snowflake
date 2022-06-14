import snowflake.connector
import streamlit as st
import pandas as pd

# connect to snowflake
@st.experimental_singleton
def connect(**secrets):
    return snowflake.connector.connect(**secrets)


@st.experimental_memo
def sales_report(dt, _conn, days_prior=90):
    """Calculates sales report for 90 days prior to a given date"""

    return pd.read_sql(
        f"""
        select
            l_returnflag,
            l_linestatus,
            sum(l_quantity) as sum_qty,
            sum(l_extendedprice) as sum_base_price,
            sum(l_extendedprice * (1-l_discount)) as sum_disc_price,
            sum(l_extendedprice * (1-l_discount) * (1+l_tax)) as sum_charge,
            avg(l_quantity) as avg_qty,
            avg(l_extendedprice) as avg_price,
            avg(l_discount) as avg_disc,
            count(*) as count_order
        from
            SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.lineitem
        where
            l_shipdate between dateadd(day, -{days_prior}, to_date('{dt}')) and '{dt}'
        group by
            l_returnflag,
            l_linestatus
        order by
            l_returnflag,
            l_linestatus
        """,
        _conn,
    )


# run report, write to CSV
# valid dates: '1992-01-02' - '1998-12-01'

#### Streamlit app
conn = connect(**st.secrets["sfdevrel"])

dt = st.sidebar.date_input(
    "Input report end date",
    value=pd.to_datetime("1992-12-01"),
    min_value=pd.to_datetime("1992-01-02"),
    max_value=pd.to_datetime("1998-12-01"),
)

days = st.sidebar.slider(
    "Enter days in report period",
    value=90,
    min_value=7,
    max_value=365,
    help="Add number of days to calculate report",
)

"# Sales Report - Widget Factory"
f"{days} days prior to {dt}"

report = sales_report(dt, conn, days)


report
