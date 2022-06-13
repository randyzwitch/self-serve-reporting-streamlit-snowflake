import snowflake.connector
import streamlit as st
import pandas as pd

# connect to snowflake
conn = snowflake.connector.connect(**st.secrets["sfdevrel"])


def sales_report(dt, conn):
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
            l_shipdate <= dateadd(day, -90, to_date('{dt}'))
        group by
            l_returnflag,
            l_linestatus
        order by
            l_returnflag,
            l_linestatus
        """,
        conn,
    )


# run report, write to CSV
report = sales_report("1998-12-01", conn)
report.to_csv("sales_report.csv", index=False)
