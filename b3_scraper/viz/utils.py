from datetime import datetime

import numpy as np
import pandas as pd
import streamlit as st


@st.cache_data
def load_data() -> pd.DataFrame:
    connection_string = "postgresql://postgres:postgres@localhost:5432/b3_scraper"
    df = pd.read_sql_table(table_name="stock_data", con=connection_string)
    df = df[["DATA", "CODNEG", "PREULT", "NOMRES", "PREMAX", "PREMIN", "VOLTOT"]]

    for col in ["PREULT", "PREMAX", "PREMIN", "VOLTOT"]:
        df[col] = df[col] / 100
    df["VOLTOT"] = df["VOLTOT"].astype(int)
    df["DATA"] = pd.to_datetime(df["DATA"], format="%Y%m%d")
    return df


def calculate_financial_metrics(df: pd.DataFrame) -> pd.DataFrame:
    df["DAILY_RETURN"] = df["PREULT"].pct_change()
    df["CUMULATIVE_PROFITABILITY"] = (((1 + df["DAILY_RETURN"]).cumprod() - 1) * 100).round(2)
    df["VOLATILITY"] = (
        np.log(df["PREULT"] / df["PREULT"].shift(1)).dropna().rolling(window=21).std()
        * np.sqrt(252)
        * 100
    )
    return df


def return_period_df(df, months=0):
    if not months:
        return df

    last_quote_date = df["DATA"].iloc[-1]
    initial_date = df["DATA"].iloc[-1] - pd.DateOffset(months=months)

    df_filtered = df[(df["DATA"] >= initial_date) & (df["DATA"] <= last_quote_date)]
    return df_filtered


def calc_prof(df):
    return (df["PREULT"].iloc[-1] / df["PREULT"].iloc[0] - 1) * 100


def calc_vol(df):
    return df["PREULT"].pct_change().std() * np.sqrt(252) * 100


@st.cache_data
def generate_statistics(df):
    stats = {
        "Volatility": {},
        "Profitability": {},
    }

    for months in [1, 12, 24, 36]:
        period_df = return_period_df(df, months)
        stats["Volatility"][f"{months} Month(s)"] = calc_vol(period_df)
        stats["Profitability"][f"{months} Month(s)"] = calc_prof(period_df)

    # Anual
    current_year_df = df[df["DATA"].dt.year == datetime.now().year]
    stats["Volatility"]["Annual"] = calc_vol(current_year_df)
    stats["Profitability"]["Annual"] = calc_prof(current_year_df)
    return stats
