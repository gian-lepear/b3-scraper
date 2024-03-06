import altair as alt
import pandas as pd
import streamlit as st

from b3_scraper.viz.utils import (
    calculate_financial_metrics,
    generate_statistics,
    load_data,
)


def setup_page():
    st.set_page_config(page_title="Brazil Stock Data", layout="wide", page_icon="📈")


def create_chart(df: pd.DataFrame, y_field: str, y_title: str) -> alt.Chart:
    chart = (
        alt.Chart(df)
        .mark_line()
        .encode(
            x="DATA",
            y=alt.Y(
                y_field,
                scale=alt.Scale(domain=[df[y_field].min() - 10, df[y_field].max() + 10], nice=10),
                axis=alt.Axis(title=y_title),
            ),
        )
    )
    return chart


def display_stock_metrics(df_metrics):
    """Função auxiliar para exibir métricas da ação selecionada."""
    st.metric("Company Name", df_metrics.iloc[0]["NOMRES"].title())

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric(
            "Last quote",
            f"R$ {df_metrics.iloc[-1]['PREULT']:.2f}",
            delta=f"{(df_metrics.iloc[-1]['DAILY_RETURN'] * 100).round(2)} %",
        )
    with col2:
        st.metric(
            "Last min.",
            f"R$ {df_metrics.iloc[-1]['PREMIN']:.2f}",
        )
    with col3:
        st.metric(
            "Last max",
            f"R$ {df_metrics.iloc[-1]['PREMAX']:.2f}",
        )
    with col4:
        st.metric("Trade volume", f"{df_metrics.iloc[-1]['VOLTOT'].astype(int):,}")


def main():
    setup_page()

    df = load_data()

    st.header("📈 Brazil Stock Data")
    stock_selected = st.selectbox(
        "Pick a Stock",
        options=df["CODNEG"].unique(),
        index=0,
    )

    df_filtered = df.loc[df["CODNEG"] == stock_selected].sort_values(by="DATA", ascending=True)
    df_metrics = calculate_financial_metrics(df_filtered)

    display_stock_metrics(df_metrics)

    st.markdown("### Daily Returns")
    st.altair_chart(
        create_chart(df_metrics, "CUMULATIVE_PROFITABILITY", "Rentabilidade"),
        use_container_width=True,
    )

    st.markdown("### Statistics")
    stats_df = pd.DataFrame(generate_statistics(df_metrics)).T
    st.table(stats_df)

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### Volatility")
        st.altair_chart(
            create_chart(df_metrics.dropna(), "VOLATILITY", "Volatilidade"),
            use_container_width=True,
        )

    with col2:
        st.markdown("### Last 7 days")
        st.table(
            df_metrics[["DATA", "PREULT", "VOLTOT", "DAILY_RETURN"]]
            .rename(
                {
                    "DATA": "Date",
                    "PREULT": "Close Price",
                    "VOLTOT": "Trade Volume",
                    "DAILY_RETURN": "Daily Returns",
                },
                axis=1,
            )
            .set_index("Date")
            .dropna()
            .sort_index(ascending=False)
            .head(7)
        )


if __name__ == "__main__":
    main()
