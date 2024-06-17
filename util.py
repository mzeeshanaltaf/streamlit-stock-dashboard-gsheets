import pandas as pd
import streamlit as st
import airbyte as ab
import json
from datetime import datetime
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from itertools import islice


# from itertools.batched, used to produce rows of columns
def batched(iterable, n_cols):
    # batched('ABCDEFG', 3) → ABC DEF G
    if n_cols < 1:
        raise ValueError("n must be at least one")
    it = iter(iterable)
    while batch := tuple(islice(it, n_cols)):
        yield batch


@st.cache_data
def _read_service_account_secret():
    return {'type': st.secrets['type'],
            'project_id': st.secrets['project_id'],
            'private_key_id': st.secrets['private_key_id'],
            'private_key': st.secrets['private_key'],
            'client_email': st.secrets['client_email'],
            'client_id': st.secrets['client_id'],
            'auth_uri': st.secrets['auth_uri'],
            'token_uri': st.secrets['token_uri'],
            'auth_provider_x509_cert_url': st.secrets['auth_provider_x509_cert_url'],
            'client_x509_cert_url': st.secrets['client_x509_cert_url'],
            'universe_domain': st.secrets['universe_domain']}
# def _read_service_account_secret():
#     with open("local-fragment-426110-s6-a99986423348.json") as f:
#         return json.load(f)


@st.cache_resource
def connect_to_gsheets():
    s_acc = _read_service_account_secret()
    gsheets_conn = ab.get_source(
        "source-google-sheets",
        config={
            "spreadsheet_id": "https://docs.google.com/spreadsheets/d/1RUKZ-qaxuULGxGESE9GlBUR2dKVe_s4ZrH837zwJIv4/edit#gid=0",
            "credentials": {
                "auth_type": "Service",
                "service_account_info": json.dumps(s_acc),
            },
        },
    )
    gsheets_conn.select_all_streams()
    return gsheets_conn


@st.cache_data
def download_data(_connection):
    airbyte_streams = _connection.read()
    ticker_df = airbyte_streams["ticker"].to_pandas()

    history_dfs = {}
    for ticker in list(ticker_df["ticker"]):
        d = airbyte_streams[ticker].to_pandas()
        history_dfs[ticker] = d

    return ticker_df, history_dfs


def transform_data(t_df, h_df):
    t_df["last_trade_time"] = pd.to_datetime(
        t_df['last_trade_time'], dayfirst=True,
    )
    for c in ["last_price", "previous_day_price", "change", "change_pct", "volume", "volume_avg", "shares", "day_high",
              "day_low", "market_cap", "p_e_ratio", "eps", ]:
        t_df[c] = pd.to_numeric(t_df[c], "coerce")

    for ticker in list(t_df['ticker']):
        h_df[ticker]['date'] = pd.to_datetime(h_df[ticker]['date'], dayfirst=True)
        for c in ["open", "high", "low", "close", "volume"]:
            h_df[ticker][c] = pd.to_numeric(h_df[ticker][c])

    ticker_to_open = [list(h_df[t]["open"]) for t in list(t_df["ticker"])]
    t_df["open"] = ticker_to_open

    return t_df, h_df


def display_overview(t_df):
    def format_currency(val):
        return "$ {:,.2f}".format(val)

    def format_percentage(val):
        return "{:,.2f} %".format(val)

    def format_change(val):
        return "color: red;" if (val < 0) else "color: green;"

    def apply_odd_row_class(row):
        return ["background-color: #f8f8f8" if row.name % 2 != 0 else "" for _ in row]

    t_df = t_df.drop(['_airbyte_raw_id', '_airbyte_extracted_at', '_airbyte_meta'], axis=1)
    styled_df = (
        t_df.style.format(
            {
                "last_price": format_currency,
                "change_pct": format_percentage,
            }
        ).apply(apply_odd_row_class, axis=1).map(format_change, subset=["change_pct"])
    )

    st.dataframe(
        styled_df,
        column_config={
            "open": st.column_config.AreaChartColumn(
                "Last 12 Months",
                width="large",
                help="Open Price for the last 12 Months",
            ),
        },
        hide_index=True,
        height=250,
    )


@st.experimental_fragment
def display_symbol_history(t_df, h_df):
    left_widget, right_widget, _ = st.columns([1, 1, 1.5])
    selected_ticker = left_widget.selectbox("Currently Showing", list(h_df.keys()))
    selected_period = right_widget.selectbox("Period", ("Week", "Month", "Trimester", "Year"), 2)

    h_df = h_df[selected_ticker]

    h_df = h_df.set_index("date")

    mapping_period = {"Week": 7, "Month": 31, "Trimester": 90, "Year": 365}
    today = datetime.today().date()
    delay_days = mapping_period[selected_period]
    h_df = h_df[(today - pd.Timedelta(delay_days, unit="d")):today]

    f_candle = plot_candlestick(h_df)
    left_chart, right_indicator = st.columns([1.5, 1])
    with left_chart:
        st.html('<span class="column_plotly"></span>')
        st.plotly_chart(f_candle, use_container_width=True)
    with right_indicator:
        st.html('<span class="column_indicator"></span>')
        st.subheader("Period Metrics")
        l, r = st.columns(2)
        with l:
            st.html('<span class="low_indicator"></span>')
            st.metric("Lowest Volume Day Trade", f'{h_df["volume"].min():,}')
            st.metric('Lowest Close Price', f'{h_df["close"].min():,}$')
        with r:
            st.html('<span class="high_indicator"></span>')
            st.metric("Highest Volume Day Trade", f'{h_df["volume"].max():,}')
            st.metric('Highest Close Price', f'${h_df["close"].max():,}')
        with st.container():
            st.html('<span class="bottom_indicator"></span>')
            st.metric("Average Daily Volume", f'{int(h_df["volume"].mean()):,}')
            st.metric(
                "Current Market Cap",
                "${:,}".format(
                    t_df[t_df["ticker"] == selected_ticker][
                        "market_cap"
                    ].values[0]
                ),
            )


def plot_candlestick(history_df):
    f_candle = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        row_heights=[0.7, 0.3],
        vertical_spacing=0.1,
    )

    f_candle.add_trace(
        go.Candlestick(
            x=history_df.index,
            open=history_df["open"],
            high=history_df["high"],
            low=history_df["low"],
            close=history_df["close"],
            name="Dollars",
        ),
        row=1,
        col=1,
    )
    f_candle.add_trace(
        go.Bar(x=history_df.index, y=history_df["volume"], name="Volume Traded"),
        row=2,
        col=1,
    )
    f_candle.update_layout(
        title="Stock Price Trends",
        showlegend=True,
        xaxis_rangeslider_visible=False,
        yaxis1=dict(title="OHLC"),
        yaxis2=dict(title="Volume"),
        hovermode="x",
    )
    f_candle.update_layout(
        title_font_family="Open Sans",
        title_font_color="#174C4F",
        title_font_size=32,
        font_size=16,
        margin=dict(l=80, r=80, t=100, b=80, pad=0),
        height=500,
    )
    f_candle.update_xaxes(title_text="Date", row=2, col=1)
    f_candle.update_traces(selector=dict(name="Dollars"), showlegend=True)
    return f_candle


def display_watchlist_card(ticker, symbol_name, last_price, change_pct, open):
    with st.container(border=True):
        st.html(f"<span class='watchlist_card'></span>")
        tl, tr = st.columns([2, 1])
        bl, br = st.columns([1, 1])

        with tl:
            st.html(f'<span class="watchlist_symbol_name"></span>')
            st.markdown(f"{symbol_name}")
        with tr:
            st.html(f'<span class="watchlist_ticker"></span>')
            st.markdown(f"{ticker}")
            negative_gradient = float(change_pct) < 0
            st.markdown(
                f":{'red' if negative_gradient else 'green'}[{'▼' if negative_gradient else '▲'} {change_pct:.2f} %]"
            )

        with bl:
            with st.container():
                st.html(f'<span class="watchlist_price_label"></span>')
                st.markdown(f"Current Value")

            with st.container():
                st.html(f'<span class="watchlist_price_value"></span>')
                st.markdown(f"$ {last_price:.2f}")

        with br:
            st.html(f'<span class="watchlist_br"></span>')
            fig_spark = plot_sparkline(open)
            st.plotly_chart(
                fig_spark, config=dict(displayModeBar=False), use_container_width=True
            )


def display_watchlist(t_df):
    n_cols = 4
    for row in batched(t_df.itertuples(), n_cols):
        cols = st.columns(n_cols)
        for col, ticker in zip(cols, row):
            if ticker:
                with col:
                    display_watchlist_card(
                        ticker.ticker,
                        ticker.symbol_name,
                        ticker.last_price,
                        ticker.change_pct,
                        ticker.open,
                    )


def plot_sparkline(data):
    fig_spark = go.Figure(
        data=go.Scatter(
            y=data,
            mode="lines",
            fill="tozeroy",
            line_color="red",
            fillcolor="pink",
        ),
    )
    fig_spark.update_traces(hovertemplate="Price: $ %{y:.2f}")
    fig_spark.update_xaxes(visible=False, fixedrange=True)
    fig_spark.update_yaxes(visible=False, fixedrange=True)
    fig_spark.update_layout(
        showlegend=False,
        plot_bgcolor="white",
        height=50,
        margin=dict(t=10, l=0, b=0, r=0, pad=0),
    )
    return fig_spark
