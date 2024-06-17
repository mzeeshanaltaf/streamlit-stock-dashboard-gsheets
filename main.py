from util import *
import streamlit as st

st.set_page_config(page_title="Stocks Dashboard", page_icon="💹", layout="wide")
st.html('styles.html')
st.html('<h1 class="title">Stocks Dashboard</h1>')
_gsheets_connection = connect_to_gsheets()
t_df, h_df = download_data(_gsheets_connection)
t_df, h_df = transform_data(t_df, h_df)
display_watchlist(t_df)
st.divider()
display_symbol_history(t_df, h_df)
display_overview(t_df)

