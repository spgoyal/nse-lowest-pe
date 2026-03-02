import streamlit as st
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import time

st.set_page_config(page_title="Lowest PE on NSE", layout="wide")
st.title("🏆 NSE Company with Lowest PE Ratio")
st.markdown("**Updated daily from public data** (NSE master list + Yahoo Finance)")

@st.cache_data(ttl=86400)  # Cache 24 hours
def get_lowest_pe_data():
    # 1. Get all NSE equity symbols (official list)
    url = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"
    df = pd.read_csv(url)
    eq_stocks = df[df['SERIES'] == 'EQ'].reset_index(drop=True)
    
    data = []
    progress_bar = st.progress(0)
    status = st.empty()
    
    for i, row in eq_stocks.iterrows():
        symbol = row['SYMBOL']
        status.text(f"Fetching {i+1}/{len(eq_stocks)}: {symbol}")
        try:
            ticker = yf.Ticker(f"{symbol}.NS")
            info = ticker.info
            pe = info.get('trailingPE')
            price = info.get('currentPrice')
            mcap = info.get('marketCap')
            sector = info.get('sector')
            
            if pe is not None and pe > 0 and mcap and mcap > 50_000_000:  # > ₹50 Cr market cap
                data.append({
                    'Symbol': symbol,
                    'Company': row['NAME OF COMPANY'],
                    'PE': round(pe, 2),
                    'Price': round(price, 2) if price else None,
                    'Market Cap (₹ Cr)': round(mcap / 10_000_000, 2) if mcap else None,
                    'Sector': sector or 'N/A',
                    'Last Updated': datetime.now().strftime("%d %b %H:%M")
                })
        except:
            pass  # skip errors
        
        progress_bar.progress((i+1)/len(eq_stocks))
        time.sleep(1.5)  # polite rate limit
    
    result_df = pd.DataFrame(data)
    result_df = result_df.sort_values('PE').reset_index(drop=True)
    
    # Save latest min PE company
    min_pe_company = result_df.iloc[0] if not result_df.empty else None
    return result_df, min_pe_company

df, min_company = get_lowest_pe_data()

# === DISPLAY ===
col1, col2 = st.columns([1, 3])
with col1:
    if min_company is not None:
        st.success("**LOWEST PE COMPANY**")
        st.metric(label=min_company['Company'], 
                  value=f"PE = {min_company['PE']}", 
                  delta=min_company['Symbol'])
        st.caption(f"₹{min_company['Price']} | {min_company['Sector']}")
    else:
        st.error("No data yet")

with col2:
    st.subheader("All NSE Stocks Sorted by PE (positive only)")
    # Filters
    sector_filter = st.multiselect("Filter Sector", options=sorted(df['Sector'].unique()), default=[])
    min_mcap = st.slider("Min Market Cap (₹ Cr)", 0, 500000, 100)
    
    filtered = df.copy()
    if sector_filter:
        filtered = filtered[filtered['Sector'].isin(sector_filter)]
    filtered = filtered[filtered['Market Cap (₹ Cr)'] >= min_mcap]
    
    st.dataframe(
        filtered.style.format({"PE": "{:.2f}", "Price": "{:.2f}", "Market Cap (₹ Cr)": "{:,.0f}"}),
        use_container_width=True,
        hide_index=True
    )

st.caption("Data source: Official NSE EQUITY_L.csv + Yahoo Finance (public). Not financial advice. PE can change intraday.")
