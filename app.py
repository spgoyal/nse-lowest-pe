import streamlit as st
import pandas as pd
import yfinance as yf
import concurrent.futures
from datetime import datetime

st.set_page_config(page_title="Lowest PE on NSE", page_icon="🏆", layout="wide")
st.title("🏆 NSE Company with Lowest PE Ratio")
st.markdown("**Public data • Updated live • No login required**")

@st.cache_data(ttl=21600)  # Cache 6 hours
def load_nse_list():
    url = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"
    return pd.read_csv(url)

def fetch_stock_info(symbol, company_name):
    try:
        ticker = yf.Ticker(f"{symbol}.NS")
        info = ticker.info
        pe = info.get("trailingPE")
        if pe is None or pe <= 0:
            return None
        mcap = info.get("marketCap", 0)
        if mcap < 100_000_000:  # Skip micro caps (< ₹100 Cr)
            return None
        return {
            "Symbol": symbol,
            "Company": company_name,
            "PE": round(pe, 2),
            "Price (₹)": round(info.get("currentPrice", 0), 2),
            "Market Cap (₹ Cr)": round(mcap / 10_000_000, 2),
            "Sector": info.get("sector", "N/A"),
        }
    except:
        return None

# Button to load/refresh
if st.button("🔄 Load / Refresh All NSE Data", type="primary", use_container_width=True):
    with st.spinner("Fetching 2,200+ companies in parallel... (usually 3–8 minutes first time)"):
        df_nse = load_nse_list()
        eq_stocks = df_nse[df_nse["SERIES"] == "EQ"].reset_index(drop=True)

        results = []
        progress_bar = st.progress(0)
        status = st.empty()

        with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
            future_to_stock = {
                executor.submit(fetch_stock_info, row["SYMBOL"], row["NAME OF COMPANY"]): i
                for i, row in eq_stocks.iterrows()
            }
            for i, future in enumerate(concurrent.futures.as_completed(future_to_stock)):
                result = future.result()
                if result:
                    results.append(result)
                progress_bar.progress((i + 1) / len(eq_stocks))
                status.text(f"✅ Processed {i+1}/{len(eq_stocks)} companies")

        if results:
            df = pd.DataFrame(results)
            df = df.sort_values("PE").reset_index(drop=True)
            st.session_state["pe_data"] = df
            st.success(f"✅ Done! Showing {len(df)} companies with valid positive PE")
        else:
            st.error("No data received. Try again in a few minutes.")

# Show results if available
if "pe_data" in st.session_state:
    df = st.session_state["pe_data"]
    
    col1, col2 = st.columns([2, 1])
    with col1:
        winner = df.iloc[0]
        st.subheader("🏆 LOWEST PE COMPANY")
        st.metric(
            label=f"{winner['Company']} ({winner['Symbol']})",
            value=f"PE = {winner['PE']}",
            delta=f"₹{winner['Price (₹)']}"
        )
        st.caption(f"Sector: {winner['Sector']} | Market Cap: ₹{winner['Market Cap (₹ Cr)']:,} Cr")

    with col2:
        st.subheader("Filters")
        sector_filter = st.multiselect("Sector", options=sorted(df["Sector"].unique()), default=[])
        min_mcap = st.slider("Minimum Market Cap (₹ Cr)", 100, 500000, 500)

    filtered = df.copy()
    if sector_filter:
        filtered = filtered[filtered["Sector"].isin(sector_filter)]
    filtered = filtered[filtered["Market Cap (₹ Cr)"] >= min_mcap]

    st.subheader("All Stocks Sorted by PE (lowest first)")
    st.dataframe(
        filtered.style.format({
            "PE": "{:.2f}",
            "Price (₹)": "{:.2f}",
            "Market Cap (₹ Cr)": "{:,.0f}"
        }),
        use_container_width=True,
        hide_index=True
    )

    st.download_button(
        label="📥 Download full table as CSV",
        data=filtered.to_csv(index=False).encode("utf-8"),
        file_name=f"nse_lowest_pe_{datetime.now().strftime('%Y%m%d')}.csv",
        mime="text/csv"
    )

else:
    st.info("👆 Click the big blue button above to load the data for the first time.")

st.caption("⚠️ Not financial advice • Data from public NSE + Yahoo Finance • PE = Trailing Twelve Months")
