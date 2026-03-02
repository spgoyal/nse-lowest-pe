import streamlit as st
import pandas as pd
from datetime import datetime
import os

st.set_page_config(page_title="Lowest PE on NSE", page_icon="🏆", layout="wide")
st.title("🏆 NSE Company with Lowest PE Ratio")
st.markdown("**Public data • Instant load • Auto-updated daily at 8 PM IST**")

DATA_PATH = "data/pe_data.csv"

@st.cache_data(ttl=3600)  # Refresh every hour if file changes
def load_data():
    if os.path.exists(DATA_PATH):
        df = pd.read_csv(DATA_PATH)
        last_updated = df['Last Updated'].iloc[0] if not df.empty else "Never"
        return df, last_updated
    return None, "Never"

df, last_updated = load_data()

st.caption(f"📅 Last updated: **{last_updated}**")

col1, col2 = st.columns([3, 1])
with col1:
    if st.button("🔄 Manual Full Refresh (3-8 min)", type="primary", use_container_width=True):
        with st.spinner("Fetching latest data from Yahoo Finance (parallel)..."):
            # Import inside button so normal loads stay instant
            from scripts.update_pe_data import update_pe_data
            df = update_pe_data()
            if df is not None:
                st.success("✅ Updated! Refreshing page...")
                st.rerun()
            else:
                st.error("Failed to fetch. Try again later.")

with col2:
    if st.button("🔃 Soft Refresh Page"):
        st.rerun()

if df is not None and not df.empty:
    winner = df.iloc[0]
    colA, colB = st.columns([2, 1])
    with colA:
        st.subheader("🏆 LOWEST PE COMPANY")
        st.metric(
            label=f"{winner['Company']} ({winner['Symbol']})",
            value=f"PE = {winner['PE']}",
            delta=f"₹{winner['Price (₹)']}"
        )
        st.caption(f"Sector: {winner['Sector']} | MCap: ₹{winner['Market Cap (₹ Cr)']:,} Cr")

    with colB:
        st.subheader("Filters")
        sector_filter = st.multiselect("Sector", options=sorted(df["Sector"].unique()), default=[])
        min_mcap = st.slider("Min Market Cap (₹ Cr)", 100, 500000, 500)

    filtered = df.copy()
    if sector_filter:
        filtered = filtered[filtered["Sector"].isin(sector_filter)]
    filtered = filtered[filtered["Market Cap (₹ Cr)"] >= min_mcap]

    st.subheader(f"All Stocks ({len(filtered)} shown)")
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
        "📥 Download CSV",
        data=filtered.to_csv(index=False).encode(),
        file_name=f"nse_lowest_pe_{datetime.now().strftime('%Y%m%d')}.csv",
        mime="text/csv"
    )
else:
    st.info("👆 Click **Manual Full Refresh** to fetch data for the first time (or wait for tonight's auto-update).")

st.caption("⚠️ Not financial advice • Data: NSE + Yahoo Finance (public) • PE = Trailing")
