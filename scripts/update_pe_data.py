import pandas as pd
import yfinance as yf
import concurrent.futures
import os
from datetime import datetime

def fetch_stock_info(symbol, company_name):
    try:
        ticker = yf.Ticker(f"{symbol}.NS")
        info = ticker.info
        pe = info.get("trailingPE")
        if pe is None or pe <= 0:
            return None
        mcap = info.get("marketCap", 0)
        if mcap < 100_000_000:  # ₹100 Cr min
            return None
        return {
            "Symbol": symbol,
            "Company": company_name,
            "PE": round(pe, 2),
            "Price (₹)": round(info.get("currentPrice", 0), 2),
            "Market Cap (₹ Cr)": round(mcap / 10_000_000, 2),
            "Sector": info.get("sector", "N/A"),
            "Last Updated": datetime.now().strftime("%d %b %Y %H:%M IST")
        }
    except:
        return None

def update_pe_data():
    url = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"
    df_nse = pd.read_csv(url)
    eq_stocks = df_nse[df_nse["SERIES"] == "EQ"].reset_index(drop=True)

    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:  # Faster!
        futures = [executor.submit(fetch_stock_info, row["SYMBOL"], row["NAME OF COMPANY"]) 
                   for _, row in eq_stocks.iterrows()]
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result:
                results.append(result)

    if results:
        df = pd.DataFrame(results).sort_values("PE").reset_index(drop=True)
        os.makedirs("data", exist_ok=True)
        df.to_csv("data/pe_data.csv", index=False)
        return df
    return None

# For GitHub Actions / manual run
if __name__ == "__main__":
    update_pe_data()
