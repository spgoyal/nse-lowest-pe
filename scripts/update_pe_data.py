import pandas as pd
import yfinance as yf
import concurrent.futures
import os
from datetime import datetime
import time
import random

def fetch_stock_info(symbol, company_name, retries=2):
    for attempt in range(retries + 1):
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
        except Exception as e:
            err_str = str(e).lower()
            if "rate limit" in err_str or "too many requests" in err_str or "429" in err_str:
                wait_time = (2 ** attempt) * 30 + random.uniform(10, 30)  # 30s → 90s → etc.
                print(f"Rate limit hit for {symbol} (attempt {attempt+1}). Sleeping {wait_time:.0f}s...")
                time.sleep(wait_time)
                if attempt == retries:
                    return None
            else:
                return None  # Other errors → skip
    return None

def update_pe_data(limit_symbols=None):  # Optional: limit for testing
    url = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"
    df_nse = pd.read_csv(url)
    eq_stocks = df_nse[df_nse["SERIES"] == "EQ"].reset_index(drop=True)
    
    if limit_symbols is not None:
        eq_stocks = eq_stocks.head(limit_symbols)  # e.g. 300 for testing

    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:  # LOW concurrency!
        futures = [
            executor.submit(fetch_stock_info, row["SYMBOL"], row["NAME OF COMPANY"])
            for _, row in eq_stocks.iterrows()
        ]
        
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result:
                results.append(result)
            # Sleep AFTER each completed future (spreads out over time)
            time.sleep(random.uniform(3.0, 8.0))  # Core delay: 3-8 seconds per ticker

    if results:
        df = pd.DataFrame(results).sort_values("PE").reset_index(drop=True)
        os.makedirs("data", exist_ok=True)
        df.to_csv("data/pe_data.csv", index=False)
        print(f"Saved {len(df)} valid stocks to data/pe_data.csv")
        return df
    print("No valid data fetched.")
    return None

# For manual / Actions run
if __name__ == "__main__":
    update_pe_data()  # Or pass limit_symbols=300 for testing
