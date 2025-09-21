import pandas as pd
import requests
import bs4
import boto3
import json
from datetime import datetime
from bs4 import BeautifulSoup

s3 = boto3.client("s3")
BUCKET = "fin-sum"  # <-- replace with your bucket

def get_daily_data(ticker):
    url = f"https://finance.yahoo.com/quote/{ticker}"
    headers = {"User-Agent": "Mozilla/5.0"}  # <-- required, Yahoo blocks bots
    r = requests.get(url, headers=headers)
    soup = BeautifulSoup(r.text, "html.parser") 

    price = soup.find("div", {"class":"container yf-16vvaki"}).find("span", {"data-testid":"qsp-price"}).text
    
    daily_summary_table = soup.find_all("span", {"class": "value yf-1qull9i"})

    text_list = soup.find_all("div", {"class": "stream-item yf-186c5b2"})

    column_text = ["article title","link"]

    df_text = pd.DataFrame(columns = column_text)
    for i in text_list:
        df_text.loc[len(df_text)] = {"article title": i.text, "link": i.find("a")["href"]}

    prev_close = daily_summary_table[0].text

    open = daily_summary_table[1].text

    bid = daily_summary_table[2].text

    ask = daily_summary_table[3].text

    day_range = daily_summary_table[4].text

    week_range_52 = daily_summary_table[5].text

    volume = daily_summary_table[6].text

    avg_volume = daily_summary_table[7].text

    market_cap = daily_summary_table[8].text

    beta = daily_summary_table[9].text

    pe_ratio = daily_summary_table[10].text

    eps = daily_summary_table[11].text

    dividend_yield = daily_summary_table[13].text

    target_est = daily_summary_table[15].text

    columns = ["company", "price", "prev_close", "open", "bid", "ask", "day_range","week_range_52","volume","avg_volume","market_cap","beta","pe_ratio","eps","forward_dividend","1y_target_est"]   

    df = pd.DataFrame(columns = columns)

    df.loc[0] = {"company": ticker, "price": price, "prev_close": prev_close, "open": open, "bid": bid, "ask": ask,"day_range": day_range,"week_range_52":week_range_52,"volume":volume,"avg_volume":avg_volume, "market_cap":market_cap,"beta": beta, "pe_ratio":pe_ratio, "eps":eps, "forward_dividend": dividend_yield,"1y_target_est":target_est}
     
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=1mo&interval=1d"
    headers = {"User-Agent": "Mozilla/5.0"}
    resp = requests.get(url, headers=headers).json()

    # Extract timestamps and OHLCV
    timestamps = resp["chart"]["result"][0]["timestamp"]
    indicators = resp["chart"]["result"][0]["indicators"]["quote"][0]
    adjclose = resp["chart"]["result"][0]["indicators"]["adjclose"][0]["adjclose"]

    # Convert to DataFrame
    df_stat = pd.DataFrame(indicators)
    df_stat["Adj Close"] = adjclose
    df_stat["Date"] = pd.to_datetime(timestamps, unit="s", utc=True).tz_convert("US/Eastern").date

    # Reorder & rename columns to match Yahoo
    df_stat = df_stat[["Date", "open", "high", "low", "close", "Adj Close", "volume"]]
    df_stat.columns = ["Date", "Open", "High", "Low", "Close", "Adj Close", "Volume"]

# Format like Yahoo
    df_stat["Date"] = df_stat["Date"].apply(lambda x: x.strftime("%b %d, %Y"))
    for col in ["Open", "High", "Low", "Close", "Adj Close"]:
        df_stat[col] = df_stat[col].map(lambda x: f"{x:.2f}")
    df_stat["Volume"] = df_stat["Volume"].map(lambda x: f"{int(x):,}")

# 🔄 Reverse the order so the latest date is on top
    df_stat = df_stat.iloc[::-1].reset_index(drop=True)

    df_json = df.to_json(orient="records")
    df_text_json = df_text.to_json(orient="records")
    df_stat_json = df_stat.to_json(orient="records")
    return df_json, df_text_json, df_stat_json

def lambda_handler(event, context):
    ticker = event.get("ticker", "AAPL")
    snapshot, news, stats = get_daily_data(ticker)

    timestamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")

    # Save 3 JSON files to S3
    s3.put_object(
        Bucket=BUCKET,
        Key=f"yahoo/{ticker}/snapshot_{timestamp}.json",
        Body=json.dumps(snapshot).encode("utf-8")
    )
    s3.put_object(
        Bucket=BUCKET,
        Key=f"yahoo/{ticker}/news_{timestamp}.json",
        Body=json.dumps(news).encode("utf-8")
    )
    s3.put_object(
        Bucket=BUCKET,
        Key=f"yahoo/{ticker}/stats_{timestamp}.json",
        Body=json.dumps(stats).encode("utf-8")
    )

    return {
        "status": "success",
        "ticker": ticker,
        "files": [
            f"yahoo/{ticker}/snapshot_{timestamp}.json",
            f"yahoo/{ticker}/news_{timestamp}.json",
            f"yahoo/{ticker}/stats_{timestamp}.json"
        ]
    }