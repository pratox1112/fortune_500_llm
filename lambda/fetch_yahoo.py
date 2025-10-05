import pandas as pd
import requests
import boto3
import json
from datetime import datetime
from bs4 import BeautifulSoup

s3 = boto3.client("s3")
BUCKET = "fin-sum"  # <-- replace with your bucket

def get_daily_data(ticker):
    # 1. Snapshot
    url = f"https://finance.yahoo.com/quote/{ticker}"
    headers = {"User-Agent": "Mozilla/5.0"}
    r = requests.get(url, headers=headers)
    soup = BeautifulSoup(r.text, "html.parser")

    price = soup.find("div", {"class":"container yf-16vvaki"}).find("span", {"data-testid":"qsp-price"}).text
    daily_summary_table = soup.find_all("span", {"class": "value yf-1qull9i"})
    text_list = soup.find_all("div", {"class": "stream-item yf-186c5b2"})

    # Snapshot JSON (single record)
    snapshot = [{
        "company": ticker,
        "price": price,
        "prev_close": daily_summary_table[0].text,
        "open": daily_summary_table[1].text,
        "bid": daily_summary_table[2].text,
        "ask": daily_summary_table[3].text,
        "day_range": daily_summary_table[4].text,
        "week_range_52": daily_summary_table[5].text,
        "volume": daily_summary_table[6].text,
        "avg_volume": daily_summary_table[7].text,
        "market_cap": daily_summary_table[8].text,
        "beta": daily_summary_table[9].text,
        "pe_ratio": daily_summary_table[10].text,
        "eps": daily_summary_table[11].text,
        "forward_dividend": daily_summary_table[13].text,
        "1y_target_est": daily_summary_table[15].text
    }]

    # 2. News JSON (multiple records)
    news = []
    for i in text_list:
        news.append({
            "company": ticker,
            "article title": i.text,
            "link": i.find("a")["href"]
        })

    # 3. Stats JSON (time series)
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=1y&interval=1d"
    resp = requests.get(url, headers=headers).json()

    timestamps = resp["chart"]["result"][0]["timestamp"]
    indicators = resp["chart"]["result"][0]["indicators"]["quote"][0]
    adjclose = resp["chart"]["result"][0]["indicators"]["adjclose"][0]["adjclose"]

    df_stat = pd.DataFrame(indicators)
    df_stat["Adj Close"] = adjclose
    df_stat["Date"] = pd.to_datetime(timestamps, unit="s", utc=True).tz_convert("US/Eastern").date
    df_stat = df_stat[["Date", "open", "high", "low", "close", "Adj Close", "volume"]]
    df_stat.columns = ["Date", "Open", "High", "Low", "Close", "Adj Close", "Volume"]

    df_stat["Date"] = df_stat["Date"].apply(lambda x: x.strftime("%b %d, %Y"))
    for col in ["Open", "High", "Low", "Close", "Adj Close"]:
        df_stat[col] = df_stat[col].map(lambda x: f"{x:.2f}")
    df_stat["Volume"] = df_stat["Volume"].map(lambda x: f"{int(x):,}")

    # Add company field to every stats row
    stats = df_stat.to_dict(orient="records")
    for row in stats:
        row["company"] = ticker

    return snapshot, news, stats


def lambda_handler(event, context):
    ticker = event.get("ticker")
    snapshot, news, stats = get_daily_data(ticker)

    timestamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")

    # Save JSON files to S3 (silver layer)
    s3.put_object(
        Bucket=BUCKET,
        Key=f"silver/snapshot_{ticker}_{timestamp}.json",
        Body=json.dumps(snapshot).encode("utf-8"),
        Metadata={
            "ticker": ticker,
            "timestamp": timestamp,
            "type": "snapshot",
            "source": "yahoo"
        }
    )
    s3.put_object(
        Bucket=BUCKET,
        Key=f"silver/news_{ticker}_{timestamp}.json",
        Body=json.dumps(news).encode("utf-8"),
        Metadata={
            "ticker": ticker,
            "timestamp": timestamp,
            "type": "news",
            "source": "yahoo"
        }
    )
    s3.put_object(
        Bucket=BUCKET,
        Key=f"silver/stats_{ticker}_{timestamp}.json",
        Body=json.dumps(stats).encode("utf-8"),
        Metadata={
            "ticker": ticker,
            "timestamp": timestamp,
            "type": "stats",
            "source": "yahoo"
        }
    )

    return {
        "status": "success",
        "ticker": ticker,
        "files": [
            f"silver/snapshot_{ticker}_{timestamp}.json",
            f"silver/news_{ticker}_{timestamp}.json",
            f"silver/stats_{ticker}_{timestamp}.json"
        ]
    }
