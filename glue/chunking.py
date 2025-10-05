import sys
import json
import datetime
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
import boto3

# ---------------- JOB ARGS ----------------
args = getResolvedOptions(sys.argv, ["JOB_NAME", "input_path", "output_path"])
input_path = args["input_path"]      # e.g. s3://fin-sum/silver/
output_path = args["output_path"]    # e.g. s3://fin-sum/chunks/

today = datetime.datetime.utcnow().strftime("%Y-%m-%d")

# ---------------- INIT ----------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
s3 = boto3.client("s3")

# ---------------- HELPERS ----------------
def write_jsonl(records, bucket, key):
    """Write list of dicts to S3 as JSONL"""
    if not records:
        return
    body = "\n".join([json.dumps(r) for r in records])
    s3.put_object(Bucket=bucket, Key=key, Body=body.encode("utf-8"))

def chunk_kpis(data):
    """Chunk KPIs from filings into fine-grained + summary chunks."""
    if not isinstance(data, dict):
        return []
    company = data.get("Company", "UNKNOWN")
    chunks = []

    # Fine-grained chunks: one KPI per chunk
    for k, v in data.items():
        if k != "Company" and v is not None:
            chunks.append({
                "company": company,
                "source": "kpi",
                "chunk_text": f"{company} KPI: {k} is {v}",
                "metric": k,
                "timestamp": today
            })

    # Summary chunk: combine all KPIs into one string
    summary_text = "; ".join([f"{k} = {v}" for k, v in data.items() if k != "Company" and v is not None])
    if summary_text:
        chunks.append({
            "company": company,
            "source": "kpi_summary",
            "chunk_text": f"{company} Financial Summary: {summary_text}",
            "timestamp": today
        })

    return chunks


def chunk_stats(data, window="monthly"):
    """Chunk stats (OHLCV) into aggregated groups instead of row-level."""
    if not isinstance(data, list):
        return []
    import pandas as pd

    df = pd.DataFrame(data)
    if df.empty:
        return []

    company = df.get("company", ["UNKNOWN"])[0]
    df["Date"] = pd.to_datetime(df["Date"])

    # Clean numeric columns (remove commas and cast)
    for col in ["Open", "High", "Low", "Close", "Adj Close"]:
        df[col] = pd.to_numeric(df[col].astype(str).str.replace(",", ""), errors="coerce")
    df["Volume"] = pd.to_numeric(df["Volume"].astype(str).str.replace(",", ""), errors="coerce")

    chunks = []
def chunk_stats(data, window="monthly"):
    """Chunk stats (OHLCV) into aggregated groups instead of row-level."""
    if not isinstance(data, list):
        return []
    import pandas as pd

    df = pd.DataFrame(data)
    if df.empty:
        return []

    company = df.get("company", ["UNKNOWN"])[0]
    df["Date"] = pd.to_datetime(df["Date"])

    # Clean numeric columns (remove commas and cast)
    for col in ["Open", "High", "Low", "Close", "Adj Close"]:
        df[col] = pd.to_numeric(df[col].astype(str).str.replace(",", ""), errors="coerce")
    df["Volume"] = pd.to_numeric(df["Volume"].astype(str).str.replace(",", ""), errors="coerce")

    chunks = []
    if window == "monthly":
        grouped = df.groupby(df["Date"].dt.to_period("M"))
    elif window == "weekly":
        grouped = df.groupby(df["Date"].dt.to_period("W"))
    else:
        grouped = [("all", df)]

    for period, sub in grouped:
        open_avg = sub["Open"].mean()
        close_avg = sub["Close"].mean()
        high_max = sub["High"].max()
        low_min = sub["Low"].min()
        vol_avg = sub["Volume"].mean()

        chunk_text = (f"{company} stats {period}: "
                      f"Avg Open {open_avg:.2f}, Avg Close {close_avg:.2f}, "
                      f"High {high_max:.2f}, Low {low_min:.2f}, "
                      f"Avg Volume {int(vol_avg):,}")

        chunks.append({
            "company": company,
            "source": "stats",
            "chunk_text": chunk_text,
            "period": str(period),
            "timestamp": today
        })

    return chunks


    for period, sub in grouped:
        open_avg = sub["Open"].mean()
        close_avg = sub["Close"].mean()
        high_max = sub["High"].max()
        low_min = sub["Low"].min()
        vol_avg = sub["Volume"].mean()

        chunk_text = (f"{company} stats {period}: "
                      f"Avg Open {open_avg:.2f}, Avg Close {close_avg:.2f}, "
                      f"High {high_max:.2f}, Low {low_min:.2f}, "
                      f"Avg Volume {int(vol_avg):,}")

        chunks.append({
            "company": company,
            "source": "stats",
            "chunk_text": chunk_text,
            "period": str(period),
            "timestamp": today
        })

    return chunks



def chunk_snapshot(data, today=None):
    """Chunk snapshot into a single compact chunk."""
    if today is None:
        today = datetime.datetime.utcnow().strftime("%Y-%m-%d")

    if isinstance(data, list):
        entry = data[0] if data else {}
    elif isinstance(data, dict):
        entry = data
    else:
        return []

    company = entry.get("company", "UNKNOWN")
    values = [f"{k} = {v}" for k, v in entry.items() if k != "company" and v is not None]
    chunk_text = f"{company} Snapshot ({today}): " + "; ".join(values)

    return [{
        "company": company,
        "source": "snapshot",
        "chunk_text": chunk_text,
        "timestamp": today
    }]


def chunk_news(data, company="UNKNOWN"):
    """Chunk news so each article is one chunk."""
    if not isinstance(data, list):
        return []
    chunks = []
    for item in data:
        if isinstance(item, dict):
            title = item.get("article title", "").strip()
            url = item.get("link")
            if title:
                chunks.append({
                    "company": item.get("company", company),
                    "source": "news",
                    "chunk_text": title,
                    "url": url,
                    "timestamp": today
                })
    return chunks

# ---------------- MAIN ----------------
bucket = "fin-sum"
prefix = "silver"
out_bucket = "fin-sum"
out_prefix = "gold"

response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
for obj in response.get("Contents", []):
    key = obj["Key"]
    if not key.endswith(".json"):
        continue

    head = s3.head_object(Bucket=bucket, Key=key)
    metadata = head.get("Metadata", {})
    file_type = metadata.get("type")

    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
    try:
        data = json.loads(body)
    except Exception as e:
        print(f"⚠️ Skipping {key}, invalid JSON: {e}")
        continue

    # Call appropriate chunker
    if file_type == "news":
        chunks = chunk_news(data)
    elif file_type == "snapshot":
        chunks = chunk_snapshot(data)
    elif file_type == "stats":
        chunks = chunk_stats(data)
    elif file_type == "filings":
        chunks = chunk_kpis(data)
    else:
        print(f"⚠️ Unknown type for {key}, skipping")
        continue

    # Derive output filename from input
    base_name = key.split("/")[-1].replace(".json", ".jsonl")
    out_key = f"{out_prefix}/{base_name}"

    # Write to S3
    write_jsonl(chunks, out_bucket, out_key)
    print(f"✅ Wrote {len(chunks)} chunks → s3://{out_bucket}/{out_key}")
