import sys
import json
import pandas as pd
from bs4 import BeautifulSoup
import boto3
from datetime import datetime

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions

# ---------------- TAG MAP ----------------
TAG_MAP = {
    "Revenue": [
        "us-gaap:Revenues", "us-gaap:SalesRevenueNet",
        "us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax",
        "us-gaap:RevenueFromContractWithCustomerIncludingAssessedTax",
        "us-gaap:TotalRevenuesAndOtherIncome", "ifrs-full:Revenue",
    ],
    "Net Income": [
        "us-gaap:NetIncomeLoss", "us-gaap:ProfitLoss",
        "us-gaap:NetIncomeLossAvailableToCommonStockholdersBasic",
        "us-gaap:NetIncomeLossAvailableToCommonStockholdersDiluted",
        "ifrs-full:ProfitLoss",
    ],
    "EPS Basic": [
        "us-gaap:EarningsPerShareBasic",
        "us-gaap:IncomeLossFromContinuingOperationsPerBasicShare",
        "ifrs-full:BasicEarningsLossPerShare",
    ],
    "EPS Diluted": [
        "us-gaap:EarningsPerShareDiluted",
        "us-gaap:IncomeLossFromContinuingOperationsPerDilutedShare",
        "ifrs-full:DilutedEarningsLossPerShare",
    ],
    "COGS": [
        "us-gaap:CostOfGoodsAndServicesSold",
        "us-gaap:CostOfRevenue",
        "us-gaap:CostOfSales",
        "ifrs-full:CostOfSales",
    ],
    "SG&A": [
        "us-gaap:SellingGeneralAndAdministrativeExpense",
        "us-gaap:GeneralAndAdministrativeExpense",
        "us-gaap:SellingAndMarketingExpense",
        "ifrs-full:AdministrativeExpense",
    ],
    "R&D": [
        "us-gaap:ResearchAndDevelopmentExpense",
        "us-gaap:ResearchAndDevelopmentExpenseExcludingAcquiredInProcessCost",
        "ifrs-full:ResearchAndDevelopmentExpense",
    ],
    "CFO": [
        "us-gaap:NetCashProvidedByUsedInOperatingActivities",
        "us-gaap:NetCashProvidedByOperatingActivitiesContinuingOperations",
        "ifrs-full:NetCashFlowsFromUsedInOperatingActivities",
    ],
    "CapEx": [
        "us-gaap:PaymentsToAcquirePropertyPlantAndEquipment",
        "us-gaap:PaymentsToAcquireProductiveAssets",
        "us-gaap:PurchaseOfPropertyAndEquipment",
        "ifrs-full:PurchaseOfPropertyPlantAndEquipmentClassifiedAsInvestingActivities",
    ],
    "Assets": ["us-gaap:Assets", "ifrs-full:Assets"],
    "Liabilities": ["us-gaap:Liabilities", "ifrs-full:Liabilities"],
    "Equity": [
        "us-gaap:StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
        "us-gaap:StockholdersEquity",
        "us-gaap:CommonStockholdersEquity",
        "ifrs-full:Equity",
    ],
}

# ---------------- HELPERS ----------------
def parse_number(value, sign=None):
    try:
        val = float(value.replace(",", ""))
        if sign == "-":
            val = -val
        return val
    except Exception:
        return None


def extract_kpis_from_html(html_text, tag_map, bucket, key, s3):
    """Extract KPI values from HTML iXBRL using tag map"""
    soup = BeautifulSoup(html_text, "xml")

    # Collect numeric facts
    facts = []
    for tag in soup.find_all(["ix:nonFraction", "ix:nonfraction"]):
        nm = tag.get("name")
        if not nm:
            continue
        val = parse_number(tag.text.strip(), tag.get("sign"))
        facts.append({"qname": nm, "value": val})

    facts_df = pd.DataFrame(facts)

    # Try metadata first
    response = s3.head_object(Bucket=bucket, Key=key)
    metadata = response.get("Metadata", {})

    ticker = metadata.get("company")

    # Build KPI dict
    out = {"Company": ticker}
    for kpi, tags in tag_map.items():
        sub = facts_df[facts_df["qname"].isin(tags)]
        out[kpi] = float(sub.iloc[-1]["value"]) if not sub.empty else None

    return out, ticker


# ---------------- MAIN ----------------
args = getResolvedOptions(sys.argv, ["JOB_NAME", "input_path", "output_path"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
s3 = boto3.client("s3")

# 1. Extract bucket/key from input_path (prefix)
input_path = args["input_path"]   # e.g. s3://fin-sum/bronze/
output_path = args["output_path"] # e.g. s3://fin-sum/gold/

in_bucket = input_path.replace("s3://", "").split("/")[0]
in_prefix = "/".join(input_path.replace("s3://", "").split("/")[1:])

# 2. List all objects under prefix
response = s3.list_objects_v2(Bucket=in_bucket, Prefix=in_prefix)

for obj in response.get("Contents", []):
    key = obj["Key"]
    if not key.endswith(".html"):
        continue

    # 3. Read HTML file from S3
    html_text = s3.get_object(Bucket=in_bucket, Key=key)["Body"].read().decode("utf-8")

    # 4. Extract KPIs
    kpis, ticker = extract_kpis_from_html(html_text, TAG_MAP, in_bucket, key, s3)

    # 5. Convert to JSON
    json_body = json.dumps(kpis)
    
    run_ts = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    
    response = s3.head_object(Bucket=in_bucket, Key=key)
    metadata = response.get("Metadata", {})
    form = metadata.get("form", "10-Q")
    filing_date = metadata.get("date", "NA")

    out_key = f"silver/{ticker}_{form}_{filing_date}_{run_ts}.json"
    
    # 7. Write JSON file to S3
    s3.put_object(
        Bucket=in_bucket,
        Key=out_key,
        Body=json_body.encode("utf-8"),
        Metadata={
        "company": ticker,
        "source": "SEC EDGAR",
        "job_name": args["JOB_NAME"],
        "type": "filings",
        "date": filing_date
    }
    )
