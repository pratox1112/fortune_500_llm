import sys
import json
import pandas as pd
from bs4 import BeautifulSoup
from pathlib import Path

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

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

def extract_kpis_from_html(html_text, tag_map, filename="filing"):
    soup = BeautifulSoup(html_text, "xml")

    facts = []
    for tag in soup.find_all(["ix:nonFraction", "ix:nonfraction"]):
        nm = tag.get("name")
        if not nm:
            continue
        val = parse_number(tag.text.strip(), tag.get("sign"))
        facts.append({"qname": nm, "value": val})

    facts_df = pd.DataFrame(facts)

    out = {"Company": Path(filename).stem}
    for kpi, tags in tag_map.items():
        sub = facts_df[facts_df["qname"].isin(tags)]
        out[kpi] = float(sub.iloc[-1]["value"]) if not sub.empty else None

    return out

# ---------------- MAIN ----------------
args = getResolvedOptions(sys.argv, ["JOB_NAME", "input_path", "output_path"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# 1. Read HTML file from S3
input_path = args["input_path"]   # e.g. s3://fin-sum/sec/Apple/10-Q_20250921-000728.html
output_path = args["output_path"] # e.g. s3://fin-sum-processed/Apple/

html_text = spark.read.text(input_path).collect()
html_text = "\n".join([row.value for row in html_text])

# 2. Extract KPIs
kpis = extract_kpis_from_html(html_text, TAG_MAP, filename=input_path)

# ---------------- SCHEMA (Explicit) ----------------
schema = StructType([
    StructField("Company", StringType(), True),
    StructField("Revenue", DoubleType(), True),
    StructField("Net Income", DoubleType(), True),
    StructField("EPS Basic", DoubleType(), True),
    StructField("EPS Diluted", DoubleType(), True),
    StructField("COGS", DoubleType(), True),
    StructField("SG&A", DoubleType(), True),
    StructField("R&D", DoubleType(), True),
    StructField("CFO", DoubleType(), True),
    StructField("CapEx", DoubleType(), True),
    StructField("Assets", DoubleType(), True),
    StructField("Liabilities", DoubleType(), True),
    StructField("Equity", DoubleType(), True),
])

# 3. Convert to Spark DataFrame with explicit schema
df = spark.createDataFrame([kpis], schema=schema)

# 4. Write JSON output
df.coalesce(1).write.mode("overwrite").json(output_path)

print(f"✅ KPIs extracted for {input_path}, saved to {output_path}")
