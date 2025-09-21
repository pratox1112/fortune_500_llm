import requests
import boto3
from datetime import datetime

s3 = boto3.client("s3")
BUCKET = "fin-sum"
HEADERS = {"User-Agent": "Pratosh Karthikeyan pratosh@gmail.com"}

def fetch_latest_filing_html(company_name):
    # 1. Get CIK
    resp = requests.get("https://www.sec.gov/files/company_tickers.json", headers=HEADERS)
    resp.raise_for_status()
    companies = resp.json()
    cik = None
    for entry in companies.values():
        if company_name.lower() in entry['title'].lower():
            cik = str(entry['cik_str']).zfill(10)
            break
    if cik is None:
        raise ValueError(f"CIK not found for '{company_name}'")

    # 2. Get most recent 10-K or 10-Q
    resp = requests.get(f"https://data.sec.gov/submissions/CIK{cik}.json", headers=HEADERS)
    resp.raise_for_status()
    data = resp.json()

    filing = None
    for form, date, acc, doc in zip(
        data['filings']['recent']['form'],
        data['filings']['recent']['filingDate'],
        data['filings']['recent']['accessionNumber'],
        data['filings']['recent']['primaryDocument']
    ):
        if form in ['10-K', '10-Q']:
            filing = {"form": form, "date": date, "accession": acc, "doc": doc}
            break
    if filing is None:
        raise ValueError(f"No 10-K/10-Q filings found for {company_name}")

    # 3. Download filing HTML
    acc_no_clean = filing['accession'].replace("-", "")
    url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc_no_clean}/{filing['doc']}"
    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()

    return filing, resp.text


def lambda_handler(event, context):
    company = event.get("company", "Apple")
    timestamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")

    filing, html_content = fetch_latest_filing_html(company)

    key = f"sec/{company}/{filing['form']}_{timestamp}.html"

    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=html_content.encode("utf-8"),
        ContentType="text/html"
    )

    return {
        "status": "success",
        "company": company,
        "form": filing['form'],
        "date": filing['date'],
        "s3_key": key
    }
