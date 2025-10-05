import requests
import boto3
from datetime import datetime, timedelta

s3 = boto3.client("s3")
BUCKET = "fin-sum"   # <-- replace with your bucket
HEADERS = {
    "User-Agent": "FinSumBot/1.0 (Pratosh Karthikeyan; pratosh@gmail.com)"
}  # SEC requires valid UA


def fetch_10q_filings_last_year(company_name):
    try:
        # 1. Get CIK + ticker from SEC mapping file
        resp = requests.get("https://www.sec.gov/files/company_tickers.json",
                            headers=HEADERS, timeout=10)
        resp.raise_for_status()
        companies = resp.json()

        cik = None
        ticker = None
        for entry in companies.values():
            if company_name.lower() in entry['title'].lower():
                cik = str(entry['cik_str']).zfill(10)
                ticker = entry['ticker']
                break

        if cik is None:
            return None, f"CIK not found for '{company_name}'"

        # 2. Get submissions JSON for this company
        resp = requests.get(f"https://data.sec.gov/submissions/CIK{cik}.json",
                            headers=HEADERS, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        forms = data['filings']['recent']['form']
        dates = data['filings']['recent']['filingDate']
        accs = data['filings']['recent']['accessionNumber']
        docs = data['filings']['recent']['primaryDocument']

        cutoff = datetime.utcnow() - timedelta(days=365)
        filings = []

        for form, date, acc, doc in zip(forms, dates, accs, docs):
            # 🔎 Only collect 10-Q filings
            if form == '10-Q':
                filing_date = datetime.strptime(date, "%Y-%m-%d")
                if filing_date >= cutoff:
                    filings.append({
                        "form": form,
                        "date": date,
                        "accession": acc,
                        "doc": doc
                    })

        if not filings:
            return None, f"No 10-Q filings found in past year for {company_name}"

        # 3. Download HTML for each 10-Q filing
        results = []
        for filing in filings:
            acc_no_clean = filing['accession'].replace("-", "")
            url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc_no_clean}/{filing['doc']}"
            r = requests.get(url, headers=HEADERS, timeout=15)
            r.raise_for_status()

            results.append({
                "filing": filing,
                "html": r.text,
                "ticker": ticker
            })

        return results, None

    except Exception as e:
        return None, str(e)


def lambda_handler(event, context):
    company_name = event.get("company")  # full company name
    timestamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")

    results, error = fetch_10q_filings_last_year(company_name)

    if error:
        return {"status": "failed", "company": company_name, "reason": error}

    saved = []
    for result in results:
        filing = result["filing"]
        html_content = result["html"]
        ticker = result["ticker"]

        key = f"bronze/{ticker}_10-Q_{filing['date']}_{timestamp}.html"
        s3.put_object(
            Bucket=BUCKET,
            Key=key,
            Body=html_content.encode("utf-8"),
            ContentType="text/html",
            Metadata={
                "company": ticker,
                "form": filing['form'],
                "date": filing['date'],
                "timestamp": timestamp,
                "source": "SEC EDGAR"
            }
        )
        saved.append(key)

    return {
        "status": "success",
        "company": company_name,
        "filings_saved": saved
    }
