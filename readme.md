# Fortune 500 LLM Pipeline

An intelligent data processing pipeline that leverages AWS services and Large Language Models to analyze Fortune 500 company data from SEC filings and Yahoo Finance. The system uses web scraping to collect financial data and Retrieval Augmented Generation (RAG) to provide accurate, context-aware responses.

## 🏗️ Architecture

This project implements a serverless, event-driven architecture using AWS Step Functions to orchestrate the following workflow:

```
Start → PreSyncKB → FetchData (Parallel) → GlueHtmlToJson → GlueChunking 
→ SyncBedrockKB → WaitForKB → RetrieveAndGenerate → DeleteS3 → End
```

### Workflow Components

1. **PreSyncKB**: Prepares the Bedrock Knowledge Base for synchronization
2. **FetchData**: Parallel web scraping from:
   - Yahoo Finance (financial metrics, stock data, analyst ratings)
   - SEC EDGAR (official financial filings, 10-K, 10-Q forms)
3. **GlueHtmlToJson**: AWS Glue job to convert HTML/iXBRL filings to structured JSON
4. **GlueChunking**: Chunks large documents for optimal embedding and retrieval
5. **SyncBedrockKB**: Syncs processed data with AWS Bedrock Knowledge Base
6. **WaitForKB**: Ensures Knowledge Base indexing is complete
7. **RetrieveAndGenerate**: RAG endpoint for querying financial data
8. **DeleteS3**: Cleans up temporary S3 objects

## 🚀 Features

- **Automated Web Scraping Pipeline**: Serverless data collection orchestrated with AWS Step Functions
- **Multi-Source Data Integration**: Scrapes and combines SEC filings and Yahoo Finance data
- **Intelligent Document Processing**: Converts complex financial documents (iXBRL/HTML) to structured JSON
- **Respectful Scraping**: Implements rate limiting and user-agent headers
- **Vector Search**: Leverages AWS Bedrock Knowledge Base for semantic search
- **RAG Implementation**: Context-aware responses using retrieved financial data
- **Scalable Architecture**: Handles multiple Fortune 500 companies concurrently
- **Cost-Optimized**: Automatic cleanup of temporary resources

## 📋 Prerequisites

### AWS Services Required
- AWS Lambda
- AWS Step Functions
- AWS Glue
- AWS S3
- AWS Bedrock (Knowledge Base + Foundation Model access)
- IAM roles with appropriate permissions

### Development Requirements
- Python 3.9+
- AWS CLI configured
- Boto3
- AWS SAM CLI (optional, for local testing)

### Python Libraries for Web Scraping
```text
beautifulsoup4>=4.12.0
requests>=2.31.0
lxml>=4.9.0
selenium>=4.15.0  # For dynamic content if needed
pandas>=2.0.0
yfinance>=0.2.0  # Alternative helper library
```

## ⚖️ Legal and Ethical Considerations

**IMPORTANT**: This project scrapes publicly available data from SEC EDGAR and Yahoo Finance websites. Please ensure compliance with:

- **SEC.gov Terms of Service**: Requires proper User-Agent header with contact information
- **Yahoo Finance Terms of Service**: Review their terms regarding automated access
- **robots.txt**: Respect crawl delays and disallowed paths
- **Rate Limiting**: Implement appropriate delays between requests (recommended: 1-2 seconds minimum)
- **Fair Use**: Only scrape data for personal research, educational purposes, or as permitted by terms of service

**User Responsibilities**:
- Review and comply with all applicable terms of service
- Do not overload target servers
- Use scraped data responsibly and legally
- Consider using official APIs where available


## 📁 Project Structure

```
fortune_500_llm/
├── lambda/
│   ├── sync_kb.py/           
│   ├── fetch_yahoo.py/          
│   ├── get_sec.py/            
│   ├── api_gateway.py/       
│   ├── retrieve_and_generate.py/ 
|   ├── describe_state.py/ 
│   └── delete_s3_folders.py/             
├── glue/
│   ├── sec_html_to_json.py        
│   └── chunking.py            
├── step function/
│   ├── fin_pipeline.json
├── app.py
├── config.py
└── README.md
```

## 🔧 Lambda Functions

### YahooLambda
**Web Scraper for Yahoo Finance**
- Scrapes real-time stock prices and historical data
- Extracts financial statements (income statement, balance sheet, cash flow)
- Collects analyst ratings and recommendations
- Implements rate limiting and error handling
- Parses HTML tables using BeautifulSoup

### SECLambda
**Web Scraper for SEC EDGAR**
- Scrapes 10-K, 10-Q, 8-K filings from SEC EDGAR
- Downloads iXBRL/HTML financial reports
- Extracts CIK numbers and filing metadata
- Complies with SEC.gov user-agent requirements
- Handles pagination and multiple filing periods

### SyncBedrockKB
- Triggers Knowledge Base ingestion
- Monitors sync status
- Handles embedding generation

### RetrieveAndGenerate
- Implements RAG pattern
- Retrieves relevant context from KB
- Generates responses using Claude

### DeleteS3
- Cleans up temporary files
- Maintains S3 storage costs

## 🕷️ Web Scraping Details

### SEC EDGAR Scraping
```python
# Example SEC scraper implementation
import requests
from bs4 import BeautifulSoup
import time

headers = {
    'User-Agent': 'YourName/YourCompany your.email@example.com'
}

def scrape_sec_filing(cik, filing_type='10-K'):
    base_url = f'https://www.sec.gov/cgi-bin/browse-edgar'
    params = {
        'action': 'getcompany',
        'CIK': cik,
        'type': filing_type,
        'count': '100'
    }
    
    response = requests.get(base_url, params=params, headers=headers)
    time.sleep(1.5)  # Rate limiting
    
    soup = BeautifulSoup(response.content, 'html.parser')
    # Extract filing links and data...
```

### Yahoo Finance Scraping
```python
# Example Yahoo Finance scraper
def scrape_yahoo_financials(ticker):
    url = f'https://finance.yahoo.com/quote/{ticker}/financials'
    response = requests.get(url, headers=headers)
    time.sleep(1.5)  # Rate limiting
    
    soup = BeautifulSoup(response.content, 'html.parser')
    # Extract financial tables...
```

### Rate Limiting Strategy
- Minimum 1.5 seconds between requests
- Exponential backoff on errors
- Respect HTTP 429 (Too Many Requests) responses
- Randomized delays to appear more human-like

## 🧪 Testing

```bash
# Unit tests
pytest tests/

# Integration tests
pytest tests/integration/

# Test individual scraper
python -m scrapers.sec_scraper --ticker AAPL

# Test Lambda locally
python -m lambda.yahoo_lambda.test_handler
```

## 🔐 IAM Permissions

Ensure your Lambda execution role has:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "bedrock:*",
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "glue:StartJobRun",
        "glue:GetJobRun",
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "*"
    }
  ]
}
```

## 💰 Cost Optimization

- **S3 Lifecycle Policies**: Auto-delete temporary files after 7 days
- **Glue Job Workers**: Optimize worker count based on data volume
- **Bedrock Model Selection**: Use Claude Haiku for cost-effective queries
- **Step Functions Express**: Use Express workflows for high-volume runs
- **Lambda Memory**: Optimize memory allocation for scraping functions

## 📈 Monitoring

Monitor via CloudWatch:
- Lambda execution times and errors
- Scraping success/failure rates
- HTTP response codes and retry counts
- Glue job status and data processing metrics
- Step Functions execution status
- Bedrock KB sync progress

## 🛡️ Error Handling

The pipeline includes robust error handling:
- **HTTP Errors**: Automatic retries with exponential backoff
- **Parsing Errors**: Logs failed pages for manual review
- **Rate Limiting**: Respects 429 responses and adjusts delays
- **Timeouts**: Configurable timeouts for slow responses
- **Data Validation**: Checks for expected data structure before processing

## 🤝 Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit changes (`git commit -m 'Add AmazingFeature'`)
4. Push to branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

**Note**: When contributing scraping code, ensure it:
- Respects rate limits
- Includes proper error handling
- Uses appropriate user-agent headers
- Complies with website terms of service

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## ⚠️ Disclaimer

This project is for educational and research purposes only. Users are responsible for:
- Complying with all applicable laws and regulations
- Respecting website terms of service
- Implementing appropriate rate limiting
- Using data ethically and legally

The authors are not responsible for any misuse of this software.

## 🙏 Acknowledgments

- AWS Bedrock for Knowledge Base and Foundation Models
- SEC EDGAR for providing public access to financial filings
- Yahoo Finance for financial data
- Anthropic Claude for LLM capabilities
- BeautifulSoup and Requests libraries for web scraping

## 📧 Contact

Pratosh Karthikeyan
Northeastern University