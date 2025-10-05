import boto3
import os

client = boto3.client("bedrock-agent", region_name="us-east-1")

def lambda_handler(event, context):
    
    response = client.start_ingestion_job(
        knowledgeBaseId="W6CWQ9LMUU",
        dataSourceId="UYJNZ8F9NU" 
    )
    
    return {
        "status": "sync_started",
        "job": response["ingestionJob"]["ingestionJobId"]
    }
