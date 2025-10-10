import json
import boto3

client = boto3.client("bedrock-agent-runtime", region_name="us-east-1")

# Direct initialization (no env variables)
KB_ID = "W6CWQ9LMUU"
MODEL_ARN = "arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-3-sonnet-20240229-v1:0"

DEFAULT_PROMPT = (
    "Summarize the company's financials which should be an advice on whether "
    "the company is doing good or not and whether to invest in it or not as "
    "full sentences, not just bullet points. Be concise and compare where possible."
)

def _extract_question(event):
    # Case 1: Direct Lambda invocation (test input)
    if isinstance(event, dict) and "question" in event:
        return event["question"]

    # Case 2: API Gateway proxy → JSON string in event["body"]
    body = event.get("body") if isinstance(event, dict) else None
    if body:
        try:
            if isinstance(body, str):
                body = json.loads(body)
            return body.get("question")
        except Exception:
            pass
    return None

def lambda_handler(event, context):
    question = _extract_question(event) or DEFAULT_PROMPT

    response = client.retrieve_and_generate(
        input={"text": question},
        retrieveAndGenerateConfiguration={
            "type": "KNOWLEDGE_BASE",
            "knowledgeBaseConfiguration": {
                "knowledgeBaseId": KB_ID,
                "modelArn": MODEL_ARN
            }
        }
    )

    payload = {
        "status": "success",
        "answer": response["output"]["text"],
        "citations": response.get("citations", [])
    }

    return {
        "statusCode": 200,
        "headers": {
            "content-type": "application/json",
            "access-control-allow-origin": "*",          # CORS for dev
            "access-control-allow-headers": "content-type,authorization",
            "access-control-allow-methods": "POST,OPTIONS"
        },
        "body": json.dumps(payload)
    }
