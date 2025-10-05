import boto3

client = boto3.client("bedrock-agent-runtime", region_name="us-east-1")

def lambda_handler(event, context):
    kb_id = "W6CWQ9LMUU"
    model_arn = "arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-3-sonnet-20240229-v1:0"

    response = client.retrieve_and_generate(
        input={"text": event.get("question", "Summarize the company's financials which should be an advice on whether the company is doing good or not and whether to invest in it or not as full sentences, not just bullet points. Be concise and compare where possible.")},
        retrieveAndGenerateConfiguration={
            "type": "KNOWLEDGE_BASE",   # REQUIRED
            "knowledgeBaseConfiguration": {
                "knowledgeBaseId": kb_id,
                "modelArn": model_arn
            }
        }
    )

    return {
        "status": "success",
        "answer": response["output"]["text"],
        "citations": response.get("citations", [])
    }
