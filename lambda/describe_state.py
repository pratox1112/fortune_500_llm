import boto3, json

sf = boto3.client("stepfunctions")

def lambda_handler(event, context):
    try:
        exec_arn = event["executionArn"]
        resp = sf.describe_execution(executionArn=exec_arn)

        output = None
        if "output" in resp:
            try:
                output = json.loads(resp["output"])
            except:
                output = resp["output"]

        return {
            "status": resp["status"],
            "executionArn": exec_arn,
            "startDate": str(resp["startDate"]),
            "stopDate": str(resp.get("stopDate", "")),
            "output": output
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}
