import boto3, json, os

sf = boto3.client("stepfunctions")

# put your Step Function ARN in an environment variable for flexibility
STATE_MACHINE_ARN = os.environ.get("STATE_MACHINE_ARN", "arn:aws:states:us-east-1:183295421422:stateMachine:fin_pipeline")

def lambda_handler(event, context):
    try:
        # event should contain ticker + company
        response = sf.start_execution(
            stateMachineArn=STATE_MACHINE_ARN,
            input=json.dumps(event)
        )
        return {
            "status": "started",
            "executionArn": response["executionArn"],
            "startDate": str(response["startDate"])
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}
