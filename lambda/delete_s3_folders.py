import boto3

s3 = boto3.client("s3")

def lambda_handler(event, context):
    bucket_name = "fin-sum"
    folders = [
        "bronze/",
        "silver/",
        "gold/"
    ]
    
    for prefix in folders:
        print(f"Cleaning {bucket_name}/{prefix}")
        
        paginator = s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            if "Contents" in page:
                objects_to_delete = [
                    {"Key": obj["Key"]}
                    for obj in page["Contents"]
                    if obj["Key"] != prefix  # keep the folder marker if it exists
                ]
                if objects_to_delete:
                    s3.delete_objects(Bucket=bucket_name, Delete={"Objects": objects_to_delete})
                    print(f"Deleted {len(objects_to_delete)} objects from {prefix}")
        
        # Ensure the folder marker exists so the folder shows in S3
        s3.put_object(Bucket=bucket_name, Key=prefix)
        print(f"Ensured folder marker for {prefix}")
    
    return {
        "statusCode": 200,
        "body": f"Emptied folders but kept structure: {', '.join(folders)}"
    }
