# lambda_function.py
import os, time, boto3, urllib.parse as urlparse

ddb = boto3.client('dynamodb')
s3  = boto3.client('s3')

JOBS_TABLE = os.environ['LAW_JOBS_TABLE']

def lambda_handler(event, context):
    print("EVENT:", event)
    for rec in event.get('Records', []):
        bucket = rec['s3']['bucket']['name']
        key    = urlparse.unquote(rec['s3']['object']['key'])
        job_id = key.split('/', 1)[0]
        if not job_id:
            print("No job_id parsed from", key)
            continue

        url = s3.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket, 'Key': key},
            ExpiresIn=3600 * 24
        )

        ddb.update_item(
            TableName=JOBS_TABLE,
            Key={'job_id': {'S': job_id}},  # include sort key if your table has one
            UpdateExpression='SET #s=:done, result_url=:u, updated_at=:t',
            ExpressionAttributeNames={'#s':'status'},
            ExpressionAttributeValues={
                ':done': {'S':'COMPLETED'},
                ':u': {'S': url},
                ':t': {'N': str(int(time.time()))},
            },
        )
        print(f"DDB updated for {job_id}")
