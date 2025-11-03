# lambda_start_job.py
import json, os, time, uuid, traceback, boto3

sagemaker = boto3.client("sagemaker")
ddb = boto3.client("dynamodb")

JOBS_TABLE = os.environ["LAW_JOBS_TABLE"]
OUTPUT_BUCKET = os.environ["LAW_OUTPUT_BUCKET"]
ROLE_ARN = os.environ["SAGEMAKER_EXEC_ROLE_ARN"]

CODE_BUCKET = "csv-file-store-6abb71a0"
SCRIPT_FILE = "Charles-Refactor.py"
IMAGE_URI = "763104351884.dkr.ecr.us-west-2.amazonaws.com/sagemaker-scikit-learn:1.2-1-cpu-py3"
REGION = "us-west-2"

def _log(msg, **kw):
    try:
        payload = {"level": "INFO", "msg": str(msg), **kw}
        print(json.dumps(payload))
    except Exception:
        print(f"[start_job] {msg} {kw}")

def _status(job_id, s):
    ddb.update_item(
        TableName=JOBS_TABLE,
        Key={"job_id": {"S": job_id}},
        UpdateExpression="SET #s=:v, updated_at=:u",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={":v": {"S": s}, ":u": {"N": str(int(time.time()))}},
    )

def _update(job_id, attrs):
    expr_names, expr_values, sets = {}, {}, []
    for i, (k, v) in enumerate(attrs.items()):
        nk, nv = f"#k{i}", f":v{i}"
        expr_names[nk] = k
        if isinstance(v, str):
            expr_values[nv] = {"S": v}
        elif isinstance(v, (int, float)):
            expr_values[nv] = {"N": str(v)}
        else:
            expr_values[nv] = {"S": json.dumps(v)}
        sets.append(f"{nk}={nv}")
    ddb.update_item(
        TableName=JOBS_TABLE,
        Key={"job_id": {"S": job_id}},
        UpdateExpression="SET " + ", ".join(sets),
        ExpressionAttributeNames=expr_names,
        ExpressionAttributeValues=expr_values,
    )

def lambda_handler(event, context):
    req_id = getattr(context, "aws_request_id", "unknown")
    _log("lambda_start_job invoked", request_id=req_id, raw_event_truncated=str(event)[:500])

    try:
        job_id = event["pathParameters"]["id"]
    except Exception as e:
        _log("missing job_id in pathParameters", request_id=req_id, error=str(e))
        return {
            "statusCode": 400,
            "headers": {"Access-Control-Allow-Origin": "*"},
            "body": json.dumps({"error": "bad_request", "details": "pathParameters.id required"}),
        }

    _log("loading job from ddb", request_id=req_id, job_id=job_id, table=JOBS_TABLE)
    res = ddb.get_item(TableName=JOBS_TABLE, Key={"job_id": {"S": job_id}})
    item = res.get("Item") or {}
    meta = json.loads(item.get("meta", {}).get("S", "{}")) if "meta" in item else {}

    input_bucket = "lawdemo-input"
    data_prefix = f"uploads/{job_id}/"
    out_prefix = f"shared/outputs/{job_id}/"

    _log("updating job I/O in ddb", request_id=req_id, job_id=job_id,
         output_bucket=OUTPUT_BUCKET, output_prefix=out_prefix,
         source_bucket=input_bucket, source_prefix=data_prefix)
    _update(job_id, {
        "output_bucket": OUTPUT_BUCKET,
        "output_prefix": out_prefix,
        "source_bucket": input_bucket,
        "source_prefix": data_prefix,
    })

    _status(job_id, "RUNNING")
    _log("status set RUNNING", request_id=req_id, job_id=job_id)

    processing_job_name = f"proc-{job_id}-{uuid.uuid4().hex[:8]}"
    _log("creating sagemaker processing job", request_id=req_id, job_id=job_id,
         processing_job_name=processing_job_name, role_arn=ROLE_ARN,
         image_uri=IMAGE_URI, code_bucket=CODE_BUCKET, script_file=SCRIPT_FILE,
         input=f"s3://{input_bucket}/{data_prefix}", output=f"s3://{OUTPUT_BUCKET}/{out_prefix}")

    try:
        sagemaker.create_processing_job(
            ProcessingJobName=processing_job_name,
            RoleArn=ROLE_ARN,
            AppSpecification={
                "ImageUri": IMAGE_URI,
                "ContainerEntrypoint": ["python3", f"/opt/ml/processing/code/{SCRIPT_FILE}"],
            },
            ProcessingResources={
                "ClusterConfig": {"InstanceCount": 1, "InstanceType": "ml.m5.xlarge", "VolumeSizeInGB": 50}
            },
            ProcessingInputs=[
                {
                    "InputName": "code",
                    "S3Input": {
                        "S3Uri": f"s3://{CODE_BUCKET}/",
                        "LocalPath": "/opt/ml/processing/code",
                        "S3DataType": "S3Prefix",
                        "S3InputMode": "File",
                    },
                },
                {
                    "InputName": "data",
                    "S3Input": {
                        "S3Uri": f"s3://{input_bucket}/{data_prefix}",
                        "LocalPath": "/opt/ml/processing/input",
                        "S3DataType": "S3Prefix",
                        "S3InputMode": "File",
                    },
                },
            ],
            ProcessingOutputConfig={
                "Outputs": [
                    {
                        "OutputName": "results",
                        "S3Output": {
                            "S3Uri": f"s3://{OUTPUT_BUCKET}/{out_prefix}",
                            "LocalPath": "/opt/ml/processing/output",
                            "S3UploadMode": "EndOfJob",
                        },
                    }
                ]
            },
        )

        _update(job_id, {"sagemaker_processing_job": processing_job_name})
        _log("sagemaker processing job created", request_id=req_id, job_id=job_id, processing_job_name=processing_job_name)

        return {
            "statusCode": 202,
            "headers": {"Access-Control-Allow-Origin": "*"},
            "body": json.dumps({
                "message": "SageMaker processing job started",
                "job_name": processing_job_name,
                "input": f"s3://{input_bucket}/{data_prefix}",
                "output": f"s3://{OUTPUT_BUCKET}/{out_prefix}",
            }),
        }

    except Exception as e:
        err = {"message": str(e), "trace": traceback.format_exc()[:4000]}
        _log("create_processing_job failed", request_id=req_id, job_id=job_id,
             processing_job_name=processing_job_name, error=str(e))
        _update(job_id, {"error": err})
        _status(job_id, "FAILED")
        return {
            "statusCode": 500,
            "headers": {"Access-Control-Allow-Origin": "*"},
            "body": json.dumps({"error": "processing_job_failed", "details": str(e)}),
        }
