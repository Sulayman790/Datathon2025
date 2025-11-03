# lambda_s3_worker.py
import os, json, time, boto3, traceback

region         = os.environ.get("AWS_REGION","us-west-2")
code_bucket    = os.environ["CODE_BUCKET"]              # where your code/ files live
jobs_table     = os.environ["LAW_JOBS_TABLE"]
output_bucket  = os.environ["OUTPUT_BUCKET"]            # same as lambda_start_job’s OUTPUT_BUCKET
role_arn       = os.environ["SM_ROLE_ARN"]              # SageMaker execution role
image_uri      = os.environ.get("SM_IMAGE_URI", "763104351884.dkr.ecr.us-west-2.amazonaws.com/sagemaker-scikit-learn:1.2-1-cpu-py3")
subnet_ids     = os.environ.get("SUBNET_IDS","").split(",") if os.environ.get("SUBNET_IDS") else []
sg_ids         = os.environ.get("SECURITY_GROUP_IDS","").split(",") if os.environ.get("SECURITY_GROUP_IDS") else []

ddb        = boto3.client("dynamodb", region_name=region)
sagemaker  = boto3.client("sagemaker", region_name=region)

def _status(job_id, s):
    ddb.update_item(
        TableName=jobs_table,
        Key={"job_id": {"S": job_id}},
        UpdateExpression="SET #s=:v, updated_at=:u",
        ExpressionAttributeNames={"#s":"status"},
        ExpressionAttributeValues={":v":{"S":s}, ":u":{"N":str(int(time.time()))}}
    )

# inside lambda_s3_worker.py (the worker your dispatcher invokes)

def _build_processing_inputs(source_bucket: str, prefixes: list[str]):
    inputs = []

    # 1) Code bundle (where Charles-Refactor.py + requirements are stored)
    inputs.append({
        "S3Input": {
            "S3Uri": f"s3://{os.environ['CODE_BUCKET']}/code/",
            "LocalPath": "/opt/ml/processing/code",
            "S3DataType": "S3Prefix",
            "S3InputMode": "File",
        }
    })

    # 2) Your three data prefixes from the same bucket
    #    They’ll be mounted at different local folders; we’ll pass all to Charles via INPUT_DIRS.
    for p in prefixes:
        # normalize trailing slash
        p = p if p.endswith("/") else p + "/"
        folder_name = p.strip("/").replace("/", "_")  # just to make a safe folder name
        inputs.append({
            "S3Input": {
                "S3Uri": f"s3://{source_bucket}/{p}",
                "LocalPath": f"/opt/ml/processing/input/{folder_name}",
                "S3DataType": "S3Prefix",
                "S3InputMode": "File",
            }
        })
    return inputs

def lambda_handler(event, _ctx):
    job_id        = event["job_id"]
    output_bucket = event["output_bucket"]
    output_prefix = event["output_prefix"]                 # e.g. "<job_id>/"
    source_bucket = event["source_bucket"]                 # csv-file-store-6abb71a0
    prefixes      = event["source_prefixes"]               # ["dzd-bfr96ijed4gea8/","3opp39w1mn4780/","shared/"]

    processing_inputs = _build_processing_inputs(source_bucket, prefixes)

    # Tell Charles where the outputs go (Processing output dir)
    processing_outputs = [{
        "S3Output": {
            "S3Uri": f"s3://{output_bucket}/{output_prefix}intermediate/step1/",
            "LocalPath": "/opt/ml/processing/output/step1",
            "S3UploadMode": "EndOfJob",
        }
    }]

    # Build the list of local input dirs to feed to Charles (as JSON string)
    local_dirs = []
    for p in prefixes:
        p = p if p.endswith("/") else p + "/"
        folder_name = p.strip("/").replace("/", "_")
        local_dirs.append(f"/opt/ml/processing/input/{folder_name}")

    env = {
        # Charles will iterate these dirs to read all the files it needs
        "INPUT_DIRS": json.dumps(local_dirs),
        # Where Charles should write CSVs; Processing will sync this to S3 as above
        "OUT_DIR": "/opt/ml/processing/output/step1",
        "AWS_REGION": os.environ.get("AWS_REGION","us-west-2"),
        # add any other knobs your script reads (MAX_DOC_CHARS, MAX_CHUNK_CHARS, Bedrock model names, etc.)
    }

    command = [
        "bash", "-lc",
        # install dependencies for Charles, then run it
        "pip install -r /opt/ml/processing/code/requirements-step1.txt && "
        "python /opt/ml/processing/code/Charles-Refactor.py"
    ]

    sagemaker.create_processing_job(
        ProcessingJobName=f"law-step1-charles-{job_id}"[:63],
        RoleArn=os.environ["SM_ROLE_ARN"],
        AppSpecification={"ImageUri": os.environ["SM_IMAGE_URI"], "ContainerEntrypoint": command},
        Environment=env,
        ProcessingInputs=processing_inputs,
        ProcessingOutputConfig={"Outputs": processing_outputs},
        ProcessingResources={"ClusterConfig": {"InstanceCount": 1, "InstanceType": "ml.m5.xlarge", "VolumeSizeInGB": 50}},
    )
