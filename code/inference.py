import os, json, csv, io, boto3, logging
s3 = boto3.client('s3')
log = logging.getLogger()
log.setLevel(logging.INFO)

def model_fn(model_dir):
    log.info("model_fn: model_dir=%s", model_dir)
    return None

def input_fn(request_body, request_content_type):
    """
    Be tolerant to:
      - empty body (Async Inference with InputLocation)
      - bytes vs str
      - non-JSON (text/plain, text/csv, octet-stream)
    Return a dict for predict_fn.
    """
    try:
        if not request_body:
            log.info("input_fn: empty body (likely Async + InputLocation). Returning {}")
            return {}

        if isinstance(request_body, (bytes, bytearray)):
            raw = request_body
            body_str = raw.decode("utf-8", errors="ignore").strip()
        else:
            raw = request_body.encode("utf-8")
            body_str = (request_body or "").strip()

        ct = (request_content_type or "").lower()
        log.info("input_fn: content_type=%s len(raw)=%s", ct, len(raw))

        if ct.startswith("application/json") or (not ct and body_str.startswith("{")):
            data = json.loads(body_str) if body_str else {}
            log.info("input_fn: parsed JSON keys=%s", list(data.keys()))
            return data

        if ct.startswith("text/plain") or ct == "" or ct.startswith("application/x-www-form-urlencoded"):
            return {"raw": body_str}

        if ct.startswith("text/csv"):
            # parse CSV to list of rows
            rows = list(csv.reader(io.StringIO(body_str)))
            return {"csv": rows, "raw": body_str}

        if ct.startswith("application/octet-stream"):
            return {"bytes": raw, "size": len(raw)}

        # Fallback: return whatever we got as raw text
        return {"raw": body_str}
    except Exception as e:
        log.exception("input_fn: failed to parse body; returning {}. Error: %s", e)
        return {}

def predict_fn(data, model):
    log.info("predict_fn: start; data keys=%s", list(data.keys()))
    bucket = data.get('bucket')
    key = data.get('key')
    risk = data.get('risk_profile', 'MEDIUM')

    size = None
    if bucket and key:
        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
            blob = obj['Body'].read()
            size = len(blob)
            log.info("predict_fn: read %s bytes from s3://%s/%s", size, bucket, key)
        except Exception as e:
            log.exception("predict_fn: failed s3 get_object: %s", e)

    return {
        "summary": f"Processed {key or 'uploaded-document'} with risk {risk}.",
        "bytes_read": size,
        "echo": {k: v for k, v in data.items() if k in ("raw", "csv", "size")},  # shows what was parsed
        "stocks": ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL"],
        "notes": "Async writes this JSON to S3 under <output-bucket>/<InferenceId>/."
    }

def output_fn(prediction, accept):
    body = json.dumps(prediction).encode("utf-8")
    return body, "application/json"
