from fastapi import FastAPI, Request, HTTPException
from google.cloud import bigquery
import os
import base64
import json
import datetime

app = FastAPI()

BQ_PROJECT_ID = os.environ["BQ_PROJECT_ID"]
BQ_DATASET_ID = os.environ["BQ_DATASET_ID"]
BQ_TABLE_ID = os.environ["BQ_TABLE_ID"]

bq = bigquery.Client(project=BQ_PROJECT_ID)
table_ref = f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID}"


def decode_pubsub_push(body: dict) -> tuple[dict, dict]:
    if "message" not in body or "data" not in body["message"]:
        raise ValueError("Invalid Pub/Sub push payload")

    msg = body["message"]
    decoded = base64.b64decode(msg["data"]).decode("utf-8")
    envelope = json.loads(decoded)

    meta = {
        "pubsub_message_id": msg.get("messageId") or msg.get("message_id"),
        "pubsub_publish_time": msg.get("publishTime") or msg.get("publish_time"),
        "delivery_attempt": body.get("deliveryAttempt"),
        "subscription": body.get("subscription"),
        "attributes": msg.get("attributes", {}),
    }
    return envelope, meta


def dumps(v):
    return json.dumps(v, ensure_ascii=False, separators=(",", ":")) if v is not None else None


def to_bq_ts(value: str | None):
    """
    BigQuery TIMESTAMP accepts RFC3339 strings. Keep as-is if it looks like ISO.
    If missing, return None.
    """
    return value


@app.post("/pubsub")
async def pubsub_handler(request: Request):
    try:
        body = await request.json()
        envelope, meta = decode_pubsub_push(body)

        row = {
            # TIMESTAMP fields
            "received_at": to_bq_ts(envelope.get("received_at")),
            "pubsub_publish_time": to_bq_ts(meta.get("pubsub_publish_time")),
            "ingested_at": datetime.datetime.utcnow().isoformat(timespec="milliseconds") + "Z",

            # STRING fields
            "request_id": envelope.get("request_id"),
            "source": envelope.get("source"),
            "method": envelope.get("method"),
            "path": envelope.get("path"),
            "content_type": envelope.get("content_type"),
            "remote_ip": envelope.get("remote_ip"),
            "user_agent": envelope.get("user_agent"),
            "headers_json": dumps(envelope.get("headers")),
            "query_json": dumps(envelope.get("query")),
            "body_text": envelope.get("body_text"),
            "body_json": dumps(envelope.get("body_json")),
            "pubsub_message_id": meta.get("pubsub_message_id"),
            "subscription": meta.get("subscription"),
            "pubsub_attributes_json": dumps(meta.get("attributes")),

            # INTEGER / BOOLEAN
            "delivery_attempt": meta.get("delivery_attempt"),
            "body_is_base64": envelope.get("body_is_base64"),
        }

        # Use Pub/Sub message id as insertId for retry dedupe
        insert_id = meta.get("pubsub_message_id") or envelope.get("request_id")
        errors = bq.insert_rows_json(table_ref, [row], row_ids=[insert_id])

        if errors:
            # Non-2xx triggers Pub/Sub retry; DLQ can catch repeated failures
            raise HTTPException(status_code=500, detail={"bq_errors": errors})

        return {"status": "ok"}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
