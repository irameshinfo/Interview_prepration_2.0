import base64
import json

from google.cloud import bigquery
from datetime import datetime


# =========================================================
# BIGQUERY CONFIG
# =========================================================

PROJECT_ID = "rameshgcplearning"

TABLE_ID = (
    "rameshgcplearning.staging.customers_stream"
)


# =========================================================
# PUBSUB → BIGQUERY
# =========================================================

def pubsub_to_bigquery(event, context):

    client = bigquery.Client()

    try:

        # =================================================
        # DECODE PUBSUB MESSAGE
        # =================================================

        message_data = base64.b64decode(
            event["data"]
        ).decode("utf-8")

        print(f"Received message: {message_data}")

        # =================================================
        # CONVERT JSON
        # =================================================

        data = json.loads(message_data)

        # =================================================
        # ADD LOAD TIME
        # =================================================

        data["load_time"] = (
            datetime.utcnow().isoformat()
        )

        # =================================================
        # OPTIONAL VALIDATION
        # =================================================

        if len(data["mobile_no"]) != 10:

            print("Invalid mobile number")

            return

        # =================================================
        # INSERT INTO BIGQUERY
        # =================================================

        errors = client.insert_rows_json(

            TABLE_ID,

            [data]
        )

        if errors:

            print(f"Insert errors: {errors}")

        else:

            print("Row inserted successfully")

    except Exception as e:

        print(f"Error: {str(e)}")