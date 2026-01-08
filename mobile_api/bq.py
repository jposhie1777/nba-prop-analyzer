#bq.py
from google.cloud import bigquery
import os

def get_bq_client() -> bigquery.Client:
    project = os.getenv("GCP_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")

    if not project:
        raise RuntimeError(
            "BigQuery project not set. "
            "Set GCP_PROJECT or GOOGLE_CLOUD_PROJECT."
        )

    return bigquery.Client(project=project)
