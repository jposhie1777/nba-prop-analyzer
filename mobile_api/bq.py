#bq.py
from google.cloud import bigquery
import os

def get_bq_client() -> bigquery.Client:
    project = os.getenv("GCP_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")

    if project:
        return bigquery.Client(project=project)

    # ✅ Cloud Run / ADC fallback
    return bigquery.Client()
