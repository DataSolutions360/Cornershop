import requests
import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator


dag = DAG(
    dag_id="CORNERSHOP_DAG",
    default_args={
        "owner": "airflow",
        "start_date": datetime(2023, 5, 29),
        "email": ["ryan_walsh@colpal.com"],
        "email_on_failure": True
    },
    schedule_interval=None
)

env = Variable.get("landscape-abbv")
PROJECT_ID = Variable.get("gcp-project")
NAMESPACE = Variable.get("namespace")
SERVICE_ACCOUNT_NAME = "vault-sidecar"
CONTAINER_NAME = "Cornershop_Cities" #Cornershop_Products and Cornershop_Stores

AIRFLOW_REG = "us-east4-docker.pkg.dev/cp-artifact-registry/cp-de-team"
CONTAINER_IMG = f"{AIRFLOW_REG}/{CONTAINER_NAME}:dev"


DAG_ID = "{{dag.dag_id}}"
RUN_ID = "{{run_id}}"
INPUT_PIPELINE_BUCKET_NAME = "cp-advancedtech-sandbox-pipeline"
INPUT_PIPELINE_BUCKET_PATH = "ryan-git/cornershop_input"
INPUT_URL = "https://cornershopapp.com/brandintegration/api/v100/countries/US/available_stores"
#INPUT_URL = "https://cornershopapp.com/brandintegration/api/v100/cpgs/<cpg_id>/products"
#INPUT_URL = "https://cornershopapp.com/brandintegration/api/v100/cpgs/<cpg_id>/reports/store-supply"
OUTPUT_PIPELINE_BUCKET_PATH = "ryan-git/cornershop_output"
OUTPUT_FILENAME = "cornershop_output.parquet"
OUTPUT_PIPELINE_BUCKET_NAME = "cp-advancedtech-sandbox-pipeline"
OUTPUT_TABLE = f"{PROJECT_ID}.sandbox_output.{DAG_ID}_{CONTAINER_NAME}_{RUN_ID}"
INGESTION_TIME = str(datetime.now())

SECRET_LOCATION = "/vault/secrets/"

annotations = {
    "vault.hashicorp.com/agent-inject": "true",
    "vault.hashicorp.com/agent-pre-populate-only": "true",
    "vault.hashicorp.com/role": NAMESPACE,
    "vault.hashicorp.com/agent-inject-secret-cornershop-credentials.json": "secret/teams/external-data-ingest/cornershop",
    "vault.hashicorp.com/agent-inject-template-cornershop-credentials.json": '''{{ with secret "secret/teams/external-data-ingest/cornershop" }},
    {{ .Data.data | toJSON }},
{{ end }}''',
    "vault.hashicorp.com/agent-inject-secret-gcp-sa-bq.json": "secret/teams/cp-de-team/gcp-sa-bq",
    "vault.hashicorp.com/agent-inject-template-gcp-sa-bq.json": '''{{ with secret "secret/teams/cp-de-team/gcp-sa-bq" }},
    {{ .Data.data.key | base64Decode }}
{{ end }}''',
    "vault.hashicorp.com/agent-inject-secret-gcp-sa-storage.json": "secret/teams/cp-de-team/gcp-sa-storage",
    "vault.hashicorp.com/agent-inject-template-gcp-sa-storage.json": '''{{ with secret "secret/teams/cp-de-team/gcp-sa-storage" }},
    {{ .Data.data.key | base64Decode }}
{{ end }}''',
#    "vault.hashicorp.com/tls-skip-verify": "true",
#    "vault.hashicorp.com/agent-inject-secret-gcp-service-account.json": "secret/teams/cp-de-team/gcp-service-account",
#    "vault.hashicorp.com/agent-inject-template-gcp-service-account.json": '''{{ with secret "secret/teams/cp-de-team/gcp-service-account" }}
#    {{ .Data.data.key | base64Decode }}
#{{ end }}'''
}

def download_data():
    try:
        response = requests.get(INPUT_URL)
        response.raise_for_status()  # Raise an exception for non-2xx status codes
        json_data = response.json()
        df = pd.DataFrame(json_data)
        output_filename = "cornershop_stores.parquet"
        df.to_parquet(output_filename, index=False)
        print(f"Data downloaded successfully as {output_filename}.")
    except requests.exceptions.RequestException as e:
        raise Exception(f"Error occurred while making the HTTP request: {str(e)}")
    except ValueError as e:
        raise Exception(f"Error occurred while parsing JSON response: {str(e)}")
    except Exception as e:
        raise Exception(f"An error occurred while downloading data: {str(e)}")

start = DummyOperator(
    task_id="start",
    dag=dag
)

download_task = DummyOperator(
    task_id="download_data",
    dag=dag
)

container_task = KubernetesPodOperator(
    task_id="container_task",
    name="container_task",
    namespace=NAMESPACE,
    image=CONTAINER_IMG,
    image_pull_policy="Always",
    arguments=[
        "--input_url",
        INPUT_URL,
        "--output_bucket_name",
        OUTPUT_PIPELINE_BUCKET_NAME,
        "--output_bucket_path",
        OUTPUT_PIPELINE_BUCKET_PATH,
        "--output_file_name",
        OUTPUT_FILENAME
    ],
    annotations=annotations,
    get_logs=True,
    dag=dag,
    is_delete_operator_pod=False,
    service_account_name="vault-sidecar",
)

end = DummyOperator(
    task_id="end",
    dag=dag
)

start >> download_task >> container_task >> end