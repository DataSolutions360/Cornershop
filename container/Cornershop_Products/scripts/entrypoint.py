import pandas as pd
from datetime import timedelta, datetime
import logging
import platform
import json
import argparse
import urllib.parse

# third party libraries
from google.cloud import storage

logging.basicConfig(level=logging.INFO)

# updated download_blob function

#referenced at bottom of code:
#    download_blob(input_bucket_path+"/"+input_file_name, input_bucket_name, local_file_name, gcs_secret_cred_path)
# updated download_blob function
def download_blob(source_blob_name, input_bucket_name, destination_file_name, gcs_secret_cred_path):
    """
    Downloads a blob from the input bucket.
    :param source_blob_name: The name of the blob to download.
    :param input_bucket_name: The name of the bucket where the blob is stored.
    :param destination_file_name: The local file path to save the downloaded blob.
    :param gcs_secret_cred_path: The path of the GCS credential file.
    """
    storage_client = storage.Client.from_service_account_json(gcs_secret_cred_path)
    bucket = storage_client.bucket(input_bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)
    logging.info(
        f"Downloaded storage object {source_blob_name} from bucket {input_bucket_name} to local file {destination_file_name}."
    )

def process_data(input_file_name, file_name):
    """
    Reads data from a CSV file and saves it to a Parquet file.
    :param input_file_name: The local file path of the CSV file to read.
    :param file_name: The local file path to save the Parquet file.
    """
    logging.info(f"reading the data from CSV file {input_file_name}")
    df = pd.read_csv(input_file_name)
    df.to_parquet(file_name)
    logging.info(f"data is saved in Parquet file {file_name}, no of records: {df.shape[0]}")

def upload_to_gcs(input_bucket_name, output_bucket_name, input_bucket_path, output_bucket_path, input_file_name, output_file_name, gcs_secret_cred_path):
    """
    Uploads a file to a Google Cloud Storage bucket.
    :param input_bucket_name: The name of the input bucket where the file is currently located.
    :param output_bucket_name: The name of the output bucket where the file will be uploaded.
    :param input_bucket_path: The path of the input file in the input bucket.
    :param output_bucket_path: The path where the file will be saved in the output bucket.
    :param input_file_name: The local file path of the file to upload.
    :param output_file_name: The name of the file to save in the output bucket.
    :param gcs_secret_cred_path: The path of the GCS credential file.
    """
    gcs_client = storage.Client.from_service_account_json(gcs_secret_cred_path)
    output_bucket = gcs_client.bucket(output_bucket_name)
    filepath = output_bucket_path + output_file_name
    logging.info(f"Uploading file {output_file_name} to GCS bucket {output_bucket_name} at path: {filepath}")
    blob = output_bucket.blob(filepath)
    blob.upload_from_filename(input_file_name)
    logging.info("file upload to GCS successful")

def parse_args():
    """
    parses arguments
    :return: parsed args
    """
    parser = argparse.ArgumentParser(description="")
    parser.add_argument(
        "--input_bucket_name",
        type=str,
        required=True,
        help="Bucket name where data needs to be to pulled from",
    )
    parser.add_argument(
        "--output_bucket_name",
        type=str,
        required=True,
        help="Bucket name where data needs to be to stored",
    )
    parser.add_argument(
        "--input_bucket_path", type=str, required=True, help="path of the input bucket name"
    )
    parser.add_argument(
        "--output_bucket_path", type=str, required=True, help="path of the output bucket name"
    )
    parser.add_argument(
        "--output_file_name", type=str, required=True, help="output file name to be stored"
    )
    parser.add_argument(
        "--input_file_name",type=str, required=True, help="name of the input file to be downloaded from GCS"
    )
    parser.add_argument(
        "--gcs_secret_cred_path",
        type=str,
        required=False,
        default="/vault/secrets/gcp-sa-storage.json",
        help="GCS credential path ex: /vault/secrets/..."
    )

    parsed_args = parser.parse_args()
    return parsed_args

# updated main function
def main():
    args = parse_args()
    input_bucket_name = args.input_bucket_name
    output_bucket_name = args.output_bucket_name
    input_bucket_path = args.input_bucket_path
    output_bucket_path = args.output_bucket_path
    input_file_name = args.input_file_name
    output_file_name = args.output_file_name
    gcs_secret_cred_path = args.gcs_secret_cred_path

    # define source_blob_name variable
    source_blob_name = input_bucket_path + "/" + input_file_name

    # download blob from input bucket
    local_file_name = f"/tmp/{output_file_name}"
    download_blob(source_blob_name, input_bucket_name, local_file_name, gcs_secret_cred_path)

    # process data
    processed_file_name = f"/tmp/processed_{output_file_name}"
    process_data(local_file_name, processed_file_name)

    # upload processed file to output bucket
    upload_file_name = output_bucket_path + "/processed_" + output_file_name
    upload_to_gcs(
        input_bucket_name,
        output_bucket_name,
        input_bucket_path,
        output_bucket_path,
        processed_file_name,
        upload_file_name,
        gcs_secret_cred_path,
    )

if __name__ == "__main__": 
    main()