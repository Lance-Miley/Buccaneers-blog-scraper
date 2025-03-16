import os
import boto3
from datetime import datetime, timedelta
import logging

def upload_to_s3(**kwargs):
    """
    Uploads CSV files to an S3 bucket.

    This function reads AWS credentials from environment variables,
    constructs the file paths, and uploads the files to the specified S3 bucket.
    """

    # Configure logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    try:
        # Retrieve AWS credentials and bucket name from environment variables
        access_key = os.getenv("AWS_ACCESS_KEY_ID")
        secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        bucket_name = os.getenv("AWS_BUCKET_NAME")

        if not all([access_key, secret_key, bucket_name]):
            logger.error("AWS credentials or bucket name not set in environment variables.")
            return

        # Initialize S3 client
        s3 = boto3.client(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )

        # Calculate post_date_string
        lag_days = int(os.getenv("LAG_DAYS", 2))  # Default to 2 days if not set
        post_date = datetime.now().date() - timedelta(days=lag_days)
        post_date_string = post_date.strftime('%m%d%Y')

        # Define file paths
        base_output_path = "/opt/airflow/output/Extracts"
        export_file_csv = os.path.join(base_output_path, f'data_{post_date_string}.csv')
        comment_file_csv = os.path.join(base_output_path, f'comments_{post_date_string}.csv')

        # List of files to upload
        files_to_upload = [
            {
                'file_path': export_file_csv,
                's3_key': f'joebucs/data_{post_date_string}.csv'
            },
            {
                'file_path': comment_file_csv,
                's3_key': f'joebucs/comments_{post_date_string}.csv'
            }
        ]

        for file in files_to_upload:
            file_path = file['file_path']
            s3_key = file['s3_key']

            if os.path.exists(file_path):
                logger.info(f"Uploading {file_path} to s3://{bucket_name}/{s3_key}")
                s3.upload_file(file_path, bucket_name, s3_key)
                logger.info(f"Successfully uploaded {file_path} to s3://{bucket_name}/{s3_key}")
            else:
                logger.warning(f"File {file_path} does not exist and cannot be uploaded.")

    except Exception as e:
        logger.error(f"An error occurred during S3 upload: {e}")
        raise
