# snowflake_load.py

import os
import snowflake.connector
import datetime
from datetime import timedelta
import logging

def load_to_snowflake(**kwargs):
    """
    Loads data from S3 into Snowflake using environment variables for credentials.
    Ensure pipeline.env or your Docker environment includes:
      SNOWFLAKE_USERNAME, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT_NAME
      (Optional) SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, LAG_DAYS
    """
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # Read Snowflake credentials from environment variables
    username = os.getenv("SNOWFLAKE_USERNAME")
    password = os.getenv("SNOWFLAKE_PASSWORD")
    account_name = os.getenv("SNOWFLAKE_ACCOUNT_NAME")

    # Optional: read DB/schema from environment or set defaults
    database = os.getenv("SNOWFLAKE_DATABASE", "PIPELINE")
    schema = os.getenv("SNOWFLAKE_SCHEMA", "JOEBUCS")

    if not all([username, password, account_name]):
        logger.error("Snowflake credentials not set in environment variables.")
        return

    # Create Snowflake connection
    snow_conn = snowflake.connector.connect(
        user=username,
        password=password,
        account=account_name,
        database=database,
        schema=schema
    )

    # Figure out how many days to go back for naming the files
    lag_days = int(os.getenv("LAG_DAYS", 2))
    post_date = datetime.date.today() - timedelta(days=lag_days)
    post_date_string = post_date.strftime('%m%d%Y')

    # COPY statements for articles and comments
    sql_data = f"""
    COPY INTO {database}.{schema}.joebucs_extract
    FROM @s3_stage/data_{post_date_string}.csv
    ON_ERROR = CONTINUE
    FILE_FORMAT = pipe_csv_format;
    """

    sql_comments = f"""
    COPY INTO {database}.{schema}.joebucs_comments
    FROM @s3_stage/comments_{post_date_string}.csv
    ON_ERROR = CONTINUE
    FILE_FORMAT = pipe_csv_format;
    """

    # Execute queries
    cur = snow_conn.cursor()
    try:
        cur.execute(sql_data)
        cur.execute(sql_comments)
        logger.info("Successfully loaded data into Snowflake.")
    except Exception as e:
        logger.error(f"Error loading data into Snowflake: {e}")
        raise
    finally:
        cur.close()
        snow_conn.close()
