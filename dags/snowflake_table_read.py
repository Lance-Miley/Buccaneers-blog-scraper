def read_from_snowflake(**kwargs):
    import os
    import logging
    import snowflake.connector
    import pandas as pd
    
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    username = os.getenv("SNOWFLAKE_USERNAME")
    password = os.getenv("SNOWFLAKE_PASSWORD")
    account_name = os.getenv("SNOWFLAKE_ACCOUNT_NAME")
    database = os.getenv("SNOWFLAKE_DATABASE", "PIPELINE")
    schema = os.getenv("SNOWFLAKE_SCHEMA", "JOEBUCS")

    output_dir = "/opt/airflow/Snowflake_data"  # Directory inside container
    os.makedirs(output_dir, exist_ok=True)  # Ensure it exists

    # Connect to Snowflake
    snow_conn = snowflake.connector.connect(
        user=username,
        password=password,
        account=account_name,
        database=database,
        schema=schema
    )

    try:
        sql_extract = f"SELECT * FROM {database}.{schema}.joebucs_extract;"
        df_extract = pd.read_sql(sql_extract, snow_conn)
        df_extract.to_excel(os.path.join(output_dir, "joebucs_extract.xlsx"), index=False)
        logger.info("Saved joebucs_extract.xlsx")

        sql_comments = f"SELECT * FROM {database}.{schema}.joebucs_comments;"
        df_comments = pd.read_sql(sql_comments, snow_conn)
        df_comments.to_excel(os.path.join(output_dir, "joebucs_comments.xlsx"), index=False)
        logger.info("Saved joebucs_comments.xlsx")

    except Exception as e:
        logger.error(f"Error reading from Snowflake: {e}")
        raise
    finally:
        snow_conn.close()
        logger.info("Snowflake connection closed.")
