FROM apache/airflow:2.9.1-python3.11
RUN pip install --no-cache-dir openai boto3 openpyxl

