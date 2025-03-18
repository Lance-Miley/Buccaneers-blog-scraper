### Project Overview
This project creates an automated ETL data pipeline for scrapping data from the popular Buccaneers website joebucsfan.com using the orchestration tool Airflow run through a Docker container. 

The code scraps and cleans a day's worth of articles (on a two day lag to allow ample time for comments) automatically each day, including the content from the article and the individual comments made about the article. Article content and comments are fed into an OpenAI API for summaries and sentiment analysis. Each day's information is stored in .csv and .json files which are dumped into an S3 Data Lake. The .csv files are loaded into tables in a Snowflake Data Warehouse. The Snowflake tables are queried each day and saved out locally in .csv files. Those .csv files are analyzed using R and summary information is generated in graphs using ggplot2. 

### Code Structure
The dags folder has the core .py scripts that are used in the process
- joebucs_pipeline.py is the script that airflow calls. It contains and summarizes the various functions into tasks for the DAG.
- data_scraper.py has the functions used to scrape the website, call the OpenAI API, clean the data and save out the daily .csv and .json files. The files are saved here in the Ouput/Extracts folder.
- S3_upload.py takes the daily files and loads them into S3
- snowflake_load.py takes the daily .csv files and inserts them into tables on the Snowflake Data Warehouse
- snowflake_table_read.py reads from the Snowflake Data Warehouse tables and saves the information into two .xlsx files in the Snowflake_data/ folder. 

The comments_analysis.R file pulls from the files in the Snowflake_data/ folder and creates the daily summary graphs that can be seen in the Analysis/ folder. 