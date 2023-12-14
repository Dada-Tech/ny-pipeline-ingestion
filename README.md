# Data Ingestion Pipeline: New York City Data
Data Ingestion Pipeline for New York City Data | Preprocessed and Deployed to Docker | Data Warehouse | Data Lake | PSQL | SQL

[View Pipeline](https://github.com/Dada-Tech/ny-pipeline-ingestion/blob/main/ingest_data.py)  
[View Notebook](https://github.com/Dada-Tech/ny-pipeline-ingestion/blob/main/psql-workbench.ipynb)

![banner](pipeline-banner.png)  

## Description
Automated ML saves countless hours of debugging, and prevents tedious work that need not be repeated.
This project is a data ingestion pipeline that takes open city data, preprocesses it and puts it ingests it into a data warehouse.

## Goals
* Create a real, usable and scalable data ingestion pipeline
* Handle both .csv and .parquet file formats for robustness
* Deploy the pipeline script in Docker for reliability across different environments
* Dockerize the Data Warehouse
* Dockerize a Data management solution to ensure services and data all works together nicely

# Examples
`export URL="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"`

`Docker build -t taxi_ingest:v1`

`docker run -it \
--network=pg-network \
--name=data_ingest \
taxi_ingest:v1 \
--user=root \
--password=root \
--host=pg-db \
--port=5432 \
--db=ny_taxi \
--table_name=yellow_taxi_trips \
--url=${URL}`

Ideally it would be set with environment file and used from the docker compose service

# Breakdown:
## Pipeline:
This is a robust data ingestion pipeline that can handle and preprocess data of many different formats

## Jupyter Notebook
This is to have proof of concept and thoroughly test the pipeline steps before deploying a final product

## Docker Compose Services
* postgres
  * Postgres DB
* pgadmin
  * Data Management tool
 
## Dockerfile
Contains the instructions to build and run the data ingestion script.

## Data Dictionary
[Data Dictionary](https://github.com/Dada-Tech/ny-pipeline-ingestion/blob/main/data_dictionary_trip_records_yellow.pdf)


