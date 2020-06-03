# Covid19-Monitoring Solution
## Description

A continuous data pipeline on Hadoop using a number of different big data technologies and automated data lifecycle, from data ingestion of raw datafiles to the analysis and reporting of the most recent data available on any browser.

The ETL process is handled by a bash script that orchestrate the processes across multiple pyspark jobs that handle each part of the ETL process separately. The high-level process
1. Extraction: of the raw data files from the John Hopkins COVID-19 data repository and into HDFS
2. Transformation: of the csv files after loading them into PySpark dataframes which are passed through different data transformation steps to create one denormalized dataset.
3. Loading: the final dataframe into a hive table to enable fast data queries to be run from the analysis and reporting stage.
4. Analysis and reporting: uses HQL queries to extract data from the hive table and create data snapshots for reporting and visualization. The collection of data visualizations generated from each snapshot are combined into one html page that is hosted on the Linux server.

The data visualization data loading process is part of the entire pipeline, therefore whenever the pipeline is executed the dashboard will be loaded with the most recent data to generate timely insights.

## Usage
extract.sh - For Data Ingestion and Storage to HDFS

transform.py - For Data transformation and Processing

viz.py - For Storing transformed data in Hive, querying using HQL and visualizing results

driver.sh - For running the ETL pipeline as a batch


## Technologies Used
![Tech Used](/Pictures/Technologies_used.jpg)

## Solution Architecture
![Solution Architecture](/Pictures/Solution_Architecture.jpg)
