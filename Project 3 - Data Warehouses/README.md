# Data Warehouse with AWS S3 and Redshift

## Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. This project is responsible for building an ETL pipeline in order to their analytics team to continue finding insights into what songs their users are listening to. The ETL pipeline inlcude:
- Extracts their data from S3
- Stages them in Redshift
- Transforms data into a set of dimensional tables

## Data Warehouse schema design
- Fact table: songplays
- Dimension tables: users, songs, artists, time 

## How to run the Python Scripts
Firstly, you must create a Reshift Cluster and IAM Role for Redshift to connect the S3.
Then, you copy the ARN and paste into `dwh.cfg`
```
    python create_tables.py
    python etl.py
```

## Explanation
- `create_table.py` is where you'll create your fact and dimension tables for the star schema in Redshift.
- `etl.py` is where you'll load data from S3 into staging tables on Redshift and then process that data into your analytics tables on Redshift.
- `sql_queries.py` containing SQL statements.
