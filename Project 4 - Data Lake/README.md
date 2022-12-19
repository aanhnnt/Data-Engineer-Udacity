# Data Lake with Spark and S3

## Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. This project is responsible for building an ETL pipeline in order to their analytics team to continue finding insights into what songs their users are listening to. The ETL pipeline inlcude:
- Extracts their data from S3
- Processes them using Spark
- Transforms data into S3 as a set of dimensional tables

## Data Warehouse schema design
- Fact table: songplays
- Dimension tables: users, songs, artists, time 

## How to run the Python Scripts
Fistly, you need to copy the AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, paste them into `dl.cfg`.
After that, run the command
```
    python etl.py
```

## Explanation
- `dl.cfg` is where contains your AWS credentials
- `etl.py` is where you'll reads data from S3, processes that data using Spark, and writes them back to S3.
